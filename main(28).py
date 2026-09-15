
import os, json, time, threading, heapq
from collections import defaultdict, deque
from flask import Flask, jsonify
import requests
from websocket import WebSocketApp

# ============================================================
# BYBIT MICRO SCALPER v11
# Event-driven paper scalper:
#   1) liquidity sweep + reversal
#   2) absorption + reversal
#   3) impulse + pullback + continuation
#
# The score is NOT an entry trigger. A concrete event is required.
# Paper only: this program never sends private/order requests.
#
# v11 plumbing:
#   - real order book maintained from snapshot + delta (best bid/ask correct)
#   - evaluation runs on a dedicated thread, NOT inside the WS callback
#   - single exchange-aligned clock for windows / cooldown / positions
#   - time stop actually caps holding time
#   - LEVERAGE caps total gross notional exposure
#   - session-loss halt is an explicit, logged, visible state
#   - shared state guarded by one lock; snapshot-then-compute in eval
#   - subscribes only to the symbols it actually monitors
# ============================================================

API = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

START_BALANCE = float(os.getenv("START_BALANCE", "150"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.005"))
LEVERAGE = float(os.getenv("LEVERAGE", "10"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "30"))
MAX_TOTAL_RISK_PCT = float(os.getenv("MAX_TOTAL_RISK_PCT", "0.10"))
MAX_SESSION_LOSS_PCT = float(os.getenv("MAX_SESSION_LOSS_PCT", "0.10"))

TOP_N = int(os.getenv("TOP_N", "300"))
MONITOR_N = int(os.getenv("MONITOR_N", "100"))

TP_PCT = float(os.getenv("TP_PCT", "0.006"))
SL_PCT = float(os.getenv("SL_PCT", "0.0025"))
TIME_STOP_SEC = int(os.getenv("TIME_STOP_SEC", "180"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "120"))

TAKER_FEE = float(os.getenv("TAKER_FEE", "0.00055"))
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.00015"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.0012"))

MIN_EVENT_SCORE = float(os.getenv("MIN_EVENT_SCORE", "76"))
EVAL_INTERVAL = float(os.getenv("EVAL_INTERVAL", "0.50"))

MAX_BOOK_AGE = float(os.getenv("MAX_BOOK_AGE", "5.0"))
MANAGE_INTERVAL = float(os.getenv("MANAGE_INTERVAL", "0.10"))

app = Flask(__name__)

# ---- shared state (guarded by LOCK) ----
LOCK = threading.RLock()

BALANCE = START_BALANCE
EQUITY = START_BALANCE
REALIZED_PNL = 0.0
GROSS_PNL = 0.0
FEES = 0.0
SLIPPAGE_COST = 0.0

T = defaultdict(lambda: deque(maxlen=2200))      # sym -> (ts, price, signed_notional)
BOOKRAW = {}                                     # sym -> {"bids":{px:sz}, "asks":{px:sz}, "ready":bool}
BOOK = {}                                        # sym -> derived {bid,ask,mid,imb,spread,ts}
LAST_PRICE = {}
LAST_EVAL = defaultdict(float)
COOLDOWN = {}
POSITIONS = {}
EVENTS = deque(maxlen=100)
SIGNAL_STATS = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0})

LAST_ERROR = ""
WS_STATE = "DISCONNECTED"
CALCULATIONS = 0
BLOCKED = 0
SYMBOLS = []
CLOCK_OFFSET = 0.0
HALTED = False

STATS = {
    "trades": 0, "wins": 0, "losses": 0,
    "tp": 0, "sl": 0, "time": 0, "reversal": 0,
    "long_trades": 0, "long_wins": 0, "long_pnl": 0.0,
    "short_trades": 0, "short_wins": 0, "short_pnl": 0.0,
}


def clip(x, a=-1.0, b=1.0):
    return max(a, min(b, x))


def note_server_ts(ms):
    """Track offset between exchange send time and local clock (light EMA)."""
    global CLOCK_OFFSET
    if not ms:
        return
    off = ms / 1000.0 - time.time()
    CLOCK_OFFSET = off if CLOCK_OFFSET == 0.0 else 0.97 * CLOCK_OFFSET + 0.03 * off


def ex_now():
    """Current time on the exchange clock — used for every market-time decision."""
    return time.time() + CLOCK_OFFSET


def log(s):
    EVENTS.appendleft(time.strftime("%H:%M:%S ") + s)


def fetch_top_symbols():
    try:
        r = requests.get(API + "/v5/market/tickers",
                         params={"category": "linear"}, timeout=8)
        rows = []
        for x in r.json().get("result", {}).get("list", []):
            sym = x.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            try:
                turnover = float(x.get("turnover24h", 0) or 0)
            except Exception:
                turnover = 0.0
            rows.append((turnover, sym))
        rows.sort(reverse=True)
        return [s for _, s in rows[:TOP_N]]
    except Exception as e:
        global LAST_ERROR
        LAST_ERROR = repr(e)
        return []


def chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


# ============================================================
# ORDER BOOK — snapshot replaces, delta upserts (size 0 = remove)
# ============================================================
def apply_book(sym, msg_type, data):
    raw = BOOKRAW.get(sym)
    if msg_type == "snapshot" or raw is None:
        raw = {"bids": {}, "asks": {}, "ready": False}
        BOOKRAW[sym] = raw
        for p, s in data.get("b", []) or []:
            raw["bids"][float(p)] = float(s)
        for p, s in data.get("a", []) or []:
            raw["asks"][float(p)] = float(s)
        raw["ready"] = True
    elif msg_type == "delta":
        if not raw.get("ready"):
            return
        for p, s in data.get("b", []) or []:
            p = float(p); s = float(s)
            if s == 0.0:
                raw["bids"].pop(p, None)
            else:
                raw["bids"][p] = s
        for p, s in data.get("a", []) or []:
            p = float(p); s = float(s)
            if s == 0.0:
                raw["asks"].pop(p, None)
            else:
                raw["asks"][p] = s
    else:
        return
    rebuild_book(sym, raw)


def rebuild_book(sym, raw):
    bids = raw["bids"]
    asks = raw["asks"]
    if not bids or not asks:
        return
    best_bid = max(bids)
    best_ask = min(asks)
    # Guard transient crossed/locked book — keep the last good snapshot.
    if best_ask <= best_bid or best_bid <= 0:
        return
    top_bids = heapq.nlargest(10, bids.keys())
    top_asks = heapq.nsmallest(10, asks.keys())
    bq = sum(bids[p] for p in top_bids)
    aq = sum(asks[p] for p in top_asks)
    mid = (best_bid + best_ask) / 2
    BOOK[sym] = {
        "bid": best_bid, "ask": best_ask, "mid": mid,
        "imb": clip((bq - aq) / max(bq + aq, 1e-12)),
        "spread": (best_ask - best_bid) / mid,
        "ts": ex_now(),
    }


def add_trade(sym, ts, price, signed_notional):
    T[sym].append((ts, price, signed_notional))
    LAST_PRICE[sym] = price


# ============================================================
# FEATURES — computed from a snapshot, no shared-state access
# ============================================================
def window(rows, tnow, seconds):
    cutoff = tnow - seconds
    return [x for x in rows if x[0] >= cutoff]


def signed_ratio(rows):
    if not rows:
        return 0.0
    buy = sum(max(0.0, x[2]) for x in rows)
    sell = sum(max(0.0, -x[2]) for x in rows)
    return clip((buy - sell) / max(buy + sell, 1e-9))


def activity_ratio(cur, prev):
    a = sum(abs(x[2]) for x in cur) / max(len(cur), 1)
    b = sum(abs(x[2]) for x in prev) / max(len(prev), 1)
    return a / max(b, 1e-9)


def price_change(rows):
    if len(rows) < 2:
        return 0.0
    return (rows[-1][1] - rows[0][1]) / max(rows[0][1], 1e-12)


def features(trades, book, tnow):
    if len(trades) < 12:
        return None

    w5 = window(trades, tnow, 5)
    w10 = window(trades, tnow, 10)
    w15 = window(trades, tnow, 15)
    w30 = window(trades, tnow, 30)
    w60 = window(trades, tnow, 60)

    if len(w5) < 6 or len(w15) < 10:
        return None

    prev5 = [x for x in w10 if x[0] < tnow - 5]
    cvd5 = signed_ratio(w5)
    cvd_prev = signed_ratio(prev5)
    impulse = clip(cvd5 - cvd_prev)

    mom5 = clip(price_change(w5) / 0.0025)
    mom15 = clip(price_change(w15) / 0.0040)
    mom30 = clip(price_change(w30) / 0.0060)

    act = activity_ratio(w5, prev5)

    # Book: require a fresh, present book. Otherwise spread stays large
    # (=> spread_ok fails) and imbalance contributes nothing.
    if book and (tnow - book.get("ts", 0.0)) <= MAX_BOOK_AGE:
        spread = float(book.get("spread", 1.0))
        bookimb = float(book.get("imb", 0.0))
    else:
        spread = 1.0
        bookimb = 0.0

    px = w5[-1][1]

    # Local extrema excluding the newest 2 seconds.
    prior = [x for x in w60 if x[0] < tnow - 2]
    high = max((x[1] for x in prior), default=px)
    low = min((x[1] for x in prior), default=px)

    return {
        "price": px, "cvd": cvd5, "cvd_prev": cvd_prev, "impulse": impulse,
        "mom5": mom5, "mom15": mom15, "mom30": mom30,
        "activity": act, "book": bookimb, "spread": spread,
        "high": high, "low": low, "w5": w5,
    }


def event_signal(f):
    """
    Returns (side, event, score) on a valid signal, else (None, None, 0.0).
    Event detection is deliberately independent from the final score.
    """
    px = f["price"]
    spread_ok = f["spread"] <= MAX_SPREAD
    cost = 2 * (TAKER_FEE + SLIPPAGE)

    # ---------- 1. LIQUIDITY SWEEP + REVERSAL ----------
    high_sweep = px > f["high"] * 1.00035
    low_sweep = px < f["low"] * 0.99965

    sweep_short = (
        high_sweep and
        f["cvd"] < -0.15 and
        f["mom5"] < -0.08 and
        f["activity"] >= 1.05
    )
    sweep_long = (
        low_sweep and
        f["cvd"] > 0.15 and
        f["mom5"] > 0.08 and
        f["activity"] >= 1.05
    )

    # ---------- 2. ABSORPTION + REVERSAL ----------
    range5 = abs(price_change(f["w5"]))
    absorption_short = (
        f["cvd_prev"] > 0.62 and
        range5 < 0.00070 and
        f["mom5"] < -0.10 and
        f["activity"] >= 1.20
    )
    absorption_long = (
        f["cvd_prev"] < -0.62 and
        range5 < 0.00070 and
        f["mom5"] > 0.10 and
        f["activity"] >= 1.20
    )

    # ---------- 3. IMPULSE + PULLBACK + CONTINUATION ----------
    continuation_long = (
        f["mom30"] > 0.35 and
        -0.55 < f["mom5"] < 0.05 and
        f["cvd"] > 0.40 and
        f["impulse"] > 0.15 and
        f["activity"] >= 1.05
    )
    continuation_short = (
        f["mom30"] < -0.35 and
        -0.05 < f["mom5"] < 0.55 and
        f["cvd"] < -0.40 and
        f["impulse"] < -0.15 and
        f["activity"] >= 1.05
    )

    candidates = []
    if sweep_short:
        candidates.append(("SHORT", "SWEEP_REVERSAL", 25))
    if sweep_long:
        candidates.append(("LONG", "SWEEP_REVERSAL", 25))
    if absorption_short:
        candidates.append(("SHORT", "ABSORPTION", 24))
    if absorption_long:
        candidates.append(("LONG", "ABSORPTION", 24))
    if continuation_long:
        candidates.append(("LONG", "PULLBACK_CONT", 23))
    if continuation_short:
        candidates.append(("SHORT", "PULLBACK_CONT", 23))

    if not candidates or not spread_ok:
        return None, None, 0.0

    best = None
    best_score = -1
    for side, event, base in candidates:
        d = 1 if side == "LONG" else -1

        book_pts = 12 * clip((f["book"] * d) / 0.30, 0, 1)
        flow_pts = 18 * clip((f["cvd"] * d) / 0.70, 0, 1)
        price_pts = 15 * clip((f["mom5"] * d) / 0.45, 0, 1)
        imp_pts = 10 * clip((f["impulse"] * d) / 0.45, 0, 1)
        act_pts = 8 * clip(f["activity"] - 1.0, 0, 1)

        score = base + book_pts + flow_pts + price_pts + imp_pts + act_pts

        late = abs(f["mom5"]) > 0.90
        expected = max(abs(f["mom5"]), abs(f["mom15"])) * 0.0025

        if late:
            continue
        if expected < cost + 0.00010:
            continue
        if score >= MIN_EVENT_SCORE and score > best_score:
            best = (side, event, score)
            best_score = score

    if best:
        return best
    return None, None, 0.0


# ============================================================
# POSITIONS  (all callers hold LOCK)
# ============================================================
def open_position(sym, side, score, event, price):
    global BLOCKED
    if HALTED:
        BLOCKED += 1
        return
    if sym in POSITIONS or len(POSITIONS) >= MAX_POSITIONS:
        BLOCKED += 1
        return
    if ex_now() < COOLDOWN.get(sym, 0):
        BLOCKED += 1
        return

    risk = BALANCE * RISK_PCT
    if sum(x["risk"] for x in POSITIONS.values()) + risk > BALANCE * MAX_TOTAL_RISK_PCT:
        BLOCKED += 1
        return

    notional = risk / SL_PCT
    # LEVERAGE now binds: total gross notional across open positions is capped.
    open_notional = sum(x["notional"] for x in POSITIONS.values())
    if open_notional + notional > BALANCE * LEVERAGE:
        BLOCKED += 1
        return
    if notional <= 0:
        BLOCKED += 1
        return

    POSITIONS[sym] = {
        "side": side, "entry": price, "qty": notional / price,
        "notional": notional, "risk": risk,
        "score": score, "event": event, "opened": ex_now(),
    }
    SIGNAL_STATS[str(event)]["n"] += 1
    log(f"ENTRY {side} {sym} event={str(event)} score={float(score):.1f} px={price:.6g}")


def close_position(sym, price, reason):
    global BALANCE, REALIZED_PNL, GROSS_PNL, FEES, SLIPPAGE_COST
    p = POSITIONS.pop(sym, None)
    if not p:
        return

    d = 1 if p["side"] == "LONG" else -1
    gross = (price - p["entry"]) / p["entry"] * d * p["notional"]
    fee = p["notional"] * 2 * TAKER_FEE
    slip = p["notional"] * 2 * SLIPPAGE
    net = gross - fee - slip

    BALANCE += net
    REALIZED_PNL += net
    GROSS_PNL += gross
    FEES += fee
    SLIPPAGE_COST += slip

    STATS["trades"] += 1
    if net > 0:
        STATS["wins"] += 1
    else:
        STATS["losses"] += 1

    if p["side"] == "LONG":
        STATS["long_trades"] += 1
        STATS["long_pnl"] += net
        if net > 0:
            STATS["long_wins"] += 1
    else:
        STATS["short_trades"] += 1
        STATS["short_pnl"] += net
        if net > 0:
            STATS["short_wins"] += 1

    STATS[reason.lower()] += 1
    SIGNAL_STATS[str(p["event"])]["pnl"] += net
    if net > 0:
        SIGNAL_STATS[str(p["event"])]["wins"] += 1

    COOLDOWN[sym] = ex_now() + COOLDOWN_SEC
    log(f"EXIT {p['side']} {sym} {reason} event={p['event']} "
        f"net={net:+.2f} gross={gross:+.2f} fee={fee:.2f}")


def manage_positions():
    global EQUITY
    tnow = ex_now()
    unreal = 0.0
    for sym, p in list(POSITIONS.items()):
        px = LAST_PRICE.get(sym)
        if not px:
            continue
        d = 1 if p["side"] == "LONG" else -1
        ret = (px - p["entry"]) / p["entry"] * d
        unreal += ret * p["notional"]

        if ret >= TP_PCT:
            close_position(sym, px, "TP")
            continue
        if ret <= -SL_PCT:
            close_position(sym, px, "SL")
            continue
        # Hard time cap: a scalper does not hold past its clock, whatever the PnL.
        if tnow - p["opened"] >= TIME_STOP_SEC:
            close_position(sym, px, "TIME")

    EQUITY = BALANCE + unreal


def evaluate(sym):
    global CALCULATIONS
    tnow = ex_now()
    if tnow - LAST_EVAL[sym] < EVAL_INTERVAL:
        return
    LAST_EVAL[sym] = tnow

    # Short lock: grab snapshots, then compute without holding it.
    with LOCK:
        trades = list(T[sym])
        book = dict(BOOK.get(sym, {}))
        in_pos = sym in POSITIONS
        pos = dict(POSITIONS[sym]) if in_pos else None

    f = features(trades, book, tnow)
    if not f:
        return
    CALCULATIONS += 1

    if in_pos:
        # Reversal is a management event, not a new entry.
        age = tnow - pos["opened"]
        d = 1 if pos["side"] == "LONG" else -1
        opp = f["cvd"] * (-d)
        adverse = (f["price"] - pos["entry"]) / pos["entry"] * d < -0.0010
        # Minimum hold + meaningful adverse move + strong opposite flow.
        if age >= 20 and adverse and opp > 0.72:
            with LOCK:
                if sym in POSITIONS:
                    close_position(sym, f["price"], "REVERSAL")
        return

    side, event, score = event_signal(f)
    if side:
        with LOCK:
            open_position(sym, side, score, event, f["price"])


# ============================================================
# THREADS
# ============================================================
def check_session_halt():
    """Called under LOCK. Trip the halt once, visibly."""
    global HALTED
    if not HALTED and BALANCE <= START_BALANCE * (1 - MAX_SESSION_LOSS_PCT):
        HALTED = True
        log(f"SESSION LOSS LIMIT HIT — trading halted at balance ${BALANCE:.2f}")


def eval_worker():
    """Runs evaluation and position management off the WS callback."""
    global LAST_ERROR
    last_manage = 0.0
    while True:
        try:
            for s in SYMBOLS:
                evaluate(s)
            t = time.time()
            if t - last_manage >= MANAGE_INTERVAL:
                last_manage = t
                with LOCK:
                    manage_positions()
                    check_session_halt()
        except Exception as e:
            LAST_ERROR = repr(e)
        time.sleep(0.02)


def ws_worker(symbols):
    global WS_STATE, LAST_ERROR

    topics = []
    for s in symbols:
        topics.append(f"publicTrade.{s}")
        topics.append(f"orderbook.50.{s}")

    def on_open(ws):
        global WS_STATE
        WS_STATE = "CONNECTED"
        for group in chunks(topics, 50):
            ws.send(json.dumps({"op": "subscribe", "args": group}))
        log(f"WS CONNECTED topics={len(topics)}")

    def on_message(ws, raw):
        global LAST_ERROR
        try:
            m = json.loads(raw)
            note_server_ts(m.get("ts"))
            topic = m.get("topic", "")
            if not topic:
                return
            data = m.get("data", [])

            if topic.startswith("publicTrade."):
                sym = topic.split(".", 1)[1]
                with LOCK:
                    for x in data:
                        px = float(x["p"])
                        qty = float(x["v"])
                        side = x.get("S", "Buy")
                        signed = px * qty * (1 if side == "Buy" else -1)
                        ts = float(x.get("T", int(ex_now() * 1000))) / 1000
                        add_trade(sym, ts, px, signed)

            elif topic.startswith("orderbook.50."):
                sym = topic.split(".")[-1]
                if isinstance(data, dict):
                    with LOCK:
                        apply_book(sym, m.get("type"), data)

        except Exception as e:
            LAST_ERROR = repr(e)

    def on_error(ws, err):
        global WS_STATE, LAST_ERROR
        WS_STATE = "ERROR"
        LAST_ERROR = repr(err)

    def on_close(ws, code, msg):
        global WS_STATE
        WS_STATE = "DISCONNECTED"
        # Force a fresh book snapshot after any gap.
        with LOCK:
            BOOKRAW.clear()

    while True:
        try:
            ws = WebSocketApp(WS_URL, on_open=on_open, on_message=on_message,
                              on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            LAST_ERROR = repr(e)
        WS_STATE = "RECONNECTING"
        time.sleep(3)


# ============================================================
# DASHBOARD
# ============================================================
def fmt_signal_stats(signal_stats):
    rows = []
    for k, v in sorted(signal_stats.items(), key=lambda z: z[1]["n"], reverse=True):
        if v["n"]:
            wr = 100 * v["wins"] / max(v["n"], 1)
            rows.append(f"<tr><td>{k}</td><td>{v['n']}</td><td>{wr:.1f}%</td><td>{v['pnl']:+.2f}</td></tr>")
    return "".join(rows) or "<tr><td colspan=4>No closed event trades yet</td></tr>"


def snapshot_state():
    with LOCK:
        return {
            "BALANCE": BALANCE, "EQUITY": EQUITY, "REALIZED_PNL": REALIZED_PNL,
            "GROSS_PNL": GROSS_PNL, "FEES": FEES, "SLIPPAGE_COST": SLIPPAGE_COST,
            "WS_STATE": WS_STATE, "HALTED": HALTED,
            "positions": [(s, dict(p)) for s, p in list(POSITIONS.items())[:30]],
            "n_positions": len(POSITIONS),
            "CALCULATIONS": CALCULATIONS, "BLOCKED": BLOCKED,
            "n_symbols": len(SYMBOLS),
            "STATS": dict(STATS),
            "SIGNAL_STATS": {k: dict(v) for k, v in SIGNAL_STATS.items()},
            "EVENTS": list(EVENTS)[:35],
            "LAST_ERROR": LAST_ERROR,
        }


def dashboard():
    st = snapshot_state()
    S = st["STATS"]
    wr = 100 * S["wins"] / max(S["trades"], 1)
    ws_line = st["WS_STATE"] + (" · <b>HALTED</b>" if st["HALTED"] else "")
    pos_rows = "".join(
        f"<tr><td>{s}</td><td>{p['side']}</td><td>{p['event']}</td><td>{p['score']:.0f}</td></tr>"
        for s, p in st["positions"]
    ) or "<tr><td>None</td></tr>"
    return f"""
<!doctype html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit Micro Scalper v11</title>
<style>
body{{font-family:system-ui;background:#111;color:#eee;margin:14px}}
.card{{background:#1b1b1b;padding:12px;border-radius:12px;margin:8px 0}}
.big{{font-size:27px;font-weight:700}}
table{{width:100%;font-size:12px;border-collapse:collapse}}
td{{padding:4px;border-bottom:1px solid #333}}
small{{color:#aaa}}
</style></head><body>
<h2>⚡ Bybit Micro Scalper v11</h2>
<div class="card"><div class="big">${st['BALANCE']:.2f}</div>
Equity ${st['EQUITY']:.2f}<br>
Net PnL ${st['REALIZED_PNL']:+.2f}<br>
Gross ${st['GROSS_PNL']:+.2f} · Fees ${st['FEES']:.2f} · Slip ${st['SLIPPAGE_COST']:.2f}</div>

<div class="card">WS <b>{ws_line}</b><br>
Positions <b>{st['n_positions']}/{MAX_POSITIONS}</b><br>
Calculations {st['CALCULATIONS']} · Monitor {min(MONITOR_N, st['n_symbols'])}<br>
Blocked {st['BLOCKED']}</div>

<div class="card">Trades {S['trades']} · W/L {S['wins']}/{S['losses']} · WR {wr:.1f}%<br>
TP {S['tp']} · SL {S['sl']} · TIME {S['time']} · REV {S['reversal']}<br>
LONG {S['long_trades']} / wins {S['long_wins']} / pnl {S['long_pnl']:+.2f}<br>
SHORT {S['short_trades']} / wins {S['short_wins']} / pnl {S['short_pnl']:+.2f}</div>

<div class="card">
Risk {RISK_PCT*100:.2f}% · Total risk {MAX_TOTAL_RISK_PCT*100:.1f}% · Max positions {MAX_POSITIONS}<br>
TP {TP_PCT*100:.2f}% · SL {SL_PCT*100:.2f}% · Event score ≥ {MIN_EVENT_SCORE:.0f}<br>
Taker {TAKER_FEE*100:.3f}% · Slip/side {SLIPPAGE*100:.3f}%
</div>

<div class="card"><b>Event performance</b>
<table><tr><td>Event</td><td>N</td><td>WR</td><td>PnL</td></tr>
{fmt_signal_stats(st['SIGNAL_STATS'])}</table></div>

<div class="card"><b>Open positions</b><table>
{pos_rows}
</table></div>

<div class="card"><b>Recent events</b><table>
{''.join(f'<tr><td>{e}</td></tr>' for e in st['EVENTS'])}
</table></div>
<div class="card"><small>{st['LAST_ERROR']}</small></div>
<script>setTimeout(()=>location.reload(),3000)</script>
</body></html>"""


@app.get("/")
def root():
    return dashboard()


@app.get("/api")
def api():
    st = snapshot_state()
    return jsonify({
        "version": "v11", "paper_only": True, "halted": st["HALTED"],
        "balance": st["BALANCE"], "equity": st["EQUITY"], "pnl": st["REALIZED_PNL"],
        "gross": st["GROSS_PNL"], "fees": st["FEES"], "slippage": st["SLIPPAGE_COST"],
        "ws": st["WS_STATE"], "positions": st["n_positions"],
        "calculations": st["CALCULATIONS"], "blocked": st["BLOCKED"],
        "stats": st["STATS"], "events": st["SIGNAL_STATS"], "error": st["LAST_ERROR"],
    })


def main():
    global SYMBOLS
    universe = fetch_top_symbols()
    if not universe:
        universe = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    # Subscribe to and monitor the same set — no wasted streams.
    SYMBOLS = universe[:MONITOR_N]

    log(f"START v11 monitor={len(SYMBOLS)}")
    threading.Thread(target=ws_worker, args=(SYMBOLS,), daemon=True).start()
    threading.Thread(target=eval_worker, daemon=True).start()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
