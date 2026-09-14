
import os, json, time, math, threading
from collections import defaultdict, deque
from flask import Flask, jsonify

import requests
from websocket import WebSocketApp

# =========================
# v9 MICRO SCALPER
# =========================
START_BALANCE = float(os.getenv("START_BALANCE", "150"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.005"))          # 0.5% balance risk / trade
LEVERAGE = float(os.getenv("LEVERAGE", "10"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "30"))
MAX_TOTAL_RISK_PCT = float(os.getenv("MAX_TOTAL_RISK_PCT", "0.10"))
MAX_SESSION_LOSS_PCT = float(os.getenv("MAX_SESSION_LOSS_PCT", "0.10"))

TOP_N = int(os.getenv("TOP_N", "300"))
MONITOR_N = int(os.getenv("MONITOR_N", "100"))

TP_PCT = float(os.getenv("TP_PCT", "0.0060"))             # 0.60%
SL_PCT = float(os.getenv("SL_PCT", "0.0025"))             # 0.25%
TIME_STOP_SEC = int(os.getenv("TIME_STOP_SEC", "180"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "90"))

# Base Bybit VIP0 USDT perpetual assumption; configurable for the account.
TAKER_FEE = float(os.getenv("TAKER_FEE", "0.00055"))
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.00015"))

ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "70"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.0012"))

API = "https://api.bybit.com"
WS = "wss://stream.bybit.com/v5/public/linear"

app = Flask(__name__)

BALANCE = START_BALANCE
EQUITY = START_BALANCE
REALIZED_PNL = 0.0
GROSS_PNL = 0.0
FEES = 0.0
SLIPPAGE_COST = 0.0

T = defaultdict(lambda: deque(maxlen=1600))
BOOK = {}
LAST_PRICE = {}
LAST_CALC = 0
LAST_ERROR = ""
WS_STATE = "DISCONNECTED"

POSITIONS = {}
COOLDOWN = {}
EVENTS = deque(maxlen=80)

STATS = {
    "trades": 0, "wins": 0, "losses": 0,
    "tp": 0, "sl": 0, "time": 0, "reversal": 0,
    "long_trades": 0, "long_wins": 0, "long_pnl": 0.0,
    "short_trades": 0, "short_wins": 0, "short_pnl": 0.0,
    "blocked": 0,
}

def now():
    return time.time()

def clip(x, a=-1.0, b=1.0):
    return max(a, min(b, x))

def log_event(s):
    EVENTS.appendleft(f"{time.strftime('%H:%M:%S')} {s}")

def fetch_top_symbols():
    try:
        r = requests.get(API + "/v5/market/tickers",
                         params={"category":"linear"}, timeout=8)
        data = r.json().get("result", {}).get("list", [])
        rows = []
        for x in data:
            sym = x.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if x.get("status", "Trading") not in ("Trading", ""):
                continue
            try:
                turnover = float(x.get("turnover24h", 0) or 0)
            except Exception:
                turnover = 0
            rows.append((turnover, sym))
        rows.sort(reverse=True)
        return [s for _, s in rows[:TOP_N]]
    except Exception as e:
        global LAST_ERROR
        LAST_ERROR = repr(e)
        return []

def chunks(xs, n=40):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def normalize_book(sym, data):
    try:
        bids = data.get("b", []) or []
        asks = data.get("a", []) or []
        if not bids or not asks:
            return
        bid = float(bids[0][0]); ask = float(asks[0][0])
        bid_qty = sum(float(x[1]) for x in bids[:10])
        ask_qty = sum(float(x[1]) for x in asks[:10])
        mid = (bid + ask) / 2
        if mid <= 0:
            return
        imb = (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1e-12)
        spread = (ask - bid) / mid
        BOOK[sym] = {"bid":bid, "ask":ask, "mid":mid,
                     "imb":clip(imb), "spread":spread, "ts":now()}
    except Exception:
        pass

def add_trade(sym, ts, price, signed_notional):
    T[sym].append((ts, price, signed_notional))
    LAST_PRICE[sym] = price

def metrics(sym):
    q = T.get(sym)
    if not q:
        return None
    tnow = now()
    # Ignore stale flow older than 35 seconds.
    cur = [x for x in q if tnow - x[0] <= 35]
    if len(cur) < 8:
        return None

    # Current 5s signed-flow window vs preceding 10s.
    w5 = [x for x in cur if tnow - x[0] <= 5]
    w10 = [x for x in cur if 5 < tnow - x[0] <= 15]
    prev5 = [x for x in cur if 10 < tnow - x[0] <= 15]

    if len(w5) < 4:
        return None

    def signed_ratio(rows):
        if not rows:
            return 0.0
        buy = sum(max(0.0, x[2]) for x in rows)
        sell = sum(max(0.0, -x[2]) for x in rows)
        return clip((buy - sell) / max(buy + sell, 1e-9))

    cvd = signed_ratio(w5)
    prev = signed_ratio(w10)
    # "Impulse" = acceleration of signed flow, bounded and robust.
    impulse = clip(cvd - prev)

    # Price momentum over roughly 5 seconds.
    p0 = w5[0][1]
    p1 = w5[-1][1]
    m5 = clip((p1 - p0) / max(p0, 1e-12) / 0.0025)

    # Activity burst: current notional/sec vs preceding 10 sec.
    cur_notional = sum(abs(x[2]) for x in w5) / 5.0
    prev_notional = sum(abs(x[2]) for x in w10) / 10.0
    activity = clip(cur_notional / max(prev_notional, 1e-9) - 1.0, -1.0, 2.0)

    b = BOOK.get(sym, {})
    book = float(b.get("imb", 0.0))
    spread = float(b.get("spread", 1.0))

    return {
        "cvd":cvd, "prev":prev, "impulse":impulse, "m5":m5,
        "activity":activity, "book":book, "spread":spread,
        "price":p1
    }

def directional_score(m, side):
    d = 1 if side == "LONG" else -1
    cvd = max(0.0, m["cvd"] * d)
    book = max(0.0, m["book"] * d)
    m5 = max(0.0, m["m5"] * d)
    imp = max(0.0, m["impulse"] * d)
    act = max(0.0, m["activity"])

    # Each component is bounded. No more "everything is automatically 100".
    score = (
        25 * clip(cvd) +
        20 * clip(book / 0.30) +
        25 * clip(m5) +
        20 * clip(imp) +
        10 * clip(act / 1.0)
    )
    return score

def entry_signal(sym, m):
    # Evaluate both sides, then require independent confluence.
    best_side = None
    best_score = 0.0

    for side in ("LONG", "SHORT"):
        d = 1 if side == "LONG" else -1
        aligned = [
            m["cvd"] * d >= 0.55,
            m["book"] * d >= 0.12,
            m["m5"] * d >= 0.18,
            m["impulse"] * d >= 0.25,
        ]
        score = directional_score(m, side)

        # Avoid buying/selling after an already exhausted burst.
        late_chase = (m["m5"] * d > 0.95 and m["cvd"] * d > 0.90)
        expected_move = max(abs(m["m5"]), abs(m["impulse"])) * 0.0025
        roundtrip_cost = 2 * (TAKER_FEE + SLIPPAGE)

        if sum(aligned) >= 3 and score >= ENTRY_SCORE and not late_chase:
            if m["spread"] <= MAX_SPREAD and expected_move >= roundtrip_cost + 0.00010:
                if score > best_score:
                    best_side, best_score = side, score

    if best_side:
        return best_side, best_score
    return None, max(directional_score(m,"LONG"), directional_score(m,"SHORT"))

def current_risk_dollars():
    return sum(p["risk"] for p in POSITIONS.values())

def open_position(sym, side, score, price):
    global LAST_ERROR
    if sym in POSITIONS:
        return
    if len(POSITIONS) >= MAX_POSITIONS:
        return
    if now() < COOLDOWN.get(sym, 0):
        return
    if BALANCE <= START_BALANCE * (1 - MAX_SESSION_LOSS_PCT):
        STATS["blocked"] += 1
        return

    risk_budget = BALANCE * RISK_PCT
    if current_risk_dollars() + risk_budget > BALANCE * MAX_TOTAL_RISK_PCT:
        STATS["blocked"] += 1
        return

    stop_distance = price * SL_PCT
    if stop_distance <= 0:
        return

    notional = risk_budget / SL_PCT
    max_notional = BALANCE * LEVERAGE
    notional = min(notional, max_notional)

    qty = notional / price
    if qty <= 0:
        return

    POSITIONS[sym] = {
        "side":side, "entry":price, "qty":qty, "notional":notional,
        "risk":risk_budget, "score":score, "opened":now()
    }
    log_event(f"ENTRY {side} {sym} score={score:.1f} px={price:.6f}")

def close_position(sym, exit_price, reason):
    global BALANCE, EQUITY, REALIZED_PNL, GROSS_PNL, FEES, SLIPPAGE_COST
    p = POSITIONS.pop(sym, None)
    if not p:
        return

    sign = 1 if p["side"] == "LONG" else -1
    raw = (exit_price - p["entry"]) / p["entry"] * sign * p["notional"]
    fee = p["notional"] * 2 * TAKER_FEE
    slip = p["notional"] * 2 * SLIPPAGE
    net = raw - fee - slip

    BALANCE += net
    REALIZED_PNL += net
    GROSS_PNL += raw
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
        if net > 0: STATS["long_wins"] += 1
    else:
        STATS["short_trades"] += 1
        STATS["short_pnl"] += net
        if net > 0: STATS["short_wins"] += 1

    STATS[reason.lower()] = STATS.get(reason.lower(), 0) + 1
    COOLDOWN[sym] = now() + COOLDOWN_SEC

    log_event(
        f"EXIT {p['side']} {sym} {reason} net={net:+.2f} "
        f"gross={raw:+.2f} fee={fee:.2f}"
    )

def manage_positions():
    global EQUITY
    unreal = 0.0
    for sym, p in list(POSITIONS.items()):
        px = LAST_PRICE.get(sym)
        if not px:
            continue
        sign = 1 if p["side"] == "LONG" else -1
        ret = (px - p["entry"]) / p["entry"] * sign
        unreal += ret * p["notional"]

        if ret >= TP_PCT:
            close_position(sym, px, "TP")
            continue
        if ret <= -SL_PCT:
            close_position(sym, px, "SL")
            continue

        # Time stop only for dead trades. Strong moves get room to reach TP.
        if now() - p["opened"] >= TIME_STOP_SEC and abs(ret) < 0.0010:
            close_position(sym, px, "TIME")

    EQUITY = BALANCE + unreal

def process_symbol(sym):
    global LAST_CALC
    m = metrics(sym)
    if not m:
        return

    side, score = entry_signal(sym, m)
    LAST_CALC += 1

    if sym in POSITIONS:
        # Exit early if the same market develops strong opposite pressure.
        p = POSITIONS[sym]
        d = 1 if p["side"] == "LONG" else -1
        opp = directional_score(m, "SHORT" if p["side"]=="LONG" else "LONG")
        own = directional_score(m, p["side"])
        if opp >= 78 and opp > own + 12:
            close_position(sym, m["price"], "REVERSAL")
        return

    if side:
        open_position(sym, side, score, m["price"])

def ws_loop(symbols):
    global WS_STATE, LAST_ERROR
    topics = []
    for s in symbols:
        topics.append(f"publicTrade.{s}")
        topics.append(f"orderbook.50.{s}")

    def on_open(ws):
        global WS_STATE
        WS_STATE = "CONNECTED"
        for group in chunks(topics, 50):
            ws.send(json.dumps({"op":"subscribe","args":group}))
        log_event(f"WS CONNECTED topics={len(topics)}")

    def on_message(ws, raw):
        global LAST_ERROR
        try:
            msg = json.loads(raw)
            topic = msg.get("topic","")
            data = msg.get("data", [])
            if topic.startswith("publicTrade."):
                sym = topic.split(".",1)[1]
                for x in data:
                    price = float(x["p"])
                    qty = float(x["v"])
                    side = x.get("S","Buy")
                    signed = price * qty * (1 if side == "Buy" else -1)
                    ts = float(x.get("T", int(now()*1000))) / 1000
                    add_trade(sym, ts, price, signed)
            elif topic.startswith("orderbook.50."):
                sym = topic.split(".")[-1]
                if isinstance(data, dict):
                    normalize_book(sym, data)

            # Process a bounded set each tick to keep the phone-hosted service light.
            for s in symbols[:MONITOR_N]:
                process_symbol(s)
            manage_positions()

        except Exception as e:
            LAST_ERROR = repr(e)

    def on_error(ws, err):
        global WS_STATE, LAST_ERROR
        WS_STATE = "ERROR"
        LAST_ERROR = repr(err)

    def on_close(ws, code, msg):
        global WS_STATE
        WS_STATE = "DISCONNECTED"

    while True:
        try:
            ws = WebSocketApp(
                WS, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            LAST_ERROR = repr(e)
        WS_STATE = "RECONNECTING"
        time.sleep(3)

def dashboard():
    return f"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit Micro Scalper v9</title>
<style>
body{{font-family:system-ui;background:#111;color:#eee;margin:14px}}
.card{{background:#1b1b1b;padding:12px;border-radius:12px;margin:8px 0}}
h2{{margin:0 0 8px}} .big{{font-size:25px;font-weight:700}}
small{{color:#aaa}} table{{width:100%;font-size:12px}}
td{{padding:3px;border-bottom:1px solid #333}}
.good{{color:#6ee7b7}} .bad{{color:#fb7185}}
</style>
</head>
<body>
<h2>⚡ Bybit Micro Scalper v9</h2>
<div class="card">
<div class="big">${BALANCE:.2f}</div>
Equity: ${EQUITY:.2f}<br>
Net PnL: ${REALIZED_PNL:+.2f}<br>
Gross: ${GROSS_PNL:+.2f} · Fees: ${FEES:.2f} · Slip: ${SLIPPAGE_COST:.2f}
</div>
<div class="card">
WS: <b>{WS_STATE}</b><br>
Positions: <b>{len(POSITIONS)}/{MAX_POSITIONS}</b><br>
Calculations: {LAST_CALC}<br>
Monitor: {min(MONITOR_N, len(SYMBOLS))}
</div>
<div class="card">
Trades {STATS['trades']} · W/L {STATS['wins']}/{STATS['losses']}<br>
TP {STATS['tp']} · SL {STATS['sl']} · TIME {STATS['time']} · REV {STATS['reversal']}<br>
LONG {STATS['long_trades']} / wins {STATS['long_wins']} / pnl {STATS['long_pnl']:+.2f}<br>
SHORT {STATS['short_trades']} / wins {STATS['short_wins']} / pnl {STATS['short_pnl']:+.2f}<br>
Blocked: {STATS['blocked']}
</div>
<div class="card">
Risk/trade: {RISK_PCT*100:.2f}% · Total risk cap: {MAX_TOTAL_RISK_PCT*100:.1f}%<br>
TP {TP_PCT*100:.2f}% · SL {SL_PCT*100:.2f}% · Score ≥ {ENTRY_SCORE:.0f}<br>
Taker fee assumption: {TAKER_FEE*100:.3f}% · Slippage/side: {SLIPPAGE*100:.3f}%
</div>
<div class="card">
<b>Recent events</b>
<table>
{''.join('<tr><td>'+e+'</td></tr>' for e in list(EVENTS)[:30])}
</table>
</div>
<div class="card"><small>{LAST_ERROR}</small></div>
<script>setTimeout(()=>location.reload(),3000)</script>
</body></html>
"""

@app.get("/")
def root():
    return dashboard()

@app.get("/api")
def api():
    return jsonify({
        "balance":BALANCE, "equity":EQUITY, "pnl":REALIZED_PNL,
        "gross":GROSS_PNL, "fees":FEES, "slippage":SLIPPAGE_COST,
        "ws":WS_STATE, "positions":len(POSITIONS),
        "stats":STATS, "error":LAST_ERROR
    })

SYMBOLS = []

def main():
    global SYMBOLS
    SYMBOLS = fetch_top_symbols()
    if not SYMBOLS:
        # Keep service alive even if the first REST call fails.
        SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT"]

    log_event(f"START v9 universe={len(SYMBOLS)} monitor={min(MONITOR_N,len(SYMBOLS))}")
    threading.Thread(target=ws_loop, args=(SYMBOLS,), daemon=True).start()

    port = int(os.getenv("PORT","8080"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
