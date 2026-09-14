
import os, json, time, threading
from collections import defaultdict, deque
from flask import Flask, jsonify
import requests
from websocket import WebSocketApp

# ============================================================
# BYBIT MICRO SCALPER v10
# Event-driven paper scalper:
#   1) liquidity sweep + reversal
#   2) absorption + reversal
#   3) impulse + pullback + continuation
#
# The score is NOT an entry trigger. A concrete event is required.
# Paper only: this program never sends private/order requests.
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
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "90"))

TAKER_FEE = float(os.getenv("TAKER_FEE", "0.00055"))
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.00015"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.0012"))

MIN_EVENT_SCORE = float(os.getenv("MIN_EVENT_SCORE", "72"))
EVAL_INTERVAL = float(os.getenv("EVAL_INTERVAL", "0.50"))

app = Flask(__name__)

BALANCE = START_BALANCE
EQUITY = START_BALANCE
REALIZED_PNL = 0.0
GROSS_PNL = 0.0
FEES = 0.0
SLIPPAGE_COST = 0.0

T = defaultdict(lambda: deque(maxlen=2200))
BOOK = {}
LAST_PRICE = {}
LAST_EVAL = defaultdict(float)
COOLDOWN = {}
POSITIONS = {}
EVENTS = deque(maxlen=100)
SIGNAL_STATS = defaultdict(lambda: {"n":0, "wins":0, "pnl":0.0})

LAST_ERROR = ""
WS_STATE = "DISCONNECTED"
CALCULATIONS = 0
BLOCKED = 0
SYMBOLS = []

STATS = {
    "trades":0, "wins":0, "losses":0,
    "tp":0, "sl":0, "time":0, "reversal":0,
    "long_trades":0, "long_wins":0, "long_pnl":0.0,
    "short_trades":0, "short_wins":0, "short_pnl":0.0,
}

def clip(x, a=-1.0, b=1.0):
    return max(a, min(b, x))

def now():
    return time.time()

def log(s):
    EVENTS.appendleft(time.strftime("%H:%M:%S ") + s)

def fetch_top_symbols():
    try:
        r = requests.get(API + "/v5/market/tickers",
                         params={"category":"linear"}, timeout=8)
        rows = []
        for x in r.json().get("result", {}).get("list", []):
            sym = x.get("symbol","")
            if not sym.endswith("USDT"):
                continue
            try:
                turnover = float(x.get("turnover24h",0) or 0)
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
        yield xs[i:i+n]

def update_book(sym, data):
    try:
        bids = data.get("b",[]) or []
        asks = data.get("a",[]) or []
        if not bids or not asks:
            return
        bid = float(bids[0][0])
        ask = float(asks[0][0])
        bq = sum(float(x[1]) for x in bids[:10])
        aq = sum(float(x[1]) for x in asks[:10])
        mid = (bid + ask) / 2
        if mid <= 0:
            return
        BOOK[sym] = {
            "bid":bid, "ask":ask, "mid":mid,
            "imb":clip((bq-aq)/max(bq+aq,1e-12)),
            "spread":(ask-bid)/mid,
            "ts":now()
        }
    except Exception:
        pass

def add_trade(sym, ts, price, signed_notional):
    T[sym].append((ts, price, signed_notional))
    LAST_PRICE[sym] = price

def window(sym, seconds):
    cutoff = now() - seconds
    return [x for x in T[sym] if x[0] >= cutoff]

def signed_ratio(rows):
    if not rows:
        return 0.0
    buy = sum(max(0.0,x[2]) for x in rows)
    sell = sum(max(0.0,-x[2]) for x in rows)
    return clip((buy-sell)/max(buy+sell,1e-9))

def activity_ratio(cur, prev):
    a = sum(abs(x[2]) for x in cur) / max(len(cur),1)
    b = sum(abs(x[2]) for x in prev) / max(len(prev),1)
    return a/max(b,1e-9)

def price_change(rows):
    if len(rows) < 2:
        return 0.0
    return (rows[-1][1]-rows[0][1])/max(rows[0][1],1e-12)

def features(sym):
    q = T[sym]
    if len(q) < 12:
        return None

    w5 = window(sym,5)
    w10 = window(sym,10)
    w15 = window(sym,15)
    w30 = window(sym,30)
    w60 = window(sym,60)

    if len(w5) < 6 or len(w15) < 10:
        return None

    prev5 = [x for x in w10 if x[0] < now()-5]
    cvd5 = signed_ratio(w5)
    cvd_prev = signed_ratio(prev5)
    impulse = clip(cvd5-cvd_prev)

    mom5 = clip(price_change(w5)/0.0025)
    mom15 = clip(price_change(w15)/0.0040)
    mom30 = clip(price_change(w30)/0.0060)

    act = activity_ratio(w5, prev5)
    b = BOOK.get(sym,{})
    spread = float(b.get("spread",1.0))
    book = float(b.get("imb",0.0))
    px = w5[-1][1]

    # Local extrema excluding the newest 2 seconds.
    prior = [x for x in w60 if x[0] < now()-2]
    high = max((x[1] for x in prior), default=px)
    low = min((x[1] for x in prior), default=px)

    return {
        "price":px, "cvd":cvd5, "cvd_prev":cvd_prev, "impulse":impulse,
        "mom5":mom5, "mom15":mom15, "mom30":mom30,
        "activity":act, "book":book, "spread":spread,
        "high":high, "low":low,
        "w5":w5, "w10":w10, "w15":w15, "w30":w30, "w60":w60
    }

def event_signal(sym, f):
    """
    Returns (side, score, event_name) or (None, score, event_name/None).
    Event detection is deliberately independent from the final score.
    """
    px = f["price"]
    spread_ok = f["spread"] <= MAX_SPREAD
    cost = 2*(TAKER_FEE+SLIPPAGE)

    # ---------- 1. LIQUIDITY SWEEP + REVERSAL ----------
    # High sweep: price first exceeded the prior 60s high, then returned
    # below it while aggressive buying weakened/reversed.
    high_sweep = px > f["high"] * 1.00035
    low_sweep = px < f["low"] * 0.99965

    # We need evidence of a reversal, not merely a wick.
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
    # Strong aggressive flow with very little price displacement suggests
    # the flow is being absorbed. Entry happens only after price starts
    # moving in the opposite direction.
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
    # A prior move, a modest retrace, then flow/price re-acceleration.
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
        return None, 0.0, None

    # Evaluate the strongest candidate.
    best = None
    best_score = -1
    for side, event, base in candidates:
        d = 1 if side == "LONG" else -1

        # Confirmation components. These add to the event base.
        book_pts = 12 * clip((f["book"]*d)/0.30, 0, 1)
        flow_pts = 18 * clip((f["cvd"]*d)/0.70, 0, 1)
        price_pts = 15 * clip((f["mom5"]*d)/0.45, 0, 1)
        imp_pts = 10 * clip((f["impulse"]*d)/0.45, 0, 1)
        act_pts = 8 * clip((f["activity"]-1.0)/1.0, 0, 1)

        score = base + book_pts + flow_pts + price_pts + imp_pts + act_pts

        # Do not enter if the move has already become too large relative
        # to a 0.60% target.
        late = abs(f["mom5"]) > 0.90
        expected = max(abs(f["mom5"]), abs(f["mom15"])) * 0.0025

        if late:
            continue
        if expected < cost + 0.00010:
            continue
        if score >= MIN_EVENT_SCORE and score > best_score:
            best = (side,event,score)
            best_score = score

    if best:
        return best
    return None, 0.0, None

def open_position(sym, side, score, event, price):
    global BLOCKED
    if sym in POSITIONS or len(POSITIONS) >= MAX_POSITIONS:
        BLOCKED += 1
        return
    if now() < COOLDOWN.get(sym,0):
        BLOCKED += 1
        return
    if BALANCE <= START_BALANCE*(1-MAX_SESSION_LOSS_PCT):
        BLOCKED += 1
        return

    risk = BALANCE*RISK_PCT
    if sum(x["risk"] for x in POSITIONS.values()) + risk > BALANCE*MAX_TOTAL_RISK_PCT:
        BLOCKED += 1
        return

    notional = min(risk/SL_PCT, BALANCE*LEVERAGE)
    if notional <= 0:
        BLOCKED += 1
        return

    POSITIONS[sym] = {
        "side":side, "entry":price, "qty":notional/price,
        "notional":notional, "risk":risk,
        "score":score, "event":event, "opened":now()
    }
    SIGNAL_STATS[event]["n"] += 1
    log(f"ENTRY {side} {sym} {event} score={score:.1f} px={price:.6g}")

def close_position(sym, price, reason):
    global BALANCE, EQUITY, REALIZED_PNL, GROSS_PNL, FEES, SLIPPAGE_COST
    p = POSITIONS.pop(sym,None)
    if not p:
        return

    d = 1 if p["side"]=="LONG" else -1
    gross = (price-p["entry"])/p["entry"]*d*p["notional"]
    fee = p["notional"]*2*TAKER_FEE
    slip = p["notional"]*2*SLIPPAGE
    net = gross-fee-slip

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

    if p["side"]=="LONG":
        STATS["long_trades"] += 1
        STATS["long_pnl"] += net
        if net > 0: STATS["long_wins"] += 1
    else:
        STATS["short_trades"] += 1
        STATS["short_pnl"] += net
        if net > 0: STATS["short_wins"] += 1

    STATS[reason.lower()] += 1
    SIGNAL_STATS[p["event"]]["pnl"] += net
    if net > 0:
        SIGNAL_STATS[p["event"]]["wins"] += 1

    COOLDOWN[sym] = now()+COOLDOWN_SEC
    log(f"EXIT {p['side']} {sym} {reason} event={p['event']} "
        f"net={net:+.2f} gross={gross:+.2f} fee={fee:.2f}")

def manage_positions():
    global EQUITY
    unreal = 0.0
    for sym,p in list(POSITIONS.items()):
        px = LAST_PRICE.get(sym)
        if not px:
            continue
        d = 1 if p["side"]=="LONG" else -1
        ret = (px-p["entry"])/p["entry"]*d
        unreal += ret*p["notional"]

        if ret >= TP_PCT:
            close_position(sym,px,"TP")
            continue
        if ret <= -SL_PCT:
            close_position(sym,px,"SL")
            continue

        if now()-p["opened"] >= TIME_STOP_SEC and abs(ret) < 0.0010:
            close_position(sym,px,"TIME")

    EQUITY = BALANCE+unreal

def evaluate(sym):
    global CALCULATIONS
    t = now()
    if t-LAST_EVAL[sym] < EVAL_INTERVAL:
        return
    LAST_EVAL[sym] = t

    f = features(sym)
    if not f:
        return
    CALCULATIONS += 1

    # Existing positions: reversal is a management event, not a new entry.
    if sym in POSITIONS:
        p = POSITIONS[sym]
        d = 1 if p["side"]=="LONG" else -1
        opp = f["cvd"]*(-d)
        own = f["cvd"]*d
        if opp > 0.60 and opp > own+0.35:
            close_position(sym,f["price"],"REVERSAL")
        return

    side, score, event = event_signal(sym,f)
    if side:
        open_position(sym,side,score,event,f["price"])

def ws_worker(symbols):
    global WS_STATE, LAST_ERROR

    topics = []
    for s in symbols:
        topics.append(f"publicTrade.{s}")
        topics.append(f"orderbook.50.{s}")

    def on_open(ws):
        global WS_STATE
        WS_STATE = "CONNECTED"
        for group in chunks(topics,50):
            ws.send(json.dumps({"op":"subscribe","args":group}))
        log(f"WS CONNECTED topics={len(topics)}")

    def on_message(ws,raw):
        global LAST_ERROR
        try:
            m = json.loads(raw)
            topic = m.get("topic","")
            data = m.get("data",[])

            if topic.startswith("publicTrade."):
                sym = topic.split(".",1)[1]
                for x in data:
                    px = float(x["p"])
                    qty = float(x["v"])
                    side = x.get("S","Buy")
                    signed = px*qty*(1 if side=="Buy" else -1)
                    ts = float(x.get("T",int(now()*1000)))/1000
                    add_trade(sym,ts,px,signed)

            elif topic.startswith("orderbook.50."):
                sym = topic.split(".")[-1]
                if isinstance(data,dict):
                    update_book(sym,data)

            # Evaluate only the monitor set, at most twice per second/symbol.
            for s in symbols[:MONITOR_N]:
                evaluate(s)
            manage_positions()

        except Exception as e:
            LAST_ERROR = repr(e)

    def on_error(ws,err):
        global WS_STATE,LAST_ERROR
        WS_STATE = "ERROR"
        LAST_ERROR = repr(err)

    def on_close(ws,code,msg):
        global WS_STATE
        WS_STATE = "DISCONNECTED"

    while True:
        try:
            ws = WebSocketApp(WS_URL,on_open=on_open,on_message=on_message,
                              on_error=on_error,on_close=on_close)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            LAST_ERROR = repr(e)
        WS_STATE = "RECONNECTING"
        time.sleep(3)

def fmt_signal_stats():
    rows = []
    for k,v in sorted(SIGNAL_STATS.items(), key=lambda z:z[1]["n"], reverse=True):
        if v["n"]:
            wr = 100*v["wins"]/max(v["n"],1)
            rows.append(f"<tr><td>{k}</td><td>{v['n']}</td><td>{wr:.1f}%</td><td>{v['pnl']:+.2f}</td></tr>")
    return "".join(rows) or "<tr><td colspan=4>No closed event trades yet</td></tr>"

def dashboard():
    wr = 100*STATS["wins"]/max(STATS["trades"],1)
    return f"""
<!doctype html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bybit Micro Scalper v10</title>
<style>
body{{font-family:system-ui;background:#111;color:#eee;margin:14px}}
.card{{background:#1b1b1b;padding:12px;border-radius:12px;margin:8px 0}}
.big{{font-size:27px;font-weight:700}}
table{{width:100%;font-size:12px;border-collapse:collapse}}
td{{padding:4px;border-bottom:1px solid #333}}
small{{color:#aaa}}
</style></head><body>
<h2>⚡ Bybit Micro Scalper v10</h2>
<div class="card"><div class="big">${BALANCE:.2f}</div>
Equity ${EQUITY:.2f}<br>
Net PnL ${REALIZED_PNL:+.2f}<br>
Gross ${GROSS_PNL:+.2f} · Fees ${FEES:.2f} · Slip ${SLIPPAGE_COST:.2f}</div>

<div class="card">WS <b>{WS_STATE}</b><br>
Positions <b>{len(POSITIONS)}/{MAX_POSITIONS}</b><br>
Calculations {CALCULATIONS} · Monitor {min(MONITOR_N,len(SYMBOLS))}<br>
Blocked {BLOCKED}</div>

<div class="card">Trades {STATS['trades']} · W/L {STATS['wins']}/{STATS['losses']} · WR {wr:.1f}%<br>
TP {STATS['tp']} · SL {STATS['sl']} · TIME {STATS['time']} · REV {STATS['reversal']}<br>
LONG {STATS['long_trades']} / wins {STATS['long_wins']} / pnl {STATS['long_pnl']:+.2f}<br>
SHORT {STATS['short_trades']} / wins {STATS['short_wins']} / pnl {STATS['short_pnl']:+.2f}</div>

<div class="card">
Risk {RISK_PCT*100:.2f}% · Total risk {MAX_TOTAL_RISK_PCT*100:.1f}% · Max positions {MAX_POSITIONS}<br>
TP {TP_PCT*100:.2f}% · SL {SL_PCT*100:.2f}% · Event score ≥ {MIN_EVENT_SCORE:.0f}<br>
Taker {TAKER_FEE*100:.3f}% · Slip/side {SLIPPAGE*100:.3f}%
</div>

<div class="card"><b>Event performance</b>
<table><tr><td>Event</td><td>N</td><td>WR</td><td>PnL</td></tr>
{fmt_signal_stats()}</table></div>

<div class="card"><b>Open positions</b><table>
{''.join(f"<tr><td>{s}</td><td>{p['side']}</td><td>{p['event']}</td><td>{p['score']:.0f}</td></tr>" for s,p in list(POSITIONS.items())[:30]) or '<tr><td>None</td></tr>'}
</table></div>

<div class="card"><b>Recent events</b><table>
{''.join(f'<tr><td>{e}</td></tr>' for e in list(EVENTS)[:35])}
</table></div>
<div class="card"><small>{LAST_ERROR}</small></div>
<script>setTimeout(()=>location.reload(),3000)</script>
</body></html>"""

@app.get("/")
def root():
    return dashboard()

@app.get("/api")
def api():
    return jsonify({
        "version":"v10","paper_only":True,
        "balance":BALANCE,"equity":EQUITY,"pnl":REALIZED_PNL,
        "gross":GROSS_PNL,"fees":FEES,"slippage":SLIPPAGE_COST,
        "ws":WS_STATE,"positions":len(POSITIONS),
        "calculations":CALCULATIONS,"blocked":BLOCKED,
        "stats":STATS,"events":dict(SIGNAL_STATS),"error":LAST_ERROR
    })

def main():
    global SYMBOLS
    SYMBOLS = fetch_top_symbols()
    if not SYMBOLS:
        SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT"]

    log(f"START v10 universe={len(SYMBOLS)} monitor={min(MONITOR_N,len(SYMBOLS))}")
    threading.Thread(target=ws_worker,args=(SYMBOLS,),daemon=True).start()
    port = int(os.getenv("PORT","8080"))
    app.run(host="0.0.0.0",port=port)

if __name__ == "__main__":
    main()
