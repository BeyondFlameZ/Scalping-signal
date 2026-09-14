import asyncio, json, time, os, math
from collections import defaultdict, deque
from contextlib import asynccontextmanager
import aiohttp, websockets
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

BASE="https://api.bybit.com"
WS="wss://stream.bybit.com/v5/public/linear"
BALANCE=float(os.getenv("START_BALANCE","10000"))
RISK=float(os.getenv("RISK_PCT","0.02"))
MAX_POS=int(os.getenv("MAX_POSITIONS","5"))
TOP_N=int(os.getenv("TOP_N","300"))
DEEP_N=int(os.getenv("DEEP_N","50"))

state={
 "started":time.time(),"balance":BALANCE,"equity":BALANCE,
 "peak":BALANCE,"positions":{}, "trades":[],
 "signals":[],"symbols":[],"last_error":"","ws":False
}
flow=defaultdict(lambda:deque(maxlen=300))
prices={}
tasks=[]

async def get_json(session,path,params):
    async with session.get(BASE+path,params=params,timeout=15) as r:
        x=await r.json()
        if x.get("retCode")!=0: raise RuntimeError(x.get("retMsg","Bybit error"))
        return x["result"]

async def load_universe(session):
    x=await get_json(session,"/v5/market/tickers",{"category":"linear"})
    rows=[]
    for r in x.get("list",[]):
        s=r.get("symbol","")
        if s.endswith("USDT") and r.get("turnover24h"):
            rows.append((s,float(r["turnover24h"])))
    rows.sort(key=lambda x:x[1],reverse=True)
    return [s for s,_ in rows[:TOP_N]]

def score_symbol(s):
    f=list(flow[s])
    if len(f)<20:return None
    cvd=sum(x[1] for x in f[-20:])
    vol=sum(abs(x[1]) for x in f[-20:])+1e-9
    z=cvd/vol
    score=50+35*max(-1,min(1,z))
    return round(score,2),z

def open_paper(s,side,price,score):
    if s in state["positions"] or len(state["positions"])>=MAX_POS:return
    risk_usd=state["equity"]*RISK
    stop_dist=0.006
    qty=risk_usd/(price*stop_dist)
    state["positions"][s]={
      "symbol":s,"side":side,"entry":price,"qty":qty,
      "stop":price*(1-stop_dist if side=="LONG" else 1+stop_dist),
      "target":price*(1+stop_dist*1.5 if side=="LONG" else 1-stop_dist*1.5),
      "score":score,"opened":time.time()
    }

def close_position(s,price,reason):
    p=state["positions"].pop(s,None)
    if not p:return
    direction=1 if p["side"]=="LONG" else -1
    pnl=(price-p["entry"])*p["qty"]*direction
    fee=(price*p["qty"]+p["entry"]*p["qty"])*0.00055
    pnl-=fee
    state["balance"]+=pnl
    state["equity"]=state["balance"]
    state["peak"]=max(state["peak"],state["equity"])
    state["trades"].append({"ts":time.time(),"symbol":s,"side":p["side"],"entry":p["entry"],"exit":price,"pnl":pnl,"reason":reason})

async def market_worker():
    async with aiohttp.ClientSession() as session:
        try:
            state["symbols"]=await load_universe(session)
        except Exception as e:
            state["last_error"]=str(e); return
        # Keep the live stream deliberately smaller than the full ranking universe:
        # Top 50 is the deep-scan layer; the ranking still tracks Top 300.
        subs=[]
        for s in state["symbols"][:DEEP_N]:
            subs.append(f"publicTrade.{s}")
        while True:
            try:
                async with websockets.connect(WS,ping_interval=20,ping_timeout=20,max_size=None) as ws:
                    await ws.send(json.dumps({"op":"subscribe","args":subs}))
                    state["ws"]=True
                    async for raw in ws:
                        m=json.loads(raw)
                        topic=m.get("topic","")
                        if not topic.startswith("publicTrade."):continue
                        for t in m.get("data",[]):
                            s=t["s"]; p=float(t["p"]); v=float(t["v"])
                            signed=p*v if t["S"]=="Buy" else -p*v
                            prices[s]=p; flow[s].append((int(t["T"]),signed))
                            sig=score_symbol(s)
                            if sig:
                                score,z=sig
                                if abs(z)>0.45:
                                    side="LONG" if z>0 else "SHORT"
                                    state["signals"].append({"ts":time.time(),"symbol":s,"side":side,"score":score,"price":p})
                                    state["signals"]=state["signals"][-100:]
                                    if score>=75 or score<=25: open_paper(s,side,p,score)
                            pos=state["positions"].get(s)
                            if pos:
                                if pos["side"]=="LONG" and (p<=pos["stop"] or p>=pos["target"]):
                                    close_position(s,p,"SL" if p<=pos["stop"] else "TP")
                                elif pos["side"]=="SHORT" and (p>=pos["stop"] or p<=pos["target"]):
                                    close_position(s,p,"SL" if p>=pos["stop"] else "TP")
            except Exception as e:
                state["ws"]=False; state["last_error"]=str(e); await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app):
    tasks.append(asyncio.create_task(market_worker()))
    yield
    for t in tasks:t.cancel()

app=FastAPI(lifespan=lifespan)

@app.get("/",response_class=HTMLResponse)
async def home():
    return """<!doctype html><html><head><meta name=viewport content="width=device-width,initial-scale=1">
<title>Bybit Paper Bot</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0b0d10;color:#eee;margin:0;padding:16px}
.card{background:#151920;border-radius:14px;padding:16px;margin-bottom:12px}
h1{font-size:22px}.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
.big{font-size:25px;font-weight:700}table{width:100%;font-size:12px;border-collapse:collapse}td,th{padding:7px;border-bottom:1px solid #292e36;text-align:left}
.ok{color:#62e6a5}.bad{color:#ff6874}.muted{color:#89919e}
</style></head><body><h1>BYBIT PAPER BOT</h1>
<div class=card><div class=grid>
<div>Balance<div id=b class=big>—</div></div><div>Drawdown<div id=dd class=big>—</div></div>
<div>WebSocket<div id=ws class=big>—</div></div><div>Positions<div id=np class=big>—</div></div>
</div></div>
<div class=card><b>Open positions</b><div id=pos>—</div></div>
<div class=card><b>Recent signals</b><div id=sig>—</div></div>
<div class=card><div class=muted>Paper only. No private API key and no real orders.</div></div>
<script>
async function tick(){let x=await (await fetch('/api/status')).json();
b.textContent='$'+x.equity.toFixed(2);dd.textContent=(x.drawdown*100).toFixed(2)+'%';
ws.innerHTML=x.ws?'<span class=ok>CONNECTED</span>':'<span class=bad>OFFLINE</span>';np.textContent=Object.keys(x.positions).length;
pos.innerHTML=Object.values(x.positions).map(p=>`${p.symbol} ${p.side} @ ${p.entry} → SL ${p.stop.toFixed(5)} / TP ${p.target.toFixed(5)}`).join('<br>')||'none';
sig.innerHTML='<table><tr><th>Symbol</th><th>Side</th><th>Score</th><th>Price</th></tr>'+x.signals.slice(-20).reverse().map(s=>`<tr><td>${s.symbol}</td><td>${s.side}</td><td>${s.score}</td><td>${s.price}</td></tr>`).join('')+'</table>';
}setInterval(tick,2000);tick();
</script></body></html>"""

@app.get("/api/status")
async def status():
    dd=(state["peak"]-state["equity"])/state["peak"] if state["peak"] else 0
    return JSONResponse({**state,"drawdown":dd,"uptime":time.time()-state["started"]})
