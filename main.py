
import os, time, json, math, asyncio
from collections import defaultdict, deque
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import httpx
import websockets

START_BALANCE=float(os.getenv("START_BALANCE","10000"))
RISK_PCT=float(os.getenv("RISK_PCT","0.02"))
LEVERAGE=float(os.getenv("LEVERAGE","10"))
MAX_POSITIONS=int(os.getenv("MAX_POSITIONS","30"))
TOP_N=int(os.getenv("TOP_N","300"))
MONITOR_N=int(os.getenv("MONITOR_N","80"))
TP_PCT=float(os.getenv("TP_PCT","0.006"))
SL_PCT=float(os.getenv("SL_PCT","0.0025"))
TIME_STOP=float(os.getenv("TIME_STOP_SEC","180"))
COOLDOWN=float(os.getenv("COOLDOWN_SEC","60"))
SIGNAL_SCORE=float(os.getenv("SIGNAL_SCORE","72"))
PORT=int(os.getenv("PORT","8080"))

app=FastAPI()
state={
 "balance":START_BALANCE,"equity":START_BALANCE,"positions":{},
 "signals":deque(maxlen=100),"symbols":[], "ws":"DISCONNECTED",
 "trades":0,"wins":0,"losses":0,"pnl":0.0,"last_error":"",
 "last_market":0
}
books={}
flows=defaultdict(lambda: deque(maxlen=120))
prices={}
last_entry=defaultdict(float)

def clamp(x,a,b): return max(a,min(b,x))

async def rest_symbols():
    url="https://api.bybit.com/v5/market/tickers?category=linear"
    async with httpx.AsyncClient(timeout=15) as c:
        r=(await c.get(url)).json()
    rows=[]
    for x in r.get("result",{}).get("list",[]):
        s=x.get("symbol","")
        if not s.endswith("USDT"): continue
        try: turn=float(x.get("turnover24h") or 0)
        except: turn=0
        rows.append((turn,s))
    rows.sort(reverse=True)
    return [s for _,s in rows[:TOP_N]]

def score(s):
    f=flows[s]
    if len(f)<8: return None
    now=time.time()
    recent=[x for x in f if now-x[0] <= 20]
    old=[x for x in f if 20 < now-x[0] <= 60]
    if len(recent)<3: return None
    rv=sum(x[1] for x in recent); ov=sum(x[1] for x in old)
    cvd=clamp(rv/(sum(abs(x[1]) for x in recent)+1e-9),-1,1)
    accel=clamp((rv/(len(recent))) / (abs(ov)/(len(old))+1e-9),-3,3) if old else 0
    p=prices.get(s,0)
    pts=0
    if cvd>0.45: pts+=28
    elif cvd<-0.45: pts-=28
    if accel>1.5 and rv>0: pts+=18
    elif accel>1.5 and rv<0: pts-=18
    b=books.get(s,{})
    imb=b.get("imb",0)
    pts += 22*clamp(imb,-1,1)
    px0=b.get("px0",p)
    if px0:
        move=(p/px0)-1
        if move>0.001: pts+=18
        elif move<-0.001: pts-=18
    vol=sum(abs(x[1]) for x in recent)
    if vol>0: pts += 14*(1 if abs(cvd)>0.65 else 0)
    return pts, cvd, imb, accel

def open_pos(s,side,px,sc):
    if s in state["positions"] or len(state["positions"])>=MAX_POSITIONS: return
    if time.time()-last_entry[s]<COOLDOWN: return
    risk=state["balance"]*RISK_PCT
    stop_dist=px*SL_PCT
    qty=risk/stop_dist
    # leverage is reflected in notional; risk is still based on stop distance
    notional=qty*px
    pos={"symbol":s,"side":side,"entry":px,"qty":qty,"notional":notional,
         "opened":time.time(),"score":round(sc,1)}
    state["positions"][s]=pos
    last_entry[s]=time.time()
    state["signals"].appendleft({"t":time.time(),"symbol":s,"side":side,"price":px,"score":round(sc,1),"event":"ENTRY"})

def close_pos(s,reason,px):
    p=state["positions"].pop(s,None)
    if not p:return
    raw=(px-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1)
    fee=(p["entry"]*p["qty"]+px*p["qty"])*0.00055
    pnl=raw-fee
    state["balance"]+=pnl; state["pnl"]+=pnl; state["trades"]+=1
    if pnl>=0: state["wins"]+=1
    else: state["losses"]+=1
    state["signals"].appendleft({"t":time.time(),"symbol":s,"side":p["side"],"price":px,"pnl":round(pnl,2),"event":reason})

async def process_trade(x):
    s=x.get("s"); px=float(x.get("p") or 0); q=float(x.get("v") or 0)
    if not s or not px or not q:return
    side=x.get("S")
    signed=q if side=="Buy" else -q
    prices[s]=px; state["last_market"]=time.time()
    flows[s].append((time.time(),signed*px))
    sc=score(s)
    if sc:
        pts,cvd,imb,acc=sc
        if s not in state["positions"]:
            if pts>=SIGNAL_SCORE: open_pos(s,"LONG",px,pts)
            elif pts<=-SIGNAL_SCORE: open_pos(s,"SHORT",px,pts)
    p=state["positions"].get(s)
    if p:
        ret=(px/p["entry"]-1)*(1 if p["side"]=="LONG" else -1)
        if ret>=TP_PCT: close_pos(s,"TP",px)
        elif ret<=-SL_PCT: close_pos(s,"SL",px)
        elif time.time()-p["opened"]>=TIME_STOP: close_pos(s,"TIME",px)

async def subscribe(ws, symbols):
    for i in range(0,len(symbols),10):
        args=[f"publicTrade.{s}" for s in symbols[i:i+10]]
        await ws.send(json.dumps({"op":"subscribe","args":args}))
        await asyncio.sleep(.15)

async def market_loop():
    while True:
        try:
            state["symbols"]=await rest_symbols()
            subs=state["symbols"][:MONITOR_N]
            url="wss://stream.bybit.com/v5/public/linear"
            async with websockets.connect(url,ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
                state["ws"]="CONNECTED"
                await subscribe(ws,subs)
                async for msg in ws:
                    d=json.loads(msg)
                    if d.get("topic","").startswith("publicTrade."):
                        for x in d.get("data",[]): await process_trade(x)
        except Exception as e:
            state["ws"]="DISCONNECTED"; state["last_error"]=str(e)
            await asyncio.sleep(3)

@app.get("/api/status")
def api_status():
    eq=state["balance"]
    for s,p in state["positions"].items():
        px=prices.get(s,p["entry"])
        eq+=(px-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1)
    state["equity"]=eq
    return JSONResponse({
      "balance":round(state["balance"],2),"equity":round(eq,2),
      "positions":list(state["positions"].values()),"position_count":len(state["positions"]),
      "max_positions":MAX_POSITIONS,"signals":list(state["signals"])[:30],
      "symbols":len(state["symbols"]),"monitor":MONITOR_N,"ws":state["ws"],
      "trades":state["trades"],"wins":state["wins"],"losses":state["losses"],
      "pnl":round(state["pnl"],2),"last_market":state["last_market"],"last_error":state["last_error"]
    })

@app.get("/",response_class=HTMLResponse)
def home():
    return """<!doctype html><html><head><meta name=viewport content="width=device-width,initial-scale=1">
<title>Bybit Scalper</title><style>body{font-family:system-ui;background:#111;color:#eee;margin:16px} .box{padding:14px;border:1px solid #333;border-radius:12px;margin:8px 0}pre{white-space:pre-wrap;font-size:12px}</style></head>
<body><h2>⚡ Bybit Micro Scalper — PAPER</h2><div id=a></div><div class=box><b>Recent events</b><pre id=s></pre></div>
<script>
async function go(){let x=await fetch('/api/status').then(r=>r.json());
document.getElementById('a').innerHTML=`<div class=box>Balance: $${x.balance}<br>Equity: $${x.equity}<br>PnL: $${x.pnl}<br>WS: ${x.ws}<br>Positions: ${x.position_count}/${x.max_positions}<br>Monitored: ${x.monitor}<br>Trades: ${x.trades} | W/L: ${x.wins}/${x.losses}<br>Last market: ${x.last_market?new Date(x.last_market*1000).toLocaleTimeString():'-' }<br>Error: ${x.last_error||'-'}</div>`;
document.getElementById('s').textContent=x.signals.map(z=>new Date(z.t*1000).toLocaleTimeString()+' '+z.event+' '+z.symbol+' '+z.side+' '+(z.price||'')+' score='+(z.score||'')+' pnl='+(z.pnl||'')).join('\\n');}
go();setInterval(go,1000)</script></body></html>"""

@app.on_event("startup")
async def startup():
    asyncio.create_task(market_loop())

if __name__=="__main__":
    uvicorn.run(app,host="0.0.0.0",port=PORT)
