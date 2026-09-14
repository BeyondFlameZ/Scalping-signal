import os,time,json,asyncio
from collections import defaultdict,deque
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,JSONResponse
import httpx,websockets,uvicorn
BAL=float(os.getenv("START_BALANCE","150")); RISK=float(os.getenv("RISK_PCT",".02")); MAXP=int(os.getenv("MAX_POSITIONS","30")); TOP=int(os.getenv("TOP_N","300")); MON=int(os.getenv("MONITOR_N","100")); TP=float(os.getenv("TP_PCT",".006")); SL=float(os.getenv("SL_PCT",".0025")); TSTOP=float(os.getenv("TIME_STOP_SEC","180")); COOL=float(os.getenv("COOLDOWN_SEC","60")); TH=float(os.getenv("SIGNAL_SCORE","72")); PORT=int(os.getenv("PORT","8080"))
app=FastAPI(); S={"balance":BAL,"equity":BAL,"positions":{},"events":deque(maxlen=100),"symbols":[],"ws":"DISCONNECTED","last_market":0,"last_error":"","trades":0,"wins":0,"losses":0,"pnl":0,"candidates":0,"long_setups":0,"short_setups":0}; F=defaultdict(lambda:deque(maxlen=300)); P={}; B=defaultdict(lambda:{"imb":0,"px":0}); last=defaultdict(float); diag={}
def clip(x): return max(-1,min(1,x))
def calc(s):
 f=F[s]
 if len(f)<5:return
 n=time.time(); r=[x for x in f if n-x[0]<=20]; q=[x for x in f if 20<n-x[0]<=60]
 if len(r)<3:return
 rv=sum(x[1] for x in r); cv=clip(rv/(sum(abs(x[1]) for x in r)+1e-9)); old=abs(sum(x[1] for x in q))/max(1,len(q)); cur=abs(rv)/len(r); acc=cur/(old+1e-9); px=P[s]; b=B[s]; mv=px/(b["px"] or px)-1
 sc=(28 if cv>.45 else -28 if cv<-.45 else 0)+(18 if acc>1.5 and rv>0 else -18 if acc>1.5 and rv<0 else 0)+22*clip(b["imb"])+(18 if mv>.001 else -18 if mv<-.001 else 0)+(14 if abs(cv)>.65 else 0)
 return {"score":sc,"cvd":cv,"accel":acc,"imb":b["imb"],"move":mv,"flow":rv}
def ev(x):S["events"].appendleft(x)
def openp(s,side,px,sc):
 if s in S["positions"] or len(S["positions"])>=MAXP or time.time()-last[s]<COOL:return
 qty=S["balance"]*RISK/(px*SL); S["positions"][s]={"symbol":s,"side":side,"entry":px,"qty":qty,"opened":time.time(),"score":round(sc,1)}; last[s]=time.time(); ev({"t":time.time(),"event":"ENTRY","symbol":s,"side":side,"price":px,"score":round(sc,1)})
def closep(s,reason,px):
 p=S["positions"].pop(s,None)
 if not p:return
 pnl=(px-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1)-(p["entry"]+px)*p["qty"]*.00055
 S["balance"]+=pnl; S["pnl"]+=pnl; S["trades"]+=1; S["wins"]+=pnl>=0; S["losses"]+=pnl<0; ev({"t":time.time(),"event":reason,"symbol":s,"side":p["side"],"price":px,"pnl":round(pnl,2)})
async def trade(x):
 s=x.get("s"); px=float(x.get("p") or 0); v=float(x.get("v") or 0)
 if not s or not px or not v:return
 P[s]=px; S["last_market"]=time.time(); F[s].append((time.time(),v*px*(1 if x.get("S")=="Buy" else -1))); z=calc(s)
 if z:
  diag[s]=z; S["candidates"]+=1
  if z["score"]>=TH:S["long_setups"]+=1
  if z["score"]<=-TH:S["short_setups"]+=1
  if s not in S["positions"]:
   if z["score"]>=TH:openp(s,"LONG",px,z["score"])
   elif z["score"]<=-TH:openp(s,"SHORT",px,z["score"])
 p=S["positions"].get(s)
 if p:
  ret=(px/p["entry"]-1)*(1 if p["side"]=="LONG" else -1)
  if ret>=TP:closep(s,"TP",px)
  elif ret<=-SL:closep(s,"SL",px)
  elif time.time()-p["opened"]>=TSTOP:closep(s,"TIME",px)
async def loop():
 while 1:
  try:
   async with httpx.AsyncClient(timeout=15) as c:d=(await c.get("https://api.bybit.com/v5/market/tickers?category=linear")).json()
   a=sorted([(float(x.get("turnover24h") or 0),x["symbol"]) for x in d["result"]["list"] if x["symbol"].endswith("USDT")],reverse=True); S["symbols"]=[x[1] for x in a[:TOP]]
   async with websockets.connect("wss://stream.bybit.com/v5/public/linear",ping_interval=20,ping_timeout=10) as w:
    S["ws"]="CONNECTED"
    for i in range(0,MON,10):await w.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{s}" for s in S["symbols"][i:i+10]]}));await asyncio.sleep(.15)
    async for m in w:
     d=json.loads(m)
     if d.get("topic","").startswith("publicTrade."):
      for x in d["data"]:await trade(x)
  except Exception as e:S["ws"]="DISCONNECTED";S["last_error"]=str(e);await asyncio.sleep(3)
@app.get("/api/status")
def status():
 eq=S["balance"]
 for s,p in S["positions"].items():eq+=(P.get(s,p["entry"])-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1)
 z=sorted(diag.items(),key=lambda x:abs(x[1]["score"]),reverse=True)[:15]
 return JSONResponse({"balance":round(S["balance"],2),"equity":round(eq,2),"pnl":round(S["pnl"],2),"ws":S["ws"],"positions":list(S["positions"].values()),"position_count":len(S["positions"]),"max_positions":MAXP,"monitor":MON,"candidates":S["candidates"],"long_setups":S["long_setups"],"short_setups":S["short_setups"],"trades":S["trades"],"wins":S["wins"],"losses":S["losses"],"last_market":S["last_market"],"last_error":S["last_error"],"diagnostics":[{"symbol":k,**v} for k,v in z],"events":list(S["events"])[:30]})
@app.get("/",response_class=HTMLResponse)
def home():
 return """<meta name=viewport content="width=device-width,initial-scale=1"><style>body{font:14px system-ui;background:#111;color:#eee;margin:14px}.box{border:1px solid #333;border-radius:12px;padding:12px;margin:8px 0}table{width:100%;font-size:11px}td,th{padding:4px;text-align:right}td:first-child,th:first-child{text-align:left}pre{font-size:11px}</style><h2>⚡ Micro Scalper — DIAGNOSTIC</h2><div id=a class=box></div><div class=box><b>Live scores</b><div style="overflow:auto"><table id=t></table></div></div><div class=box><b>Events</b><pre id=e></pre></div><script>async function g(){let x=await fetch('/api/status').then(r=>r.json());a.innerHTML=`Balance $${x.balance} | Equity $${x.equity} | PnL $${x.pnl}<br>WS ${x.ws} | Positions ${x.position_count}/${x.max_positions} | Monitor ${x.monitor}<br>Calculations ${x.candidates} | LONG ${x.long_setups} | SHORT ${x.short_setups}<br>Trades ${x.trades} W/L ${x.wins}/${x.losses}<br>Last ${x.last_market?new Date(x.last_market*1000).toLocaleTimeString():'-'} | Error ${x.last_error||'-'}`;t.innerHTML='<tr><th>Symbol</th><th>Score</th><th>CVD</th><th>Book</th><th>Move</th><th>Accel</th></tr>'+x.diagnostics.map(z=>`<tr><td>${z.symbol}</td><td>${z.score.toFixed(1)}</td><td>${z.cvd.toFixed(2)}</td><td>${z.imb.toFixed(2)}</td><td>${(z.move*100).toFixed(3)}%</td><td>${z.accel.toFixed(2)}</td></tr>`).join('');e.textContent=x.events.map(z=>new Date(z.t*1000).toLocaleTimeString()+' '+z.event+' '+z.symbol+' '+z.side+' '+(z.price||'')+' score='+(z.score||'')+' pnl='+(z.pnl??'')).join('\n')}g();setInterval(g,1000)</script>"""
@app.on_event("startup")
async def start():asyncio.create_task(loop())
