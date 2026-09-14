import os,time,json,asyncio
from collections import defaultdict,deque
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,JSONResponse
import httpx,websockets,uvicorn
BAL=float(os.getenv("START_BALANCE","150"));RISK=float(os.getenv("RISK_PCT",".02"));MAXP=int(os.getenv("MAX_POSITIONS","30"));MAXR=float(os.getenv("MAX_TOTAL_RISK_PCT",".10"));TOP=int(os.getenv("TOP_N","300"));MON=int(os.getenv("MONITOR_N","100"));TP=float(os.getenv("TP_PCT",".0045"));SL=float(os.getenv("SL_PCT",".0025"));TSTOP=float(os.getenv("TIME_STOP_SEC","120"));COOL=float(os.getenv("COOLDOWN_SEC","60"));TH=float(os.getenv("SIGNAL_SCORE","70"));PORT=int(os.getenv("PORT","8080"))
app=FastAPI();S={"balance":BAL,"pnl":0.0,"positions":{},"events":deque(maxlen=150),"symbols":[],"ws":"DISCONNECTED","last_market":0,"last_error":"","trades":0,"wins":0,"losses":0,"fees":0.0,"long_setups":0,"short_setups":0,"entries_blocked":0};T=defaultdict(lambda:deque(maxlen=600));BOOK=defaultdict(lambda:{"b":{},"a":{},"imb":0.0,"mid":0.0});D={};last=defaultdict(float)
def clip(x):return max(-1,min(1,x))
def metrics(s):
 f=T[s]
 if len(f)<12:return
 n=time.time();r=[x for x in f if n-x[0]<=5];p=[x for x in f if 5<n-x[0]<=15]
 if len(r)<4 or len(p)<3:return
 fr=sum(x[1] for x in r);fp=sum(x[1] for x in p);cv=clip(fr/(sum(abs(x[1]) for x in r)+1e-9));pv=clip(fp/(sum(abs(x[1]) for x in p)+1e-9))
 acc=clip((sum(abs(x[1]) for x in r)/len(r))/(sum(abs(x[1]) for x in p)/len(p)+1e-9)-1)
 p0=r[0][2];p1=r[-1][2];m5=clip((p1/p0-1)/.0025) if p0 else 0
 i0=p[0][2];i1=p[-1][2];imp=clip((i1/i0-1)/.0025) if i0 else 0
 book=BOOK[s]["imb"];return {"cvd":cv,"prev":pv,"accel":acc,"m5":m5,"impulse":imp,"book":book,"flow":fr}
def score(s):
 z=metrics(s)
 if not z:return
 L=28*max(z["cvd"],0)+18*max(z["book"],0)+18*max(z["m5"],0)+14*max(z["impulse"],0)
 H=28*max(-z["cvd"],0)+18*max(-z["book"],0)+18*max(-z["m5"],0)+14*max(-z["impulse"],0)
 if z["impulse"]>.35 and z["cvd"]>.35 and z["m5"]>.15:L+=22
 if z["impulse"]<-.35 and z["cvd"]<-.35 and z["m5"]<-.15:H+=22
 if abs(z["m5"])>.95 and abs(z["cvd"])>.85:
  if z["m5"]>0:L-=18
  else:H-=18
 return clip(L,0,100)-clip(H,0,100),z
def ev(x):S["events"].appendleft(x)
def openp(s,side,px,sc):
 if s in S["positions"] or len(S["positions"])>=MAXP or time.time()-last[s]<COOL:S["entries_blocked"]+=1;return
 used=sum(x["risk"] for x in S["positions"].values());risk=min(S["balance"]*RISK,max(0,S["balance"]*MAXR-used))
 if risk<=0:S["entries_blocked"]+=1;return
 qty=risk/(px*SL);S["positions"][s]={"symbol":s,"side":side,"entry":px,"qty":qty,"risk":risk,"opened":time.time(),"score":round(sc,1)};last[s]=time.time();ev({"t":time.time(),"event":"ENTRY","symbol":s,"side":side,"price":px,"score":round(sc,1),"risk":round(risk,2)})
def closep(s,why,px):
 p=S["positions"].pop(s,None)
 if not p:return
 raw=(px-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1);fee=(p["entry"]+px)*p["qty"]*.00055;pnl=raw-fee;S["balance"]+=pnl;S["pnl"]+=pnl;S["fees"]+=fee;S["trades"]+=1
 if pnl>=0:S["wins"]+=1
 else:S["losses"]+=1
 ev({"t":time.time(),"event":why,"symbol":s,"side":p["side"],"pnl":round(pnl,2)})
async def trade(x):
 s=x.get("s");px=float(x.get("p") or 0);v=float(x.get("v") or 0)
 if not s or not px or not v:return
 T[s].append((time.time(),px*v*(1 if x.get("S")=="Buy" else -1),px));S["last_market"]=time.time();z=score(s)
 if z:
  sc,m=z;D[s]={"score":sc,**m}
  if sc>=TH:S["long_setups"]+=1;openp(s,"LONG",px,sc)
  elif sc<=-TH:S["short_setups"]+=1;openp(s,"SHORT",px,sc)
 p=S["positions"].get(s)
 if p:
  ret=(px-p["entry"])*(1 if p["side"]=="LONG" else -1)/p["entry"]
  if ret>=TP:closep(s,"TP",px)
  elif ret<=-SL:closep(s,"SL",px)
  elif time.time()-p["opened"]>=TSTOP:closep(s,"TIME",px)
def book_update(d):
 s=d.get("topic","").split(".")[-1];x=d.get("data",{});b=BOOK[s]
 if x.get("type")=="snapshot":b["b"]={str(a[0]):float(a[1]) for a in x.get("b",[])};b["a"]={str(a[0]):float(a[1]) for a in x.get("a",[])}
 else:
  for a in x.get("b",[]):b["b"].pop(str(a[0]),None) if float(a[1])==0 else b["b"].__setitem__(str(a[0]),float(a[1]))
  for a in x.get("a",[]):b["a"].pop(str(a[0]),None) if float(a[1])==0 else b["a"].__setitem__(str(a[0]),float(a[1]))
 bb=sorted(((float(k),v) for k,v in b["b"].items()),reverse=True)[:20];aa=sorted(((float(k),v) for k,v in b["a"].items()))[:20]
 if bb and aa:
  bv=sum(v for _,v in bb);av=sum(v for _,v in aa);b["imb"]=clip((bv-av)/(bv+av+1e-9));b["mid"]=(bb[0][0]+aa[0][0])/2
async def loop():
 while 1:
  try:
   async with httpx.AsyncClient(timeout=15) as c:d=await c.get("https://api.bybit.com/v5/market/tickers?category=linear");j=d.json()
   rows=sorted([(float(x.get("turnover24h") or 0),x["symbol"]) for x in j.get("result",{}).get("list",[]) if x["symbol"].endswith("USDT")],reverse=True);S["symbols"]=[x[1] for x in rows[:TOP]]
   async with websockets.connect("wss://stream.bybit.com/v5/public/linear",ping_interval=20,ping_timeout=10,max_size=2**22) as ws:
    S["ws"]="CONNECTED";sy=S["symbols"][:MON]
    for i in range(0,len(sy),10):
     batch=sy[i:i+10];await ws.send(json.dumps({"op":"subscribe","args":[f"publicTrade.{x}" for x in batch]+[f"orderbook.50.{x}" for x in batch]}));await asyncio.sleep(.12)
    async for raw in ws:
     d=json.loads(raw);t=d.get("topic","")
     if t.startswith("publicTrade."):
      for x in d.get("data",[]):await trade(x)
     elif t.startswith("orderbook.50."):book_update(d)
  except Exception as e:S["ws"]="DISCONNECTED";S["last_error"]=repr(e);await asyncio.sleep(3)
@app.get("/api/status")
def status():
 eq=S["balance"]
 for s,p in S["positions"].items():
  px=BOOK[s]["mid"] or (T[s][-1][2] if T[s] else p["entry"]);eq+=(px-p["entry"])*p["qty"]*(1 if p["side"]=="LONG" else -1)
 top=sorted(D.items(),key=lambda x:abs(x[1]["score"]),reverse=True)[:20]
 return JSONResponse({"balance":round(S["balance"],2),"equity":round(eq,2),"pnl":round(S["pnl"],2),"fees":round(S["fees"],2),"ws":S["ws"],"position_count":len(S["positions"]),"max_positions":MAXP,"monitor":MON,"calculations":sum(len(v) for v in T.values()),"long_setups":S["long_setups"],"short_setups":S["short_setups"],"entries_blocked":S["entries_blocked"],"trades":S["trades"],"wins":S["wins"],"losses":S["losses"],"last_market":S["last_market"],"last_error":S["last_error"],"diagnostics":[{"symbol":k,**v} for k,v in top],"events":list(S["events"])[:35]})
@app.get("/",response_class=HTMLResponse)
def home():
 return """<meta name=viewport content="width=device-width,initial-scale=1"><style>body{font:14px system-ui;background:#111;color:#eee;margin:14px}.box{border:1px solid #333;border-radius:12px;padding:12px;margin:8px 0}table{width:100%;font-size:11px}td,th{padding:5px;text-align:right}td:first-child,th:first-child{text-align:left}pre{font-size:11px;white-space:pre-wrap}</style><h2>⚡ Bybit Micro Scalper v8 — PAPER</h2><div id=a class=box>Loading...</div><div class=box><b>Top setups</b><div style=overflow:auto><table id=t></table></div></div><div class=box><b>Events</b><pre id=e>Loading...</pre></div><script>
async function g(){try{let r=await fetch("/api/status?"+Date.now());if(!r.ok)throw Error("HTTP "+r.status);let x=await r.json();a.innerHTML="Balance $"+x.balance+" | Equity $"+x.equity+" | PnL $"+x.pnl+"<br>WS "+x.ws+" | Positions "+x.position_count+"/"+x.max_positions+" | Monitor "+x.monitor+"<br>Calc "+x.calculations+" | LONG "+x.long_setups+" | SHORT "+x.short_setups+" | Blocked "+x.entries_blocked+"<br>Trades "+x.trades+" W/L "+x.wins+"/"+x.losses+" | Fees $"+x.fees+"<br>Last "+(x.last_market?new Date(x.last_market*1000).toLocaleTimeString():"-")+" | Error "+(x.last_error||"-");t.innerHTML="<tr><th>Symbol</th><th>Score</th><th>CVD</th><th>Book</th><th>M5</th><th>Imp</th></tr>"+x.diagnostics.map(z=>"<tr><td>"+z.symbol+"</td><td>"+z.score.toFixed(1)+"</td><td>"+z.cvd.toFixed(2)+"</td><td>"+z.book.toFixed(2)+"</td><td>"+z.m5.toFixed(2)+"</td><td>"+z.impulse.toFixed(2)+"</td></tr>").join("");e.textContent=x.events.map(z=>new Date(z.t*1000).toLocaleTimeString()+" "+z.event+" "+z.symbol+" "+z.side+" score="+(z.score||"")+" pnl="+(z.pnl??"")).join("\\n")}catch(err){a.textContent="Dashboard error: "+err.message}}g();setInterval(g,1000)</script>"""
@app.on_event("startup")
async def start():asyncio.create_task(loop())
if __name__=="__main__":uvicorn.run(app,host="0.0.0.0",port=PORT)
