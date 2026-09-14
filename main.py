import asyncio,json,time,os
from collections import defaultdict,deque
from contextlib import asynccontextmanager
import aiohttp,websockets
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,JSONResponse

BASE='https://api.bybit.com'
WS='wss://stream.bybit.com/v5/public/linear'
BAL=float(os.getenv('START_BALANCE','10000'))
RISK=min(float(os.getenv('RISK_PCT','0.02')),0.02)
MAXP=int(os.getenv('MAX_POSITIONS','30'))
TOP=min(int(os.getenv('TOP_N','300')),300)
DEEP=min(int(os.getenv('DEEP_N','50')),50)
state={'balance':BAL,'equity':BAL,'peak':BAL,'positions':{},'signals':[],'trades':[],'symbols':[],'ws':False,'last_error':'','max_positions':MAXP}
bars=defaultdict(lambda:{'bucket':None,'cvd':0.0,'gross':0.0,'trades':0})
hist=defaultdict(lambda:deque(maxlen=100))
last_signal={}; last_entry={}; tasks=[]

async def get(s,path,params):
    async with s.get(BASE+path,params=params,timeout=15) as r:
        x=await r.json()
        if x.get('retCode')!=0: raise RuntimeError(x.get('retMsg','Bybit error'))
        return x['result']

async def universe(s):
    x=await get(s,'/v5/market/tickers',{'category':'linear'})
    a=[]
    for r in x.get('list',[]):
        if r.get('symbol','').endswith('USDT'):
            try:a.append((r['symbol'],float(r.get('turnover24h',0))))
            except:pass
    return [x[0] for x in sorted(a,key=lambda z:z[1],reverse=True)[:TOP]]

def add(symbol,ts,signed):
    bucket=(ts//900000)*900000; b=bars[symbol]; closed=None
    if b['bucket'] is None:b['bucket']=bucket
    elif bucket!=b['bucket']:
        closed=dict(b); b.update({'bucket':bucket,'cvd':0.0,'gross':0.0,'trades':0})
    b['cvd']+=signed; b['gross']+=abs(signed); b['trades']+=1
    return closed

def score(symbol):
    h=list(hist[symbol])
    if len(h)<20:return None
    z=sum(x['cvd'] for x in h[-20:])/(sum(x['gross'] for x in h[-20:])+1e-12)
    return round(50+35*max(-1,min(1,z)),2),z

def emit(symbol,side,sc,p):
    now=time.time()
    if now-last_signal.get(symbol,0)<60:return False
    last_signal[symbol]=now
    state['signals'].append({'ts':now,'symbol':symbol,'side':side,'score':sc,'price':p})
    state['signals']=state['signals'][-100:]
    return True

def enter(symbol,side,p,sc):
    if symbol in state['positions'] or len(state['positions'])>=MAXP:return
    now=time.time()
    if now-last_entry.get(symbol,0)<900:return
    stop=.006; qty=state['equity']*RISK/(p*stop)
    state['positions'][symbol]={'symbol':symbol,'side':side,'entry':p,'qty':qty,'stop':p*(1-stop if side=='LONG' else 1+stop),'target':p*(1+stop*1.5 if side=='LONG' else 1-stop*1.5),'score':sc,'opened':now}
    last_entry[symbol]=now

def close(symbol,p,reason):
    x=state['positions'].pop(symbol,None)
    if not x:return
    d=1 if x['side']=='LONG' else -1
    pnl=(p-x['entry'])*x['qty']*d-(p*x['qty']+x['entry']*x['qty'])*.00055
    state['balance']+=pnl; state['equity']=state['balance']; state['peak']=max(state['peak'],state['equity'])
    state['trades'].append({'ts':time.time(),'symbol':symbol,'side':x['side'],'entry':x['entry'],'exit':p,'pnl':pnl,'reason':reason})

async def worker():
    async with aiohttp.ClientSession() as s:
        try:state['symbols']=await universe(s)
        except Exception as e:state['last_error']=str(e);return
        topics=[f'publicTrade.{x}' for x in state['symbols'][:DEEP]]
        while True:
            try:
                async with websockets.connect(WS,ping_interval=20,ping_timeout=20,max_size=None) as ws:
                    await ws.send(json.dumps({'op':'subscribe','args':topics})); state['ws']=True
                    async for raw in ws:
                        m=json.loads(raw)
                        if not m.get('topic','').startswith('publicTrade.'):continue
                        for t in m.get('data',[]):
                            try:sym=t['s'];p=float(t['p']);v=float(t['v']);ts=int(t['T'])
                            except:continue
                            signed=p*v if t.get('S')=='Buy' else -p*v
                            closed=add(sym,ts,signed)
                            if closed is None:continue
                            hist[sym].append(closed); z=score(sym)
                            if not z:continue
                            sc,imb=z
                            if abs(imb)>=.55:
                                side='LONG' if imb>0 else 'SHORT'
                                if emit(sym,side,sc,p) and (sc>=69 or sc<=31):enter(sym,side,p,sc)
                            pos=state['positions'].get(sym)
                            if pos and ((pos['side']=='LONG' and p<=pos['stop']) or (pos['side']=='SHORT' and p>=pos['stop'])):close(sym,p,'SL')
                            elif pos and ((pos['side']=='LONG' and p>=pos['target']) or (pos['side']=='SHORT' and p<=pos['target'])):close(sym,p,'TP')
            except Exception as e:state['ws']=False;state['last_error']=str(e);await asyncio.sleep(3)

@asynccontextmanager
async def life(app):
    tasks.append(asyncio.create_task(worker()));yield
    [x.cancel() for x in tasks]

app=FastAPI(lifespan=life)
@app.get('/',response_class=HTMLResponse)
async def home():
    return '<meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:Arial;background:#0b0d10;color:#eee;padding:16px}.c{background:#151920;padding:16px;border-radius:14px;margin:12px 0}.g{display:grid;grid-template-columns:1fr 1fr;gap:12px}.n{font-size:25px;font-weight:bold}.ok{color:#55e6a0}table{width:100%;font-size:12px}td{padding:6px;border-bottom:1px solid #292e36}</style><h2>BYBIT PAPER BOT</h2><div class="c"><div class="g"><div>Balance<div id="b" class="n">-</div></div><div>Drawdown<div id="d" class="n">-</div></div><div>WebSocket<div id="w" class="n">-</div></div><div>Positions<div id="p" class="n">-</div></div></div></div><div class="c"><b>15m CVD signal engine · 30-position stress test</b></div><div class="c"><b>Open positions</b><div id="o">-</div></div><div class="c"><b>Recent signals</b><table><tbody id="s"></tbody></table></div><script>async function q(){let x=await(await fetch("/api/status")).json();b.textContent="$"+x.equity.toFixed(2);d.textContent=(x.drawdown*100).toFixed(2)+"%";w.innerHTML=x.ws?"<span class=ok>CONNECTED</span>":"OFFLINE";p.textContent=Object.keys(x.positions).length;o.innerHTML=Object.values(x.positions).map(a=>a.symbol+" "+a.side+" @ "+a.entry+" → SL "+a.stop.toFixed(6)+" / TP "+a.target.toFixed(6)).join("<br>")||"none";s.innerHTML=x.signals.slice(-20).reverse().map(a=>"<tr><td>"+a.symbol+"</td><td>"+a.side+"</td><td>"+a.score+"</td><td>"+a.price+"</td></tr>").join("")}setInterval(q,2000);q()</script>'
@app.get('/api/status')
async def status():
    dd=(state['peak']-state['equity'])/state['peak'] if state['peak'] else 0
    return JSONResponse({**state,'drawdown':dd,'uptime':time.time()})