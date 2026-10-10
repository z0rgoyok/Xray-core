import concurrent.futures,http.client,json,os,resource,socket,struct,subprocess,tempfile,threading,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
BINARY=Path(os.environ['WEX_QUOTA_BINARY']).resolve()
class UnixHTTP(http.client.HTTPConnection):
 def __init__(self,path):super().__init__('local',timeout=15);self.path=path
 def connect(self):self.sock=socket.socket(socket.AF_UNIX);self.sock.settimeout(self.timeout);self.sock.connect(self.path)
def freeport():
 s=socket.socket();s.bind(('127.0.0.1',0));p=s.getsockname()[1];s.close();return p
class Targets:
 def __init__(self):
  self.stop=threading.Event();self.sockets=[];self.received={};self.lock=threading.Lock();self.stall_started=threading.Event()
  self.echo=self.tcp('echo');self.sink=self.tcp('sink');self.flood=self.tcp('flood');self.udp=self.udpserver()
 def tcp(self,mode):
  s=socket.socket();s.bind(('127.0.0.1',0));s.listen();s.settimeout(.1);self.sockets.append(s);p=s.getsockname()[1];self.received[p]=0
  def work(c):
   try:
    if mode=='flood':
     c.recv(1);self.stall_started.set()
     for _ in range(2048):c.sendall(b'x'*65536)
    else:
     while data:=c.recv(65536):
      with self.lock:self.received[p]+=len(data)
      if mode=='echo':c.sendall(data)
   except OSError:pass
   finally:c.close()
  def run():
   while not self.stop.is_set():
    try:c,_=s.accept()
    except socket.timeout:continue
    except OSError:return
    self.sockets.append(c);threading.Thread(target=work,args=(c,),daemon=True).start()
  threading.Thread(target=run,daemon=True).start();return p
 def udpserver(self):
  s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(('127.0.0.1',0));s.settimeout(.1);self.sockets.append(s);p=s.getsockname()[1];self.received[p]=0
  def run():
   while not self.stop.is_set():
    try:data,a=s.recvfrom(65536)
    except socket.timeout:continue
    except OSError:return
    with self.lock:self.received[p]+=len(data)
    s.sendto(data,a)
  threading.Thread(target=run,daemon=True).start();return p
 def close(self):
  self.stop.set()
  for s in self.sockets:
   try:s.close()
   except OSError:pass

def exact(s,n):
 out=b''
 while len(out)<n:
  part=s.recv(n-len(out))
  if not part:raise OSError('closed')
  out+=part
 return out

def stamp(t):return time.strftime('%Y-%m-%dT%H:%M:%S',time.gmtime(t))+f'.{int((t%1)*1000000):06d}Z'

def run():
 targets=Targets();process=None;connections=[];results={}
 with tempfile.TemporaryDirectory(prefix='wex-quota-fixture-') as td:
  td=Path(td);ledger_id=subprocess.check_output([str(BINARY),'wexquota-init',str(td)],text=True).strip();raw,xhttp=freeport(),freeport();uids=[uuid.uuid4() for _ in range(3)];emails=['wex-paid:a','wex-paid:b','pilot'];sock=td/'control.sock'
  clients=[{'id':str(u),'email':e} for u,e in zip(uids,emails)]
  settings={'decryption':'none','clients':clients}
  cfg={'log':{'loglevel':'none'},'api':{'tag':'api','listen':'unix://'+str(td/'handler.sock'),'services':['HandlerService']},'inbounds':[{'tag':'raw','listen':'127.0.0.1','port':raw,'protocol':'vless','settings':settings},{'tag':'xh','listen':'127.0.0.1','port':xhttp,'protocol':'vless','settings':settings,'streamSettings':{'network':'xhttp','xhttpSettings':{'path':'/fixture','mode':'packet-up','scMaxEachPostBytes':65536}}}],'outbounds':[{'protocol':'freedom','tag':'direct'}]}
  revision=0;grant=None
  def rpc(method,path,body=None):
   h=UnixHTTP(str(sock));h.request(method,path,json.dumps(body) if body is not None else None,{'Content-Type':'application/json'});r=h.getresponse();data=r.read();h.close();assert r.status==200,(r.status,data);return json.loads(data)
  def status():return rpc('GET','/v1/state')
  def start():
   nonlocal process
   process=subprocess.Popen([str(BINARY),'run','-config','stdin:'],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,env={**os.environ,'WEX_QUOTA_DIR':str(td)})
   process.stdin.write(json.dumps(cfg).encode());process.stdin.close()
   for _ in range(200):
    try:status();return
    except (OSError,AssertionError):time.sleep(.02)
   raise RuntimeError('candidate start failed')
  def apply(limit=1_000_000_000,email='wex-paid:a',ends=None,empty=False,period=None,minseq=0):
   nonlocal revision,grant
   revision+=1;now=time.time();grant={'email':email,'packageId':'happ-fixture','periodId':period or f'period-{revision}','startsAt':stamp(now-60),'endsAt':stamp(ends or now+3600),'limitBytes':limit}
   request={'revision':revision,'expectedLedgerId':ledger_id,'minSequence':minseq,'grants':[] if empty else [grant],'floors':[]};return rpc('PUT','/v1/grants',request)
  def connection(uid,port,payload=b'',command=1):
   s=socket.create_connection(('127.0.0.1',raw),3);s.settimeout(5);connections.append(s);s.sendall(b'\0'+uid.bytes+b'\0'+bytes([command])+(b'' if command==3 else port.to_bytes(2,'big')+b'\x01\x7f\0\0\1')+payload);return s
  def echo(uid=0,data=b'hi'):
   s=connection(uids[uid],targets.echo,data);assert exact(s,2)==b'\0\0';assert exact(s,len(data))==data;s.close()
  def xudp(uid,globalid,payload,nested=False):
   s=connection(uids[uid],0,command=3)
   meta=b'\0\1\x01\x01\x02'+targets.udp.to_bytes(2,'big')+b'\x01\x7f\0\0\1'+globalid
   frame=len(meta).to_bytes(2,'big')+meta+len(payload).to_bytes(2,'big')+payload
   if nested:
    name=b'v1.mux.cool';meta=b'\0\1\x01\x01\x01\0\0\x02'+bytes([len(name)])+name
    frame=len(meta).to_bytes(2,'big')+meta+len(frame).to_bytes(2,'big')+frame
   s.sendall(frame);return s
  try:
   start();assert not status()['healthy'];apply();assert status()['healthy']
   assert os.stat(td/'handler.sock').st_mode&0o777==0o600
   h=subprocess.run([str(BINARY),'api','inboundusercount','--server=unix://'+str(td/'handler.sock'),'-tag=raw'],capture_output=True,text=True,timeout=5);assert h.returncode==0,h.stderr
   results['private_handler_api']=True
   tiny=[]
   for _ in range(25):
    t=time.monotonic();echo(data=b'x');tiny.append((time.monotonic()-t)*1000)
   results['tiny_request_latency_ms']={'mean':round(sum(tiny)/len(tiny),2),'max':round(max(tiny),2)}
   benchmarks=[]
   for concurrency in (1,8,32):
    apply();t=time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:list(pool.map(lambda _:echo(data=b'b'*1048576),range(concurrency)))
    elapsed=time.monotonic()-t;used=next(x['usedBytes'] for x in status()['usage'] if x['periodId']==grant['periodId']);assert used==concurrency*2097152,(used,concurrency)
    benchmarks.append({'concurrency':concurrency,'MiB_up_down_each':1,'seconds':round(elapsed,3),'total_MiB_per_second':round(concurrency*2/elapsed,2)})
   results['benchmark']=benchmarks
   results['wal_bytes_after_82MiB_payload']=os.stat(td/'ledger.jsonl').st_size
   apply(limit=4096);before=targets.received[targets.sink];s=connection(uids[0],targets.sink,b'u'*32768);time.sleep(.3);after=targets.received[targets.sink]-before;used=status()['usage'][-1]['usedBytes'] if False else next(x['usedBytes'] for x in status()['usage'] if x['periodId']==grant['periodId']);assert after<=4096 and used==4096,(after,used);results['quota_overshoot_payload_bytes']=max(0,after-4096);s.close()
   apply(limit=10000);old=connection(uids[0],targets.echo,b'hi');exact(old,4);pilot=connection(uids[2],targets.echo,b'hi');exact(pilot,4);oldperiod=grant['periodId'];t=time.monotonic();apply(limit=10000,email='wex-paid:b',period=oldperiod);results['transfer_ack_seconds']=round(time.monotonic()-t,3)
   old.settimeout(.5)
   try:old.sendall(b'forbidden');d=old.recv(20);assert not d
   except OSError:pass
   pilot.sendall(b'alive');assert exact(pilot,5)==b'alive';echo(uid=1);old.close();pilot.close();results['pilot_preserved']=True
   apply(limit=10000,ends=time.time()+.3);e=connection(uids[0],targets.echo,b'hi');exact(e,4);time.sleep(.4)
   try:e.sendall(b'expired');assert not e.recv(20)
   except OSError:pass
   results['expiry_closed_active_stream']=True;e.close()
   # Process-global UDP collision with a pilot route must not bypass the paid quota.
   gid=b'quota-ID';p=xudp(2,gid,b'pilot');time.sleep(.1);before=targets.received[targets.udp];apply(limit=100);payed=xudp(0,gid,b'u'*200);time.sleep(.2);assert targets.received[targets.udp]==before;results['xudp_pilot_collision_blocked']=True;payed.close();p.close()
   apply(limit=10000);before=targets.received[targets.udp];valid=xudp(0,b'valid-ID',b'valid');time.sleep(.15);assert targets.received[targets.udp]==before+5;valid.close();results['xudp_paid_usable']=True
   apply(limit=100);before=targets.received[targets.udp];nested=xudp(0,b'nestedID',b'u'*200,True);time.sleep(.2);assert targets.received[targets.udp]==before;results['nested_mux_quota_blocked']=True;nested.close()
   # Stalled XHTTP consumer: a concurrent policy ACK must remain bounded.
   apply();session=str(uuid.uuid4());down=socket.create_connection(('127.0.0.1',xhttp),3);connections.append(down);down.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,4096);down.sendall(f'GET /fixture/{session}?x_padding=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX HTTP/1.1\r\nHost: localhost\r\nX-Padding: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n\r\n'.encode())
   payload=b'\0'+uids[0].bytes+b'\0\x01'+targets.flood.to_bytes(2,'big')+b'\x01\x7f\0\0\1'+b'D';h=http.client.HTTPConnection('127.0.0.1',xhttp,timeout=3);h.request('POST',f'/fixture/{session}/0?x_padding='+('X'*120),payload,{'X-Padding':'x'*120});resp=h.getresponse();assert resp.status==200,resp.status;resp.read();h.close();assert targets.stall_started.wait(3);time.sleep(.2);assert status()['activeFlows']>0;t=time.monotonic();apply(empty=True);dt=time.monotonic()-t;assert dt<1,dt;results['stalled_xhttp_revoke_ack_seconds']=round(dt,3);down.close()
   apply();echo(data=b'restart');high=status();saved={'revision':revision,'expectedLedgerId':ledger_id,'minSequence':high['sequence'],'grants':[grant],'floors':high['usage']};process.kill();process.wait(5);start();assert not status()['healthy'];rpc('PUT','/v1/grants',saved);assert status()['usage']==high['usage'];echo();results['crash_restart_retained_usage']=True
   results['candidate_child_peak_rss_bytes']=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
   results['production_touched']=False;print(json.dumps(results,indent=2))
  finally:
   for c in connections:
    try:c.close()
    except OSError:pass
   if process:process.terminate();process.wait(5)
   targets.close()
if __name__=='__main__':run()
