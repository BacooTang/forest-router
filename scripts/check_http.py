#!/usr/bin/env python3
"""Isolated HTTP acceptance. No production keys, ports or daemons."""
import json,os,pathlib,socket,subprocess,tempfile,threading,time,urllib.request,urllib.error,http.server,http.cookiejar
ROOT=pathlib.Path(__file__).resolve().parents[1]
seen=[]
hooks=[]
candy_answer="21"
monitor_delay=0
transient_fail=False
probe_throttled=False
monitor_started=threading.Event()
class Upstream(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  b=json.dumps({'mode':'unrestricted','remaining':100,'unit':'USD'}).encode();self.send_response(200);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def do_POST(self):
  b=self.rfile.read(int(self.headers.get('Content-Length','0')));d=json.loads(b);seen.append((self.path,dict(self.headers),d))
  if self.path=='/hook':
   hooks.append(d);out=b'{"code":0}';self.send_response(200);self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out);return
  if self.path.endswith('/chat/completions'):
   monitor_started.set();time.sleep(monitor_delay)
   out=json.dumps({'choices':[{'message':{'content':candy_answer},'finish_reason':'stop'}]}).encode();self.send_response(200);self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out);return
  if self.path.startswith('/transient/') and transient_fail and d.get('input')!='Reply OK.':
   self.send_response(503);self.send_header('Content-Length','0');self.end_headers();return
  if self.path.startswith('/early/'):
   out=b'event: response.failed\ndata: {"type":"response.failed","response":{"error":{"code":"server_error"}}}\n\n';self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out);return
  if self.path.startswith('/usage/'):
   usage={'input_tokens':20,'output_tokens':3,'input_tokens_details':{'cached_tokens':10}}
   value={'status':'completed','output':[],'pad':'x'*70000,'usage':usage}
   out=(('data: '+json.dumps({'type':'response.completed','response':value})+'\n\n')*2).encode() if d.get('stream') else json.dumps(value).encode()
   self.send_response(200);self.send_header('Content-Type','text/event-stream' if d.get('stream') else 'application/json');self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out);return
  if self.path.startswith('/review/'):
   name=self.path.split('/')[2];status=200;ct='text/event-stream'
   delta=b'data: {"type":"response.output_text.delta","delta":"hello"}\n\n'
   failed=b'data: {"type":"response.failed","response":{"error":{"code":"server_error"}}}\n\n'
   if name=='large':out=b'data: '+json.dumps({'response':{'status':'completed','output':[],'pad':'P'*70000},'type':'response.completed'}).encode()+b'\n\n'
   elif name=='alias':out=b'data: {"type":"response.done"}\n\n'
   elif name=='unknownterminal':out=delta
   elif name in ('together','split'):out=delta+failed
   elif name=='silent':out=b'data: {"type":"response.completed","response":{"status":"completed","output":[]}}\n\n'
   elif name=='huge':ct='application/json';out=json.dumps({'status':'completed','output':[],'pad':'x'*(9*1024*1024)}).encode()
   elif name=='input':status=400;ct='application/json';out=b'{"error":{"message":"Unsupported parameter: foo"}}'
   elif name=='validation':status=422;ct='application/json';out=b'{"detail":[{"msg":"field required"}]}'
   elif name=='okextra':ct='application/json';out=b'{"status":"completed","output":[],"code":"ok","error":{}}'
   else:raise AssertionError(name)
   self.send_response(status);self.send_header('Content-Type',ct);self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.flush()
   try:
    if name=='silent':time.sleep(11)
    if name=='split':self.wfile.write(delta);self.wfile.flush();time.sleep(.15);self.wfile.write(failed)
    else:self.wfile.write(out)
   except (BrokenPipeError,ConnectionResetError):pass
   return
  if probe_throttled and self.path=='/ok/v1/responses' and d.get('input')=='Reply OK.':
   self.send_response(429);self.send_header('Retry-After','120');self.send_header('Content-Length','0');self.end_headers();return
  variants={'/jsonauth/':(200,{'code':401,'msg':'令牌已过期或验证不正确','success':False}),'/unknown/':(200,{'success':False,'code':937,'msg':'unknown vendor error'}),'/input/':(400,{'error':{'code':'context_length_exceeded','message':'too long'}})}
  for prefix,(status,value) in variants.items():
   if self.path.startswith(prefix):
    out=json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out);return
  if self.path.startswith('/bad/'):
   self.send_response(503);self.send_header('Content-Length','0');self.end_headers();return
  if self.path.startswith('/empty/'):
   b=b'{"error":{"code":"insufficient_quota"}}';self.send_response(429);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b);return
  if d.get('stream'):
   b=b'event: response.created\ndata: {"type":"response.created"}\n\nevent: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","output":[]}}\n\n';ct='text/event-stream'
  else:b=b'{"id":"test","status":"completed","output":[]}';ct='application/json'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
def freeport():
 s=socket.socket();s.bind(('127.0.0.1',0));p=s.getsockname()[1];s.close();return p
def main():
 global candy_answer,monitor_delay,probe_throttled,transient_fail
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Upstream);threading.Thread(target=server.serve_forever,daemon=True).start();up=server.server_port
 with tempfile.TemporaryDirectory(prefix='forest-check-') as temp:
  port=freeport();base=f'http://127.0.0.1:{port}';env=dict(os.environ,FOREST_ROUTER_HOME=temp,FOREST_LISTEN=f'127.0.0.1:{port}',FOREST_ADMIN_PASSWORD='test-password',FOREST_API_KEY='company-secret')
  proc=subprocess.Popen([str(ROOT/'target/debug/forest-router')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
  def request(path,data=None,auth=False):
   h={'content-type':'application/json','x-forest-admin':'1'}
   if auth:h['Authorization']='Bearer '+(auth if isinstance(auth,str) else 'company-secret')
   req=urllib.request.Request(base+path,data=None if data is None else json.dumps(data).encode(),headers=h)
   try:
    with opener.open(req,timeout=20) as r:
     raw=r.read()
     if path=='/v1/responses':
      assert r.headers.get('X-Request-ID','').startswith('fr_')
      request.last_id=r.headers['X-Request-ID']
     if path=='/admin/api/save' and r.status==200:data['_revision']=json.loads(raw)['revision']
     return r.status,raw
   except urllib.error.HTTPError as e:
    if path=='/v1/responses':
     assert e.headers.get('X-Request-ID','').startswith('fr_')
     request.last_id=e.headers['X-Request-ID']
    return e.code,e.read()
  try:
   for _ in range(100):
    try:request('/');break
    except OSError:time.sleep(.05)
   assert request('/admin/api/state')[0]==401
   assert request('/v1/models')[0]==401
   assert json.loads(request('/v1/models',auth=True)[1])=={'object':'list','data':[]}
   assert request('/admin/api/login',{'password':'test-password'})[0]==200
   cfg=json.loads(request('/admin/api/state')[1])['config'];assert 'admin_password_hash' not in cfg
   assert len(cfg['monitor_schedule'])==4
   bad=json.loads(json.dumps(cfg));bad['monitor_schedule']=[{'start':0,'end':0,'interval_minutes':5},{'start':60,'end':120,'interval_minutes':10}]
   assert request('/admin/api/save',bad)[0]==400
   cfg['monitor_schedule'][0]['interval_minutes']=10
   assert request('/admin/api/save',cfg)[0]==200
   assert json.loads(request('/admin/api/state')[1])['config']['monitor_schedule'][0]['interval_minutes']==10
   cfg['monitor_schedule'][0]['interval_minutes']=5
   assert request('/admin/api/save',cfg)[0]==200
   def channel(i,path):return {'id':i,'name':i,'base_url':f'http://127.0.0.1:{up}/{path}/v1','upstream_model':'upstream-model','adapter':'sub2_api','enabled':True,'keys':[{'id':i+'-key','label':i,'secret':'upstream-secret'}]}
   employee='fr_'+'a'*64
   cfg['employee_keys']=[{'id':'employee-a','name':'测试员工','secret':employee,'enabled':True}]
   cfg['models']=[{'id':'test-model','channels':[channel('usage','usage')]}]
   assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/models',auth=employee)[0]==200
   for stream in (True,False):
    assert request('/v1/responses',{'model':'test-model','input':'hello','stream':stream},employee)[0]==200
   ledger=json.loads(request('/admin/api/state')[1])['client_usage']['clients']['employee-a']
   day=str(int((time.time()+28800)//86400));u=ledger['days'][day]
   assert (u['requests'],u['input'],u['output'],u['cached'],u['missing'])==(2,40,6,20,0),u
   cfg['employee_keys'][0]['enabled']=False;assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/models',auth=employee)[0]==401
   assert request('/v1/responses',{'model':'test-model','input':'hello'},employee)[0]==401
   cfg['employee_keys']=[];assert request('/admin/api/save',cfg)[0]==200
   assert 'employee-a' in json.loads(request('/admin/api/state')[1])['client_usage']['clients']
   cfg['models']=[{'id':'test-model','channels':[channel('broken','bad'),channel('good','ok')]}]
   assert request('/admin/api/save',cfg)[0]==200
   status,catalog=request('/v1/models',auth=True);assert status==200
   assert json.loads(catalog)=={'object':'list','data':[{'id':'test-model','object':'model','created':0,'owned_by':'forest-router'}]}
   assert b'upstream-secret' not in catalog and b'upstream-model' not in catalog
   payload={'model':'test-model','input':'hello','stream':True,'reasoning':{'effort':'high'},'unknown':{'nested':[1,True,'untouched']}}
   assert request('/v1/responses',payload)[0]==401
   status,body=request('/v1/responses',payload,True);assert status==200 and b'response.completed' in body
   assert seen[-2][0]=='/bad/v1/responses' and seen[-1][0]=='/ok/v1/responses'
   assert seen[-1][2]==dict(payload,model='upstream-model')
   assert seen[-1][1].get('authorization')=='Bearer upstream-secret'
   state=json.loads(request('/admin/api/state')[1])['state'];assert state['keys']['broken-key']['suspect'];assert state['last_used']['test-model']=='good';assert state['keys']['good-key']['checked']
   before=len(seen);assert request('/v1/responses',payload,True)[0]==200;assert len(seen)==before+1
   assert request('/v1/responses',dict(payload,model='missing'),True)[0]==404
   assert request('/v1/responses',dict(payload,previous_response_id='private'),True)[0]==400
   assert request('/admin/api/verify',{'channel_id':'good'})[0]==200
   # Disabled providers/keys never enter request candidate rotation.
   first=channel('switches','ok/switches');first['keys']=[{'id':'off-key','label':'off','secret':'disabled-secret','enabled':False},{'id':'on-key','label':'on','secret':'enabled-secret','enabled':True}]
   cfg['models'][0]['channels']=[first,channel('fallback','ok/fallback')];assert request('/admin/api/save',cfg)[0]==200
   before=len(seen);assert request('/v1/responses',payload,True)[0]==200;assert len(seen)==before+1 and seen[-1][1].get('authorization')=='Bearer enabled-secret'
   first['keys'][1]['enabled']=False;assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/ok/fallback/v1/responses'
   first['keys'][1]['enabled']=True;first['enabled']=False;assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/ok/fallback/v1/responses'
   first['enabled']=True;assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/ok/switches/v1/responses'
   # Restored priority providers immediately take precedence.
   cfg['models'][0]['channels'][1]['enabled']=False;assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/ok/switches/v1/responses'
   cfg['models'][0]['channels']=[channel('empty','empty'),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200
   state=json.loads(request('/admin/api/state')[1])['state'];assert state['keys']['empty-key']['allowance']['exhausted'];assert 'broken-key' not in state['keys']
   cfg['models'][0]['channels']=[channel('early','early'),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
   status,body=request('/v1/responses',payload,True);assert status==200 and b'response.failed' not in body
   cfg['webhook']=f'http://127.0.0.1:{up}/hook';cfg['monitors']=[{'id':'quality','name':'quality','base_url':f'http://127.0.0.1:{up}/v1','key':'monitor-key','model':'test'}]
   cfg['models'][0]['channels']=[channel('watched','ok')];cfg['models'][0]['channels'][0]['monitor_id']='quality';assert request('/admin/api/save',cfg)[0]==200
   assert request('/admin/api/verify',{'monitor_id':'quality'})[0]==200
   assert request('/v1/responses',payload,True)[0]==200
   monitor_started.clear();monitor_delay=1
   checks=[];worker=threading.Thread(target=lambda:checks.append(request('/admin/api/verify',{'monitor_id':'quality'})[0]));worker.start()
   assert monitor_started.wait(5)
   assert request('/admin/api/verify',{'monitor_id':'quality'})[0]==409
   worker.join();assert checks==[200];monitor_delay=0
   candy_answer='20';assert request('/admin/api/verify',{'monitor_id':'quality'})[0]==200
   before=len(seen)
   assert request('/v1/responses',payload,True)[0]==503
   assert len(seen)==before
   fallback=channel('quality-fallback','ok/quality-fallback')
   cfg['models'][0]['channels'].append(fallback);assert request('/admin/api/save',cfg)[0]==200
   before=len(seen)
   assert request('/v1/responses',payload,True)[0]==200
   assert len(seen)==before+1 and seen[-1][0]=='/ok/quality-fallback/v1/responses'
   cfg['models'][0]['channels'].pop();assert request('/admin/api/save',cfg)[0]==200
   for _ in range(80):
    if hooks:break
    time.sleep(.1)
   assert len(hooks)==1, hooks
   assert request('/admin/api/verify',{'monitor_id':'quality'})[0]==200
   assert not json.loads(request('/admin/api/state')[1])['state']['notices']
   candy_answer='21';assert request('/admin/api/verify',{'monitor_id':'quality'})[0]==200
   assert request('/v1/responses',payload,True)[0]==200
   assert not cfg.get('notify_all_monitors',False)
   cfg['notify_all_monitors']=True;assert request('/admin/api/save',cfg)[0]==200
   for _round in range(2):
    before=len(hooks)
    assert request('/admin/api/verify',{'all_monitors':True})[0]==200
    for _ in range(80):
     if len(hooks)>before:break
     time.sleep(.1)
    assert len(hooks)==before+1,hooks
    card=hooks[-1];assert card['msg_type']=='interactive'
    assert card['card']['header']['template']=='green'
    assert 'monitor-key' not in json.dumps(card)
   cfg['notify_all_monitors']=False;assert request('/admin/api/save',cfg)[0]==200
   cfg['webhook']=''
   cfg['models'][0]['channels']=[channel('transient','transient'),channel('standby','ok/standby')];assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/transient/v1/responses'
   transient_fail=True
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/ok/standby/v1/responses'
   state=json.loads(request('/admin/api/state')[1])['state'];assert state['keys']['transient-key']['suspect'] and not state['keys']['transient-key']['service_failed']
   transient_fail=False
   assert request('/admin/api/verify',{'channel_id':'transient'})[0]==200
   state=json.loads(request('/admin/api/state')[1])['state'];assert not state['keys']['transient-key']['suspect']
   assert request('/v1/responses',payload,True)[0]==200 and seen[-1][0]=='/transient/v1/responses'
   transient_fail=True
   cfg['models'][0]['channels'][1]['enabled']=False;assert request('/admin/api/save',cfg)[0]==200
   # A second business failure after successful tiny probe must escalate.
   assert request('/v1/responses',payload,True)[0]==503
   assert json.loads(request('/admin/api/state')[1])['state']['keys']['transient-key']['service_failed']
   transient_fail=False

   for prefix,flag in [('jsonauth','credential_failed'),('unknown','suspect')]:
    cfg['models'][0]['channels']=[channel(prefix,prefix),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
    status,body=request('/v1/responses',payload,True);assert status==200 and b'response.completed' in body
    state=json.loads(request('/admin/api/state')[1])['state'];assert state['keys'][prefix+'-key'][flag]
    before=len(seen);assert request('/v1/responses',payload,True)[0]==200;assert len(seen)==before+1
   cfg['models'][0]['channels']=[channel('input','input'),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
   before=len(seen);assert request('/v1/responses',payload,True)[0]==400;assert len(seen)==before+1
   cfg['models'][0]['channels']=[channel('duplicate1','bad'),channel('duplicate2','bad'),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
   before=len(seen);assert request('/v1/responses',payload,True)[0]==200;assert len(seen)==before+2
   cfg['models'][0]['channels']=[channel('big-input','ok')];assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',dict(payload,input='x'*(9*1024*1024)),True)[0]==200
   cfg['models'][0]['channels']=[channel('cap'+str(i),'bad/'+str(i)) for i in range(10)];assert request('/admin/api/save',cfg)[0]==200
   before=len(seen);assert request('/v1/responses',payload,True)[0]==503;assert len(seen)==before+10
   for name,expected in [('large',200),('alias',200),('unknownterminal',200),('together',200),('split',200),('silent',200),('huge',200),('input',400),('validation',422),('okextra',200)]:
    cfg['models'][0]['channels']=[channel('review-'+name,'review/'+name),channel('good','ok')];assert request('/admin/api/save',cfg)[0]==200
    before=len(seen);status,body=request('/v1/responses',payload,True)
    assert status==expected,(name,status,body[:200]);assert len(seen)==before+1,(name,'request replayed')
    state=json.loads(request('/admin/api/state')[1])['state'];key=state['keys']['review-'+name+'-key']
    assert key['suspect']==(name in ('together','split')),(name,key)
    if name in ('large','alias','silent','okextra'):assert key['checked'],(name,'successful response not recorded')
    if name in ('unknownterminal','together','split','input','validation'):assert not key['checked'],(name,'failure recorded as success')
    if name=='large':assert b'P'*70000 in body
    if name=='huge':assert len(body)>8*1024*1024
    if name in ('together','split'):assert b'hello' in body and b'response.failed' in body
    traffic=json.loads(request('/admin/api/state')[1])['traffic']
    assert traffic['active']==0,traffic
    if name in ('together','split','unknownterminal','input','validation'):
     report=next(r for r in traffic['failures'] if r['id']==request.last_id)
     assert len(report['attempts'])==1
     assert report['outcome']==('unknown' if name=='unknownterminal' else 'failed')
     assert report['output_started']==(name in ('together','split','unknownterminal'))
    assert 'upstream-secret' not in json.dumps(traffic)

   # Last-write-wins is rejected; a stale editor cannot overwrite a newer save.
   stale=json.loads(json.dumps(cfg));assert request('/admin/api/save',cfg)[0]==200
   cfg['api_key']='company-secret-2';assert request('/admin/api/save',cfg)[0]==200
   assert request('/admin/api/save',stale)[0]==409
   cfg['api_key']='company-secret';assert request('/admin/api/save',cfg)[0]==200
   cfg['models'][0]['channels']=[channel('allbad','bad')];assert request('/admin/api/save',cfg)[0]==200
   assert request('/v1/responses',payload,True)[0]==503
   before=len(seen);cfg['models'][0]['id']='renamed-model';assert request('/admin/api/save',cfg)[0]==200
   assert len(seen)==before
   assert json.loads(request('/admin/api/state')[1])['state']['keys']['allbad-key']['suspect']
   cfg['models'][0]['id']='test-model';assert request('/admin/api/save',cfg)[0]==200
   assert len(json.loads(request('/admin/api/state')[1])['state']['events'])<=200
   # Persisted failures survive graceful restart; manual reset requires actual success.
   cfg['models'][0]['channels']=[channel('recover','ok')];assert request('/admin/api/save',cfg)[0]==200
   proc.terminate();proc.wait(timeout=10)
   statepath=pathlib.Path(temp,'state.json');saved=json.loads(statepath.read_text());now=int(time.time())
   saved['keys']['recover-key'].update(service_failed=True,cooldown_until=now+600,retry_at=now-1,probe_attempts=4,recovery_successes=0,reason='fixture outage')
   statepath.write_text(json.dumps(saved))
   probe_throttled=True
   configpath=pathlib.Path(temp,'config.json');disk=json.loads(configpath.read_text());disk['webhook']='http://127.0.0.1:1/unreachable';configpath.write_text(json.dumps(disk))
   proc=subprocess.Popen([str(ROOT/'target/debug/forest-router')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
   for _ in range(100):
    try:request('/');break
    except OSError:time.sleep(.05)
   assert request('/v1/responses',payload,True)[0]==503
   assert request('/admin/api/login',{'password':'test-password'})[0]==200
   for _ in range(60):
    key=json.loads(request('/admin/api/state')[1])['state']['keys']['recover-key']
    if key['retry_at']>now:break
    time.sleep(.1)
   assert key['probe_attempts']==4 and key['retry_at']>now and key['service_failed'],key
   probe_throttled=False
   assert request('/admin/api/verify',{'channel_id':'recover','reset':True})[0]==200
   unlocked=json.loads(request('/admin/api/state')[1])['state']['keys']['recover-key'];assert unlocked['cooldown_until']==0 and not unlocked['service_failed']
   assert request('/v1/responses',payload,True)[0]==200

   cfg=json.loads(request('/admin/api/state')[1])['config'];cfg['new_password']='changed-password';assert request('/admin/api/save',cfg)[0]==200
   assert request('/admin/api/state')[0]==401
   assert request('/admin/api/login',{'password':'changed-password'})[0]==200

   print('PASS: login, configuration, SSE pass-through, header/model substitution, priority failover, failed candidate skip, unknown model, nonportable history, manual check, quota classification, cache cleanup, all unavailable')
  finally:proc.terminate();proc.wait(timeout=5);server.shutdown()
if __name__=='__main__':main()
