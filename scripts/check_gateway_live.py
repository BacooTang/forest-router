#!/usr/bin/env python3
"""Real provider calls through isolated Forest gateway, no production changes."""
import json,os,pathlib,subprocess,tempfile,time,urllib.request,http.cookiejar
from check_http import freeport
if os.environ.get('FOREST_ALLOW_LIVE')!='1':
 raise SystemExit('Real provider requests require FOREST_ALLOW_LIVE=1 and FOREST_PROVIDER_CONFIG')
ROOT=pathlib.Path(__file__).resolve().parents[1]
(ROOT/'.runtime').mkdir(mode=0o700,exist_ok=True)
def main():
 source=json.loads(pathlib.Path(os.environ['FOREST_PROVIDER_CONFIG']).read_text());port=freeport();results=[]
 with tempfile.TemporaryDirectory(prefix='forest-live-') as tmp:
  env=dict(os.environ,FOREST_ROUTER_HOME=tmp,FOREST_LISTEN=f'127.0.0.1:{port}',FOREST_ADMIN_PASSWORD='live-validation',FOREST_API_KEY='local-validation-only')
  p=subprocess.Popen([str(ROOT/'target/debug/forest-router')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
  def call(path,data=None,auth=False):
   h={'Content-Type':'application/json','x-forest-admin':'1'}
   if auth:h['Authorization']='Bearer local-validation-only'
   r=opener.open(urllib.request.Request(f'http://127.0.0.1:{port}'+path,data=None if data is None else json.dumps(data).encode(),headers=h),timeout=210)
   with r:
    raw=r.read()
    if path=='/admin/api/save' and r.status==200:data['_revision']=json.loads(raw)['revision']
    return r.status,raw
  try:
   for _ in range(80):
    try:call('/');break
    except OSError:time.sleep(.1)
   call('/admin/api/login',{'password':'live-validation'});cfg=json.loads(call('/admin/api/state')[1])['config']
   names=[n.strip() for n in os.environ['FOREST_LIVE_PROVIDERS'].split(',') if n.strip()]
   if not names:raise ValueError('FOREST_LIVE_PROVIDERS must select at least one provider')
   for name in names:
    provider=source['providers'][name];model=provider['models'][0]['model'];cfg['models']=[{'id':model,'channels':[{'id':name,'name':name,'base_url':provider['base_url'],'upstream_model':model,'adapter':'auto','enabled':True,'keys':[{'id':name+'-key','label':'validation','secret':provider['api_keys'][0]}]}]}]
    try:
     call('/admin/api/save',cfg)
     status,raw=call('/v1/responses',{'model':model,'input':'Reply OK.','stream':True,'max_output_tokens':256,'reasoning':{'effort':'low'}},True)
     completed=b'"response.completed"' in raw
     state=json.loads(call('/admin/api/state')[1])['state'];a=state['keys'][name+'-key'];results.append({'provider':name,'http':status,'completed':completed,'balance_adapter':a['detected'],'balance_available':not a['allowance']['exhausted'],'last_used':state['last_used'].get(model)})
    except Exception as e:results.append({'provider':name,'failure':type(e).__name__})
   provider=source['providers'][names[0]];model=provider['models'][0]['model']
   cfg['models']=[{'id':model,'channels':[{'id':'quality-live','name':'quality-live','base_url':provider['base_url'],'upstream_model':model,'adapter':'auto','enabled':True,'monitor_id':'live-monitor','keys':[{'id':'quality-live-key','label':'validation','secret':provider['api_keys'][0]}]}]}]
   cfg['monitors']=[{'id':'live-monitor','name':'live monitor','base_url':provider['base_url'],'key':provider['api_keys'][0],'model':model}]
   call('/admin/api/save',cfg)
   call('/admin/api/verify',{'monitor_id':'live-monitor'})
   quality=json.loads(call('/admin/api/state')[1])['state']['quality']['live-monitor']
   print('Live quality:',json.dumps({k:quality[k] for k in ['verdict','checked_at','next_at','error']},ensure_ascii=False))
   (ROOT/'.runtime/quality-live-results.json').write_text(json.dumps(quality,ensure_ascii=False,indent=2))
   print(json.dumps(results,ensure_ascii=False,indent=2));(ROOT/'.runtime/gateway-live-results.json').write_text(json.dumps(results,indent=2))
   assert all(r.get('completed') and r.get('balance_available') for r in results)
  finally:p.terminate();p.wait(timeout=10)
if __name__=='__main__':main()
