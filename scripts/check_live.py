#!/usr/bin/env python3
"""Authorized provider acceptance; only tiny prompts, never emit credentials."""
import os,concurrent.futures,json,pathlib,urllib.request,urllib.error,time
if os.environ.get('FOREST_ALLOW_LIVE')!='1':
 raise SystemExit('Real provider requests require FOREST_ALLOW_LIVE=1 and FOREST_PROVIDER_CONFIG')
ROOT=pathlib.Path(__file__).resolve().parents[1]
(ROOT/'.runtime').mkdir(mode=0o700,exist_ok=True)
SOURCE=pathlib.Path(os.environ['FOREST_PROVIDER_CONFIG'])
def run(job):
 name,p,idx,key,model=job;url=p['base_url'].rstrip('/')+('/v1/responses' if p['base_url'].rstrip('/')=='https://api.deepseek.com' else '/responses')
 body=json.dumps({'model':model,'input':'Reply OK.','stream':True,'max_output_tokens':256,'reasoning':{'effort':'low'}}).encode()
 req=urllib.request.Request(url,data=body,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json','User-Agent':'Mozilla/5.0'})
 result={'channel':name,'key_index':idx,'model':model}
 start=time.monotonic()
 try:
  with urllib.request.urlopen(req,timeout=45) as r:
   result['http']=r.status;result['content_type']=r.headers.get('content-type');b=r.read(256*1024)
  events=[]
  for line in b.splitlines():
   if line.startswith(b'data:'):
    try:
     e=json.loads(line[5:]);events.append(e.get('type'));err=e.get('error') or e.get('response',{}).get('error')
     if isinstance(err,dict):result['error_code']=err.get('code')
    except (ValueError,AttributeError):pass
  result['completed']='response.completed' in events;result['failed']='response.failed' in events;result['incomplete']='response.incomplete' in events;result['bytes']=len(b)
 except urllib.error.HTTPError as e:
  result['http']=e.code
  try:
   err=json.loads(e.read(65536)).get('error',{});result['error_code']=err.get('code') if isinstance(err,dict) else 'error'
  except Exception:pass
 except Exception as e:result['failure']=type(e).__name__
 result['seconds']=round(time.monotonic()-start,2);return result
def main():
 c=json.loads(SOURCE.read_text());jobs=[]
 for name,p in c['providers'].items():
  models=[m['model'] for m in p.get('models',[]) if 'gpt-6' in m.get('model','').lower()]
  if name in ('zhipu','deepseek'):models=[p['models'][0]['model']]
  if models:
   for i,k in enumerate(p.get('api_keys',[])):jobs.append((name,p,i+1,k,models[0]))
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(run,jobs))
 (ROOT/'.runtime/live-results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
 for r in results:print(json.dumps(r,ensure_ascii=False))
if __name__=='__main__':main()
