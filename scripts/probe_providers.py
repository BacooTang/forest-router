#!/usr/bin/env python3
"""Read-only balance probes. Never print keys or raw response bodies."""
import os, concurrent.futures, json, pathlib, urllib.request, urllib.error, urllib.parse
if os.environ.get('FOREST_ALLOW_LIVE')!='1':
 raise SystemExit('Real provider requests require FOREST_ALLOW_LIVE=1 and FOREST_PROVIDER_CONFIG')
SOURCE = pathlib.Path(os.environ['FOREST_PROVIDER_CONFIG'])
PATHS = ['/api/status', '/api/usage/token/', '/v1/usage', '/v1/subscriptions']
def shape(x, depth=0):
    if depth > 5: return '…'
    if isinstance(x,dict):
        return {k:shape(v,depth+1) for k,v in x.items() if not any(s in k.lower() for s in ['key','secret','email','token','name','url','id','message','error'])}
    if isinstance(x,list): return [shape(v,depth+1) for v in x[:3]]
    if isinstance(x,(int,float,bool)) or x is None: return x
    return '<string>'
def probe(job):
    name, idx, base, key, path = job
    req=urllib.request.Request(base+path, headers={'Authorization':'Bearer '+key,'Accept':'application/json','User-Agent':'forest-router-verification/0.1'})
    try:
        with urllib.request.urlopen(req,timeout=18) as r:
            status=r.status; body=r.read(128*1024)
        try: payload=shape(json.loads(body))
        except Exception: payload={'format':'non-json'}
        return {'channel':name,'key_index':idx,'path':path,'status':status,'shape':payload}
    except urllib.error.HTTPError as e:
        return {'channel':name,'key_index':idx,'path':path,'status':e.code}
    except Exception as e:
        return {'channel':name,'key_index':idx,'path':path,'failure':type(e).__name__}
def main():
    config=json.loads(SOURCE.read_text()); jobs=[]
    for name,p in config['providers'].items():
        if not any('gpt-6' in m.get('model','').lower() or 'gpt6' in m.get('model','').lower() for m in p.get('models',[])):continue
        u=urllib.parse.urlsplit(p['base_url']); root=urllib.parse.urlunsplit((u.scheme,u.netloc,'','',''))
        for i,key in enumerate(p.get('api_keys',[])):
            for path in PATHS:
                if path=='/api/status' and i:continue
                jobs.append((name,i+1,root,key,path))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(probe,jobs))
    dest=pathlib.Path('.runtime'); dest.mkdir(mode=0o700,exist_ok=True)
    (dest/'provider-probes.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
    for r in results: print(json.dumps(r,ensure_ascii=False))
if __name__=='__main__':main()
