#!/usr/bin/env python3
"""Isolated sustained requests and cancellation RSS/FD acceptance."""
import concurrent.futures,http.server,json,os,pathlib,socket,subprocess,tempfile,threading,time,urllib.request
from check_http import freeport
ROOT=pathlib.Path(__file__).resolve().parents[1]
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def do_GET(self):
  b=b'{"mode":"unrestricted","remaining":100}';self.send_response(200);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def do_POST(self):
  d=json.loads(self.rfile.read(int(self.headers['Content-Length'])));self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
  try:
   self.wfile.write(b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n');self.wfile.flush()
   if d.get('input')=='cancel':
    for _ in range(200):self.wfile.write(b': heartbeat\n\n');self.wfile.flush();time.sleep(.01)
   self.wfile.write(b'data: {"type":"response.completed","response":{"status":"completed"}}\n\n')
  except (BrokenPipeError,ConnectionResetError):pass
def main():
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
 with tempfile.TemporaryDirectory(prefix='forest-memory-') as tmp:
  port=freeport();env=dict(os.environ,FOREST_ROUTER_HOME=tmp,FOREST_LISTEN=f'127.0.0.1:{port}',FOREST_ADMIN_PASSWORD='memory-test',FOREST_API_KEY='memory-key')
  binary=ROOT/'target/release/forest-router';proc=subprocess.Popen([str(binary)],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  try:
   for _ in range(100):
    if pathlib.Path(tmp,'config.json').exists():break
    time.sleep(.05)
   proc.terminate();proc.wait(timeout=5)
   path=pathlib.Path(tmp,'config.json');cfg=json.loads(path.read_text());cfg['models']=[{'id':'m','channels':[{'id':'c','name':'c','base_url':f'http://127.0.0.1:{server.server_port}/v1','upstream_model':'upstream','adapter':'sub2_api','enabled':True,'keys':[{'id':'k','label':'k','secret':'test'}]}]}];path.write_text(json.dumps(cfg))
   proc=subprocess.Popen([str(binary)],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);time.sleep(.4)
   def rss():return int(subprocess.check_output(['ps','-o','rss=','-p',str(proc.pid)]).strip())
   def call(i,cancel=False):
    body=json.dumps({'model':'m','input':'cancel' if cancel else ('x'*32768),'stream':True}).encode();req=urllib.request.Request(f'http://127.0.0.1:{port}/v1/responses',data=body,headers={'Authorization':'Bearer memory-key','Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=10) as r:
     if cancel:r.readline()
     else:assert b'response.completed' in r.read()
   samples=[{'phase':'idle','rss_kib':rss()}]
   with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
    for batch in range(5):
     list(pool.map(call,range(400)));samples.append({'phase':f'requests_{(batch+1)*400}','rss_kib':rss()})
    list(pool.map(lambda i:call(i,True),range(200)))
   time.sleep(3);samples.append({'phase':'after_200_cancellations','rss_kib':rss()})
   # More than concurrent limit must still succeed after disconnects: permits returned.
   with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:list(pool.map(call,range(200)))
   time.sleep(2);samples.append({'phase':'settled','rss_kib':rss()})
   assert samples[-1]['rss_kib']<150*1024,samples
   assert samples[-1]['rss_kib']-samples[2]['rss_kib']<30*1024,samples
   proc.terminate();proc.wait(timeout=10)
   telemetry=json.loads(pathlib.Path(tmp,'state.json').read_text())['telemetry']
   assert telemetry['today']['requests']==2400,telemetry['today']
   assert telemetry['today']['success']==2200,telemetry['today']
   assert telemetry['today']['cancelled']==200,telemetry['today']
   assert len(telemetry['failures'])==200
   print(json.dumps({'requests':2200,'cancelled':200,'samples':samples},indent=2));(ROOT/'.runtime/memory-results.json').write_text(json.dumps(samples,indent=2))
  finally:proc.terminate();proc.wait(timeout=10);server.shutdown()
if __name__=='__main__':main()
