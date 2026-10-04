#!/usr/bin/env python3
"""Knowledge coverage acceptance, with isolated data and a local mock upstream."""
import concurrent.futures
import http.server
import json
import os
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROMPT = (ROOT / 'src/knowledge_prompt.txt').read_text()
COMPLETE = '\n'.join(f'Q{i:02}｜'+('English response' if i % 2 == 0 else '中文回答') for i in range(1,26))

class Upstream(http.server.BaseHTTPRequestHandler):
    seen = []
    lock = threading.Lock()
    barrier = None
    active = 0
    peak = 0
    entered = threading.Event()
    release = None
    mode = 'mixed'

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        assert self.path == '/v1/chat/completions'
        assert self.headers['Authorization'] == 'Bearer fixture-key'
        assert body['model'] == 'fixture-model'
        assert body['messages'] == [{'role':'user','content':PROMPT}]
        assert body['stream'] is False and body['thinking'] == {'type':'enabled'}
        assert 'tools' not in body
        effort = body['reasoning_effort']
        with self.lock:
            type(self).active += 1
            type(self).peak = max(self.peak, self.active)
            self.seen.append(effort)
        try:
            self.entered.set()
            if self.barrier:
                self.barrier.wait(timeout=5)
            if self.release:
                assert self.release.wait(timeout=5)
            text = COMPLETE
            status, finish = 200, 'stop'
            if self.mode == 'mixed' and effort == 'medium':
                text = text.replace('Q24｜English response', 'Q24｜不清楚')
            elif self.mode == 'mixed' and effort == 'high':
                text = text.replace('Q25｜中文回答', 'Q25｜---')
            elif self.mode == 'missing':
                text = '\n'.join(text.splitlines()[:24])
            elif self.mode == 'empty':
                text = ''
            elif self.mode == 'truncated':
                finish = 'length'
            elif self.mode == 'http':
                status = 429
            elif self.mode == 'large':
                text = 'x' * 262145
            value = {'choices':[{'message':{'content':text},'finish_reason':finish}]}
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        finally:
            with self.lock:
                type(self).active -= 1

def main():
    upstream = http.server.ThreadingHTTPServer(('127.0.0.1',0),Upstream)
    threading.Thread(target=upstream.serve_forever,daemon=True).start()
    with tempfile.TemporaryDirectory(prefix='forest-knowledge-') as directory:
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1',0))
            port = reservation.getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        env = dict(os.environ,FOREST_ROUTER_HOME=directory,FOREST_LISTEN=f'127.0.0.1:{port}',
                   FOREST_ADMIN_PASSWORD='fixture-password',FOREST_API_KEY='fixture-company-key')
        child = subprocess.Popen([str(ROOT/'target/debug/forest-router')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        cookie = ''
        def request(path, data=None, authenticated=True, admin=True):
            headers = {'content-type':'application/json'}
            if authenticated:
                headers['cookie'] = cookie
            if admin:
                headers['x-forest-admin'] = '1'
            req = urllib.request.Request(base+path,headers=headers,data=None if data is None else json.dumps(data).encode())
            try:
                with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=10) as response:
                    raw = response.read()
                    return response.status, raw, response.headers
            except urllib.error.HTTPError as error:
                return error.code, error.read(), error.headers
        def state():
            return json.loads(request('/admin/api/state')[1])
        def save(cfg):
            code, raw, _ = request('/admin/api/save',cfg)
            assert code == 200, raw
            cfg['_revision'] = json.loads(raw)['revision']
        def check(effort='medium'):
            code, raw, _ = request('/admin/api/knowledge',{'monitor_id':'fixture','effort':effort,'revision':cfg['_revision']})
            assert code == 200, raw
            return json.loads(raw)
        try:
            for _ in range(100):
                try:
                    request('/')
                    break
                except OSError:
                    time.sleep(.05)
            code, _, headers = request('/admin/api/login',{'password':'fixture-password'})
            assert code == 200
            cookie = headers['set-cookie'].split(';')[0]
            cfg = state()['config']
            cfg['monitors'] = [{'id':'fixture','name':'知识检测','base_url':f'http://127.0.0.1:{upstream.server_port}/v1/responses','key':'fixture-key','model':'fixture-model'}]
            save(cfg)
            before = state()
            payload = {'monitor_id':'fixture','effort':'medium','revision':cfg['_revision']}
            assert request('/admin/api/knowledge',payload,authenticated=False)[0] == 401
            assert request('/admin/api/knowledge',payload,admin=False)[0] == 401
            assert request('/admin/api/knowledge',dict(payload,effort='low'))[0] == 400
            assert request('/admin/api/knowledge',dict(payload,monitor_id='missing'))[0] == 404
            assert request('/admin/api/knowledge',dict(payload,revision='stale'))[0] == 409
            Upstream.barrier = threading.Barrier(3)
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                results = list(pool.map(check,['medium','high','xhigh']))
            Upstream.barrier = None
            assert Upstream.peak == 3, 'requests did not overlap'
            assert Upstream.seen == list(dict.fromkeys(Upstream.seen)) and set(Upstream.seen) == {'medium','high','xhigh'}
            assert [(r['verdict'],r['answered']) for r in results] == [('degraded',24),('degraded',24),('healthy',25)]
            assert results[0]['questions'][24]['answered'], 'unknown must not stop later questions'
            assert results[2]['groups'][-1]['answered'] == 5
            for mode, verdict, count in [('missing','degraded',24),('empty','degraded',0),('truncated','unknown',25),('http','unknown',0),('large','unknown',0)]:
                Upstream.mode = mode
                result = check()
                assert (result['verdict'],result['answered']) == (verdict,count), (mode,result)
            Upstream.mode = 'complete'
            Upstream.entered.clear()
            Upstream.release = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(request,'/admin/api/knowledge',payload)
                assert Upstream.entered.wait(5)
                assert request('/admin/api/knowledge',payload)[0] == 409, 'duplicate effort must not issue another request'
                cfg['monitors'][0]['model'] = 'changed-model'
                save(cfg)
                Upstream.release.set()
                assert future.result()[0] == 409, 'stale results must not be displayed'
            after = state()
            assert before['state']['quality'] == after['state']['quality'], 'knowledge test changed automatic quality state'
            assert before['traffic'] == after['traffic'], 'knowledge test counted as business traffic'
            assert before['client_usage'] == after['client_usage'], 'knowledge test counted employee usage'
            assert not any(e['kind']=='quality' for e in after['state']['events'])
            print('PASS knowledge: three efforts overlap; exact prompt, coverage-only judgement, unknown/blank/missing/placeholders, independent questions, truncation/HTTP/size errors, auth, deduplication, config protection; no route or usage mutation')
        finally:
            Upstream.barrier = None
            if Upstream.release:
                Upstream.release.set()
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            upstream.shutdown()
            upstream.server_close()

if __name__ == '__main__':
    main()
