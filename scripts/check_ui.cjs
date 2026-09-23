const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const fs=require('fs'),os=require('os'),path=require('path'),cp=require('child_process'),http=require('http');
(async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'forest-ui-'));
 const upstream=http.createServer((req,res)=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({mode:'unrestricted',remaining:80,unit:'USD'}))});await new Promise(r=>upstream.listen(0,'127.0.0.1',r));
 const reservation=http.createServer();await new Promise(r=>reservation.listen(0,'127.0.0.1',r));const port=reservation.address().port;await new Promise(r=>reservation.close(r));
 const child=cp.spawn(path.join(__dirname,'../target/debug/forest-router'),{env:{...process.env,FOREST_ROUTER_HOME:dir,FOREST_LISTEN:`127.0.0.1:${port}`,FOREST_ADMIN_PASSWORD:'ui-password',FOREST_API_KEY:'ui-company-key'},stdio:'ignore'});
 let browser;
 try{
  for(let i=0;i<80;i++){try{await fetch(`http://127.0.0.1:${port}`);break}catch{await new Promise(r=>setTimeout(r,100))}}
  browser=await chromium.launch({headless:true});const page=await browser.newPage({viewport:{width:1440,height:1000}});let errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto(`http://127.0.0.1:${port}`);await page.locator('#password').fill('ui-password');await page.getByRole('button',{name:'登录',exact:true}).click();await page.locator('#app').waitFor({state:'visible'});
  await page.getByRole('button',{name:'创建第一个模型'}).click();await page.locator('#modelName').fill('gpt-6-astra');await page.locator('#modelForm button[type=submit]').click();await page.locator('#dialog').waitFor({state:'hidden'});
  await page.getByRole('button',{name:'＋ 添加渠道',exact:true}).click();await page.locator('#cname').fill('示例主渠道');await page.locator('#cbase').fill(`http://127.0.0.1:${upstream.address().port}/v1`);await page.locator('[data-secret="0"]').fill('fixture-key');await page.getByRole('button',{name:'识别平台与额度',exact:true}).click();await page.getByText('主 Key：80.00 USD',{exact:true}).waitFor();await page.locator('#channelForm button[type=submit]').click();await page.locator('#dialog').waitFor({state:'hidden'});await page.locator('.card.current').waitFor();
  await page.screenshot({path:path.join(__dirname,'../.runtime/ui-codex.png'),fullPage:true});
  await page.locator('#nav button').filter({hasText:'运行统计'}).click();await page.getByRole('heading',{name:'运行统计',exact:true}).waitFor();await page.locator('#traceSearch').fill('fr_missing');await page.getByText('暂无匹配记录',{exact:true}).waitFor();await page.setViewportSize({width:375,height:800});if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth))throw Error('traffic mobile overflow');await page.setViewportSize({width:1440,height:1000});
  await page.locator('#nav button').filter({hasText:'日志'}).click();await page.getByRole('heading',{name:'事件日志'}).waitFor();if(!await page.locator('.log').textContent())throw Error('missing logs');
  await page.locator('#nav button').filter({hasText:'设置'}).click();if(await page.locator('#notifyAllMonitors').isChecked())throw Error('summary must default off');await page.locator('#notifyAllMonitors').check();await page.locator('#apiKey').fill('changed-company-key');await page.getByRole('button',{name:'保存设置'}).click();await page.waitForTimeout(300);if(!await page.locator('#notifyAllMonitors').isChecked())throw Error('summary setting not retained');
  await page.locator('#nav button').filter({hasText:'配置中心'}).click();await page.getByRole('button',{name:'编辑',exact:true}).click();if(await page.locator('[data-secret="0"]').inputValue()!=='fixture-key')throw Error('plaintext key not retained');await page.getByRole('button',{name:'取消',exact:true}).click();
  await page.setViewportSize({width:750,height:1000});await page.screenshot({path:path.join(__dirname,'../.runtime/ui-narrow.png'),fullPage:true});
  if(errors.length)throw Error(errors.join('\n'));console.log('PASS UI: login, model creation, provider identification, plaintext key persistence, channel save, current-route highlight, logs and settings; no JS errors');
 }finally{if(browser)await browser.close();child.kill('SIGTERM');await new Promise(r=>child.on('exit',r));upstream.close();fs.rmSync(dir,{recursive:true,force:true});}
})().catch(e=>{console.error(e);process.exitCode=1});
