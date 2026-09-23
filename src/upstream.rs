use crate::{
    balance::{self, Adapter, Allowance},
    config,
};
use futures_util::StreamExt;
use serde_json::Value;
use std::time::Duration;
pub fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(600))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
}
pub async fn bounded_json(response: reqwest::Response) -> Result<Value, String> {
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status().as_u16()));
    }
    let mut stream = response.bytes_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "读取响应失败")?;
        if data.len() + chunk.len() > 262144 {
            return Err("额度响应超过256KiB".into());
        }
        data.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&data).map_err(|_| "接口未返回有效JSON".into())
}
async fn query(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    raw_auth: bool,
) -> Result<Value, String> {
    let r = client.get(url).timeout(Duration::from_secs(20));
    let r = if raw_auth {
        r.header("Authorization", key)
    } else {
        r.bearer_auth(key)
    };
    let response = r.send().await.map_err(|_| "额度接口连接失败")?;
    bounded_json(response).await
}
pub async fn allowance(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    adapter: Adapter,
) -> Result<(Adapter, Allowance), String> {
    let root = config::root(base);
    let candidates: Vec<(Adapter, String, bool)> = match adapter {
        Adapter::Deepseek => vec![(
            Adapter::Deepseek,
            "https://api.deepseek.com/user/balance".into(),
            false,
        )],
        Adapter::Glm => vec![(
            Adapter::Glm,
            "https://open.bigmodel.cn/api/monitor/usage/quota/limit".into(),
            true,
        )],
        Adapter::Subscription => vec![(
            Adapter::Subscription,
            format!("{root}/v1/subscriptions"),
            false,
        )],
        Adapter::NewApi => vec![(Adapter::NewApi, format!("{root}/api/usage/token/"), false)],
        Adapter::Sub2Api => vec![(Adapter::Sub2Api, format!("{root}/v1/usage"), false)],
        Adapter::Auto => vec![
            (Adapter::Sub2Api, format!("{root}/v1/usage"), false),
            (Adapter::NewApi, format!("{root}/api/usage/token/"), false),
        ],
    };
    let mut errors = Vec::new();
    for (kind, url, raw) in candidates {
        match query(client, &url, key, raw).await {
            Ok(v) => match balance::parse(&kind, &v) {
                Ok(a) => return Ok((kind, a)),
                Err(e) => errors.push(e.to_owned()),
            },
            Err(e) => errors.push(e),
        }
    }
    Err(errors.join("；"))
}
