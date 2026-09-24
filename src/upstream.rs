use crate::{
    balance::{self, Adapter, Allowance},
    config,
};
use futures_util::StreamExt;
use serde_json::Value;
use std::time::Duration;
pub fn client(system_proxy: bool) -> Result<reqwest::Client, reqwest::Error> {
    let builder = reqwest::Client::builder();
    let builder = if system_proxy {
        builder
    } else {
        builder.no_proxy()
    };
    builder
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

pub async fn monitor_json(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    let mut stream = response.bytes_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "检测响应读取失败".to_string())?;
        if data.len() + chunk.len() > 262144 {
            return Err("检测响应超过256KiB".into());
        }
        data.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let parsed = serde_json::from_slice::<Value>(&data).ok();
        return Err(http_error(
            status.as_u16(),
            status.canonical_reason().unwrap_or_default(),
            parsed.as_ref(),
            &content_type,
            data.len(),
        ));
    }
    serde_json::from_slice(&data).map_err(|_| "检测响应不是有效JSON".into())
}

fn status_hint(status: u16) -> &'static str {
    match status {
        400 => "请求参数被上游拒绝",
        401 => "检测 Key 未通过认证",
        402 => "检测账号额度或余额不足",
        403 => "上游拒绝访问",
        404 => "检测接口或模型不存在",
        408 => "上游等待请求超时",
        409 => "请求与上游当前状态冲突",
        413 => "检测请求超过上游大小限制",
        422 => "检测请求参数校验失败",
        429 => "检测请求被上游限流",
        500..=599 => "上游服务异常",
        _ => "上游返回错误",
    }
}

fn push_field(fields: &mut Vec<String>, key: &str, value: &Value) {
    let text = match value {
        Value::String(s) => s.trim(),
        Value::Number(n) => return fields.push(format!("{key}={n}")),
        _ => return,
    };
    if !text.is_empty() {
        let mut short = text.chars().take(180).collect::<String>();
        if short.chars().count() == 180 {
            short.push('…');
        }
        fields.push(format!("{key}={short}"));
    }
}

fn collect_error_fields(v: &Value, fields: &mut Vec<String>) {
    let Some(object) = v.as_object() else {
        return;
    };
    for key in ["code", "type", "message", "msg", "reason"] {
        if fields.len() >= 8 {
            return;
        }
        if let Some(value) = object.get(key) {
            push_field(fields, key, value);
        }
    }
    for key in ["error", "detail"] {
        if fields.len() >= 8 {
            return;
        }
        if let Some(value) = object.get(key) {
            if value.is_object() {
                collect_error_fields(value, fields);
            } else if let Some(items) = value.as_array() {
                for item in items {
                    collect_error_fields(item, fields);
                }
            } else {
                push_field(fields, key, value);
            }
        }
    }
}

fn http_error(
    status: u16,
    reason: &str,
    body: Option<&Value>,
    content_type: &str,
    length: usize,
) -> String {
    let result = format!("HTTP {status} {reason} · {}", status_hint(status));
    let mut fields = Vec::new();
    if let Some(v) = body {
        collect_error_fields(v, &mut fields);
    }
    if fields.is_empty() {
        let kind = if body.is_some() {
            "错误响应未提供可识别字段"
        } else {
            "错误响应不是JSON"
        };
        let media = if content_type.is_empty() {
            format!("{length} 字节")
        } else {
            format!("{content_type}，{length} 字节")
        };
        return format!("{result}：{kind}（{media}）");
    }
    fields.dedup();
    format!("{result}：{}", fields.join("，"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_errors_explain_http_and_upstream_fields() {
        let raw = r#"{"error":{"code":"permission_denied","type":"upstream_forbidden","message":"模型无权限"}}"#.as_bytes();
        let parsed: Value = serde_json::from_slice(raw).unwrap();
        let message = http_error(
            403,
            "Forbidden",
            Some(&parsed),
            "application/json",
            raw.len(),
        );
        assert!(message.contains("HTTP 403 Forbidden"));
        assert!(message.contains("上游拒绝访问"));
        assert!(message.contains("code=permission_denied"));
        assert!(message.contains("message=模型无权限"));

        let html = http_error(403, "Forbidden", None, "text/html", 37);
        assert!(html.contains("HTTP 403 Forbidden"));
        assert!(html.contains("错误响应不是JSON"));
        assert!(html.contains("text/html"));
    }
}
