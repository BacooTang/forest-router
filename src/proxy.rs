use crate::{app::App, config, state::KeyState};
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[derive(Clone)]
struct RouterError(String);
fn error(status: StatusCode, code: &str, message: &str) -> Response {
    let mut response = (
        status,
        Json(json!({"error":{"code":code,"message":message,"type":"router_error"}})),
    )
        .into_response();
    response.extensions_mut().insert(RouterError(code.into()));
    response
}
/// Public model catalog; availability is evaluated when a response is requested.
pub async fn models(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let cfg = app.config.read().await;
    if cfg
        .authenticate(
            headers
                .get("authorization")
                .and_then(|h| h.to_str().ok())
                .unwrap_or(""),
        )
        .is_none()
    {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "公司 API Key 不正确",
        );
    }
    let models: Vec<_> = cfg.models.iter().map(|model| {
        json!({"id": model.id, "object": "model", "created": 0, "owned_by": "forest-router"})
    }).collect();
    (
        [("cache-control", "no-store")],
        Json(json!({"object": "list", "data": models})),
    )
        .into_response()
}
pub async fn responses(State(app): State<Arc<App>>, request: Request) -> Response {
    let (trace, mut guard) = crate::telemetry::Trace::new(app.metrics.clone());
    let response = responses_inner(app, request, trace.clone()).await;
    trace.response(
        response.status().as_u16(),
        response
            .extensions()
            .get::<RouterError>()
            .map(|e| e.0.as_str()),
    );
    let success_http = response.status().is_success();
    let (mut parts, body) = response.into_parts();
    parts
        .headers
        .insert("x-request-id", trace.id().parse().unwrap());
    let stream = async_stream::stream! {
        let mut body = body.into_data_stream();
        let _keep_alive = &mut guard;
        while let Some(chunk) = body.next().await {
            if let Ok(bytes) = &chunk && success_http && !bytes.is_empty() { trace.output(); }
            if chunk.is_err() { trace.result("failed", "downstream_body_error"); }
            yield chunk;
        }
        guard.finish(false);
    };
    Response::from_parts(parts, Body::from_stream(stream))
}
async fn responses_inner(
    app: Arc<App>,
    request: Request,
    trace: crate::telemetry::Trace,
) -> Response {
    let (parts, incoming) = request.into_parts();
    let headers = parts.headers;
    let cfg = app.config.read().await.clone();
    let identity = cfg.authenticate(
        headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .unwrap_or(""),
    );
    if identity.is_none() {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "公司 API Key 不正确",
        );
    }
    let (id, name) = identity.unwrap();
    trace.client(id, name);
    trace.begin(&app.metrics);
    let mut body = Vec::new();
    let mut incoming = incoming.into_data_stream();
    let read = tokio::time::timeout(Duration::from_secs(600), async {
        while let Some(chunk) = incoming.next().await {
            let chunk = chunk.map_err(|_| StatusCode::BAD_REQUEST)?;
            body.extend_from_slice(&chunk);
        }
        Ok(())
    })
    .await;
    match read {
        Ok(Ok(())) => {}
        Ok(Err(status)) => {
            return error(status, "request_read_error", "请求正文读取失败");
        }
        Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "body_timeout", "读取请求超时"),
    }
    // RawValue keeps all non-model JSON values untouched, including unknown fields.
    let mut payload: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                    "请求必须是JSON对象",
                );
            }
        };
    let model = payload
        .get("model")
        .and_then(|m| serde_json::from_str::<String>(m.get()).ok());
    let Some(model) = model else {
        return error(StatusCode::BAD_REQUEST, "missing_model", "缺少模型");
    };
    let Some(route) = cfg.models.iter().find(|m| m.id == model) else {
        return error(StatusCode::NOT_FOUND, "model_not_found", "模型未配置");
    };
    trace.model(&model);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    // External stored response identifiers are not portable across vendors.
    let pinned = payload
        .get("previous_response_id")
        .is_some_and(|x| x.get() != "null")
        || payload
            .get("conversation")
            .is_some_and(|x| x.get() != "null");
    if pinned {
        return error(
            StatusCode::BAD_REQUEST,
            "nonportable_history",
            "此网关需要完整请求历史，不支持 previous_response_id 或 conversation 跨渠道状态引用",
        );
    }

    let mut attempted = std::collections::HashSet::new();
    'channels: for channel in &route.channels {
        if !channel.enabled {
            continue;
        }
        let (keys, start) = {
            let mut state = app.state.lock().await;
            let now = chrono::Utc::now().timestamp();
            if let Some(id) = &channel.monitor_id
                && !state
                    .quality
                    .get(id)
                    .is_some_and(|q| q.last_definite == "healthy" && q.valid_until > now)
            {
                continue;
            }
            let eligible = channel
                .keys
                .iter()
                .enumerate()
                .filter(|(_, k)| k.enabled && state.keys.get(&k.id).is_none_or(|s| s.eligible(now)))
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            let index = state.round_robin.entry(channel.id.clone()).or_default();
            let start = *index;
            *index = index.wrapping_add(1);
            (eligible, start)
        };
        for offset in 0..keys.len() {
            let key = &channel.keys[keys[(start + offset) % keys.len()]];
            if tokio::time::Instant::now() >= deadline {
                break 'channels;
            }
            // A repeated configuration of the same credential/model is one candidate.
            let identity = (
                config::responses_url(&channel.base_url),
                channel.upstream_model.clone(),
                key.secret.clone(),
            );
            if !attempted.insert(identity) {
                continue;
            }
            // Recheck switches from the current configuration before dispatch.
            let enabled = app
                .config
                .read()
                .await
                .models
                .iter()
                .flat_map(|m| &m.channels)
                .any(|c| {
                    c.id == channel.id
                        && c.enabled
                        && c.keys.iter().any(|k| k.id == key.id && k.enabled)
                });
            if !enabled {
                continue;
            }
            // Recheck quality before every attempt, including retries within one provider.
            // A newly degraded monitor excludes the entire linked provider.
            {
                let state = app.state.lock().await;
                let now = chrono::Utc::now().timestamp();
                if !state.channel_ready(channel, now) {
                    continue 'channels;
                }
                if state.keys.get(&key.id).is_some_and(|s| !s.eligible(now)) {
                    continue;
                }
            }

            let Ok(raw) = serde_json::value::to_raw_value(&channel.upstream_model) else {
                continue;
            };
            payload.insert("model".into(), raw);
            let Ok(out) = serde_json::to_vec(&payload) else {
                continue;
            };
            let mut request = app
                .upstream_client(cfg.use_system_proxy)
                .post(config::responses_url(&channel.base_url))
                .bearer_auth(&key.secret)
                .header("content-type", "application/json")
                .body(out);
            // Forward end-to-end request headers, excluding credentials and framing.
            let connection_headers = headers
                .get("connection")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .collect::<Vec<_>>();
            for (name, value) in &headers {
                if !connection_headers.iter().any(|h| h == name.as_str())
                    && !matches!(
                        name.as_str(),
                        "authorization"
                            | "host"
                            | "content-length"
                            | "content-type"
                            | "connection"
                            | "transfer-encoding"
                            | "cookie"
                            | "proxy-authorization"
                            | "accept-encoding"
                            | "x-api-key"
                            | "api-key"
                            | "keep-alive"
                            | "te"
                            | "trailer"
                            | "upgrade"
                            | "openai-organization"
                            | "openai-project"
                    )
                {
                    request = request.header(name, value);
                }
            }
            trace.attempt(channel, key);
            let result = tokio::time::timeout_at(
                deadline.min(tokio::time::Instant::now() + Duration::from_secs(600)),
                request.send(),
            )
            .await;
            let response = match result {
                Ok(Ok(r)) => r,
                _ => {
                    trace.retry("connection_or_timeout");
                    fail(&app, &cfg, &key.id, &channel.name, 0).await;
                    continue;
                }
            };
            let status = response.status();
            trace.status(status.as_u16());
            if !status.is_success() {
                let retry = response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(60)
                    .clamp(1, 3600);
                let mut raw = Vec::new();
                let mut source = response.bytes_stream();
                let _ = tokio::time::timeout_at(
                    deadline.min(tokio::time::Instant::now() + Duration::from_secs(600)),
                    async {
                        while let Some(Ok(b)) = source.next().await {
                            raw.extend_from_slice(&b);
                        }
                    },
                )
                .await;
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&raw) {
                    trace.usage(crate::usage::Tokens::parse(&v["usage"]));
                }
                let classified = classify(status.as_u16(), &raw);
                if classified == 400 {
                    return (status, [("content-type", "application/json")], raw).into_response();
                }
                trace.retry(crate::telemetry::failure_reason(classified));
                fail(&app, &cfg, &key.id, &channel.name, classified).await;
                if classified == 429
                    && let Some(s) = app.state.lock().await.keys.get_mut(&key.id)
                {
                    s.retry_at = chrono::Utc::now().timestamp() + retry;
                }
                continue;
            }
            let mut builder = Response::builder().status(status);
            let connection_headers = response
                .headers()
                .get("connection")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .collect::<Vec<_>>();
            for (name, value) in response.headers() {
                if !connection_headers.iter().any(|h| h == name.as_str())
                    && !matches!(
                        name.as_str(),
                        "connection"
                            | "transfer-encoding"
                            | "content-length"
                            | "set-cookie"
                            | "keep-alive"
                            | "proxy-authenticate"
                            | "upgrade"
                            | "trailer"
                            | "te"
                    )
                {
                    builder = builder.header(name, value);
                }
            }
            let is_sse = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream"));
            // Non-streaming responses must be checked before committing HTTP 200.
            if !is_sse {
                let mut source = response.bytes_stream();
                let mut bytes = Vec::new();
                let collected = tokio::time::timeout_at(
                    deadline.min(tokio::time::Instant::now() + Duration::from_secs(600)),
                    async {
                        while let Some(chunk) = source.next().await {
                            let chunk = chunk.map_err(|_| 503u16)?;
                            bytes.extend_from_slice(&chunk);
                        }
                        Ok::<(), u16>(())
                    },
                )
                .await;
                if !matches!(collected, Ok(Ok(()))) {
                    trace.retry("upstream_body_error");
                    fail(&app, &cfg, &key.id, &channel.name, 503).await;
                    continue;
                }
                let v = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
                trace.usage(
                    v.as_ref()
                        .and_then(|v| crate::usage::Tokens::parse(&v["usage"])),
                );
                if !v.as_ref().is_some_and(crate::errors::valid_response) {
                    let class = classify(status.as_u16(), &bytes);
                    trace.retry(crate::telemetry::failure_reason(class));
                    fail(&app, &cfg, &key.id, &channel.name, class).await;
                    continue;
                }
                trace.first();
                trace.result("success", "completed");
                record_success(&app, &cfg, &key.id).await;
                record_route(&app, &cfg, &model, &channel.id, &channel.name).await;
                let output =
                    async_stream::stream! {yield Ok::<Bytes,std::io::Error>(Bytes::from(bytes));};
                return builder.body(Body::from_stream(output)).unwrap_or_else(|_| {
                    error(StatusCode::BAD_GATEWAY, "response_error", "响应构造失败")
                });
            }
            let mut source = response.bytes_stream();
            let mut observer = crate::sse::Observer::default();
            let mut prefix = Vec::<Bytes>::new();
            let mut prefix_size = 0;
            let mut early_failure = false;
            if status.is_success() && is_sse {
                let first_deadline =
                    deadline.min(tokio::time::Instant::now() + Duration::from_secs(10));
                loop {
                    match tokio::time::timeout_at(first_deadline, source.next()).await {
                        Ok(Some(Ok(bytes))) => {
                            observer.feed(&bytes);
                            trace.usage(observer.usage.take());
                            if observer.output_text {
                                trace.first();
                            }
                            prefix_size += bytes.len();
                            prefix.push(bytes);
                            if observer.retryable_failure {
                                early_failure = true;
                                break;
                            }
                            if observer.meaningful || prefix_size >= 65536 {
                                break;
                            }
                        }
                        Ok(None) => {
                            observer.finish();
                            trace.usage(observer.usage.take());
                            if observer.retryable_failure {
                                early_failure = true;
                                break;
                            }
                            if prefix.is_empty() {
                                return error(
                                    StatusCode::BAD_GATEWAY,
                                    "empty_upstream_response",
                                    "上游返回空SSE；当前请求未重放",
                                );
                            }
                            break;
                        }
                        Ok(Some(Err(_))) => {
                            early_failure = true;
                            break;
                        }
                        // Observation window expiration commits the stream, even if empty.
                        // It is not evidence of outage and never triggers replay.
                        Err(_) => break,
                    }
                }
            }
            if early_failure {
                trace.retry(crate::telemetry::failure_reason(
                    observer.failure_status.unwrap_or(503),
                ));
                fail(
                    &app,
                    &cfg,
                    &key.id,
                    &channel.name,
                    observer.failure_status.unwrap_or(503),
                )
                .await;
                continue;
            }
            record_route(&app, &cfg, &model, &channel.id, &channel.name).await;
            let app2 = app.clone();
            let config2 = cfg.clone();
            let key_id = key.id.clone();
            let channel_name = channel.name.clone();
            let stream = async_stream::stream! {

                            if observer.terminal && !observer.failed {record_success(&app2,&config2,&key_id).await;}
                            for bytes in prefix {yield Ok::<Bytes,std::io::Error>(bytes);}
                            loop {
                                match tokio::time::timeout(Duration::from_secs(600),source.next()).await {
                                    Ok(Some(Ok(bytes)))=>{if is_sse {observer.feed(&bytes); trace.usage(observer.usage.take());if observer.output_text {trace.first();}
            if observer.terminal && !observer.failed {record_success(&app2,&config2,&key_id).await;}}yield Ok::<Bytes,std::io::Error>(bytes);},
                                    Ok(None)=>{
                                        observer.finish(); trace.usage(observer.usage.take());
                                        if observer.failed {trace.result("failed", crate::telemetry::failure_reason(observer.failure_status.unwrap_or(503)));fail(&app2,&config2,&key_id,&channel_name,observer.failure_status.unwrap_or(503)).await;}
                                        else if !observer.terminal {trace.result("unknown", "unrecognized_terminal");protocol_warning(&app2,&config2,&key_id,&channel_name).await;}
                                        else {trace.result("success", "completed");record_success(&app2,&config2,&key_id).await;}
                                        break;
                                    },
                                    _=>{trace.result("failed", "stream_interrupted_or_timeout");fail(&app2,&config2,&key_id,&channel_name,0).await;break;}
                                }
                            }
                        };
            return builder.body(Body::from_stream(stream)).unwrap_or_else(|_| {
                error(StatusCode::BAD_GATEWAY, "response_error", "响应构造失败")
            });
        }
    }
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "no_available_channel",
        "当前没有可用渠道",
    )
}
async fn protocol_warning(app: &App, cfg: &Arc<config::Config>, id: &str, name: &str) {
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return;
    }
    let mut state = app.state.lock().await;
    let k = state.keys.entry(id.into()).or_default();
    if !k.protocol_warning {
        k.protocol_warning = true;
        state.event(
            "protocol",
            format!("{name}：SSE正常关闭但未识别终态，保持原文，不判定Key故障"),
        );
    }
}
// Successful business traffic is service evidence, but never clears a newer fault or cooldown.
async fn record_success(app: &App, cfg: &Arc<config::Config>, id: &str) {
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return;
    }
    let mut state = app.state.lock().await;
    let key = state.keys.entry(id.to_owned()).or_default();
    key.checked = true;
}
async fn record_route(app: &App, cfg: &Arc<config::Config>, model: &str, id: &str, name: &str) {
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return;
    }
    let mut state = app.state.lock().await;
    let previous = state.last_used.insert(model.into(), id.into());
    if previous.as_deref() != Some(id) {
        let returning = previous.as_ref().is_some_and(|old| {
            cfg.models.iter().find(|m| m.id == model).is_some_and(|m| {
                m.channels.iter().position(|c| c.id == id)
                    < m.channels.iter().position(|c| c.id == *old)
            })
        });
        if previous.is_some() {
            app.metrics.lock().unwrap().route_switch(returning);
        }
        state.event("route", format!("{model} → {name}（按优先级选择）"));
    }
}

async fn fail(app: &App, expected: &Arc<config::Config>, id: &str, name: &str, status: u16) {
    let cfg = app.config.read().await;
    if !Arc::ptr_eq(&cfg, expected) {
        return;
    }
    if !cfg
        .models
        .iter()
        .flat_map(|m| &m.channels)
        .flat_map(|c| &c.keys)
        .any(|k| k.id == id)
    {
        return;
    }
    let mut state = app.state.lock().await;
    let now = chrono::Utc::now().timestamp();
    let s = state
        .keys
        .entry(id.to_owned())
        .or_insert_with(KeyState::default);
    let was_suspect = s.suspect;
    let (changed, kind) = match status {
        401 => {
            let changed = !s.credential_failed;
            s.credential_failed = true;
            s.revision += 1;
            (changed, "credential")
        }
        402 => {
            s.request_exhausted = true;
            s.balance_checked = chrono::Utc::now().timestamp();
            let a = s.allowance.get_or_insert_with(Default::default);
            let changed = !a.exhausted;
            a.exhausted = true;
            s.revision += 1;
            (changed, "balance")
        }
        429 => {
            let changed = s.retry_at <= now;
            s.retry_at = now + 60;
            s.revision += 1;
            (changed, "rate_limit")
        }
        _ => (s.suspect_service(now), "service"),
    };
    s.reason = if s.suspect {
        "暂时异常，等待快速确认"
    } else {
        match status {
            401 => "凭证失效",
            402 => "额度耗尽",
            429 => "上游限流，暂时跳过",
            0 => "连接失败或流中断",
            _ => "上游服务或未知协议错误，暂停路由并受限探测",
        }
    }
    .into();
    if s.suspect && !was_suspect {
        state.event("service", format!("{name} / {id}：暂时异常，等待快速确认"));
    }
    if changed && matches!(kind, "balance" | "service") && !cfg.webhook.is_empty() {
        state.notify(format!(
            "{} / {}：{}",
            name,
            id,
            if kind == "balance" {
                "额度耗尽"
            } else {
                "服务中断"
            }
        ));
    }
    if changed {
        state.event(
            kind,
            format!(
                "{name} / {id}：{}",
                if status == 0 {
                    "连接失败".into()
                } else {
                    format!("HTTP {status}")
                }
            ),
        );
    }
}

pub use crate::errors::classify;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_supplier_errors() {
        assert_eq!(
            classify(403, br#"{"error":{"code":"insufficient_user_quota"}}"#),
            402
        );
        assert_eq!(classify(429, br#"{"code":"rate_limit_exceeded"}"#), 429);
        assert_eq!(classify(403, br#"{"code":"INSUFFICIENT_BALANCE"}"#), 402);
        assert_eq!(classify(429, br#"{"code":"USAGE_LIMIT_EXCEEDED","message":"WEEKLY_LIMIT_EXCEEDED: weekly usage limit exceeded"}"#),402);
        assert_eq!(
            classify(
                429,
                br#"{"code":"USAGE_LIMIT_EXCEEDED","message":"concurrent request limit exceeded"}"#
            ),
            429
        );
    }
}
