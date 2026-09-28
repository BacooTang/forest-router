use crate::{
    app::App,
    config::{self, Channel, Config, Key},
    upstream,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
const PROBE_BYTE_LIMIT: usize = 16 * 1024 * 1024;
const ERROR_CAPTURE_LIMIT: usize = 256 * 1024;
#[derive(Clone, Copy)]
struct ProbeLimits {
    headers: Duration,
    idle: Duration,
    total: Duration,
}
impl Default for ProbeLimits {
    fn default() -> Self {
        Self {
            headers: Duration::from_secs(150),
            idle: Duration::from_secs(90),
            total: Duration::from_secs(240),
        }
    }
}
/// Recovery requests use a tiny independent prompt, never replay company history.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Probe {
    Healthy,
    Failed,
    Inconclusive,
    Definite(u16),
    Throttled(i64),
}
struct ProbeReport {
    outcome: Probe,
    stage: &'static str,
    detail: String,
    http: Option<u16>,
    header_ms: Option<u128>,
    first_byte_ms: Option<u128>,
    total_ms: u128,
    bytes: usize,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    complete: bool,
}
impl Default for ProbeReport {
    fn default() -> Self {
        Self {
            outcome: Probe::Failed,
            stage: "awaiting_headers",
            detail: String::new(),
            http: None,
            header_ms: None,
            first_byte_ms: None,
            total_ms: 0,
            bytes: 0,
            headers: Vec::new(),
            body: Vec::new(),
            complete: false,
        }
    }
}
impl ProbeReport {
    fn event_message(&self, c: &Channel, k: &Key, manual: bool, status: &str) -> String {
        let source = if manual {
            "手动服务验证"
        } else {
            "自动服务探测"
        };
        let stage = match self.stage {
            "awaiting_headers" => "等待响应头",
            "sse" => "读取SSE流",
            "json" => "读取JSON响应",
            "http_error_body" => "读取上游错误正文",
            _ => self.stage,
        };
        let http = self
            .http
            .map(|s| format!("HTTP {s}"))
            .unwrap_or_else(|| "未收到HTTP响应".into());
        let cause = match self.detail.as_str() {
            "response_headers_deadline" => "等待响应头达到本地时限，本次无结论",
            "response_idle_deadline" => "连续无数据达到本地时限，本次无结论",
            "probe_total_deadline" => "探测总时长达到本地时限，本次无结论",
            "probe_byte_limit" | "json_or_error_body_limit" => "达到本地观察大小上限",
            "sse_missing_terminal" => "流已结束，但缺少完成事件",
            "sse_error_frame" => "上游返回流内错误",
            "upstream_http_error" => "上游明确报错",
            "invalid_response_or_error_envelope" => "响应结构异常或包含错误",
            "" => "完成",
            other => other,
        };
        let mut message = format!(
            "{} / {} · {source} · {http} · {:.2}秒\n{status}\n阶段：{stage}；{cause}",
            c.name,
            k.label,
            self.total_ms as f64 / 1000.0
        );
        if self.outcome != Probe::Healthy && !self.body.is_empty() {
            message.push_str(&format!(
                "\n上游原文摘要：{}",
                upstream::error_excerpt(&self.body)
            ));
        }
        message
    }
    fn capture(&mut self, chunk: &[u8]) {
        self.bytes += chunk.len();
        // Keep the tail: SSE error frames often follow a large progress frame.
        if chunk.len() >= ERROR_CAPTURE_LIMIT {
            self.body.clear();
            self.body
                .extend_from_slice(&chunk[chunk.len() - ERROR_CAPTURE_LIMIT..]);
        } else {
            let excess = (self.body.len() + chunk.len()).saturating_sub(ERROR_CAPTURE_LIMIT);
            self.body.drain(..excess);
            self.body.extend_from_slice(chunk);
        }
    }
}
pub async fn probe(app: &App, c: &Channel, k: &Key) -> bool {
    matches!(probe_result(app, c, k, false).await.outcome, Probe::Healthy)
}
async fn run_probe(
    client: &reqwest::Client,
    c: &Channel,
    k: &Key,
    limits: ProbeLimits,
) -> ProbeReport {
    use futures_util::StreamExt;
    let started = std::time::Instant::now();
    let mut report = ProbeReport::default();
    let result = tokio::time::timeout(limits.total, async {
        let send = upstream::control_request(client, reqwest::Method::POST, &config::responses_url(&c.base_url)).bearer_auth(&k.secret)
            .json(&json!({"model":c.upstream_model,"input":"Reply OK.","max_output_tokens":256,"reasoning":{"effort":"low"},"stream":true}))
            .send();
        let response = match tokio::time::timeout(limits.headers, send).await {
            Err(_) => { report.detail = "response_headers_deadline".into(); return Probe::Inconclusive; }
            Ok(Err(e)) => { report.detail = e.without_url().to_string(); return Probe::Failed; }
            Ok(Ok(r)) => r,
        };
        let status = response.status().as_u16();
        report.http = Some(status);
        report.header_ms = Some(started.elapsed().as_millis());
        report.headers = response.headers().iter().map(|(k,v)| (k.to_string(), String::from_utf8_lossy(v.as_bytes()).into_owned())).collect();
        let success = response.status().is_success();
        let sse = success && response.headers().get("content-type").and_then(|h|h.to_str().ok()).is_some_and(|h|h.starts_with("text/event-stream"));
        let delay = response.headers().get("retry-after").and_then(|h|h.to_str().ok()).and_then(|s|s.parse::<i64>().ok()).unwrap_or(300).clamp(60,3600);
        report.stage = if sse { "sse" } else if success { "json" } else { "http_error_body" };
        let mut source = response.bytes_stream();
        let mut observer = crate::sse::Observer::default();
        loop {
            let chunk = match tokio::time::timeout(limits.idle, source.next()).await {
                Err(_) => { report.detail = "response_idle_deadline".into(); return Probe::Inconclusive; }
                Ok(Some(Err(e))) => { report.detail = e.without_url().to_string(); return Probe::Failed; }
                Ok(Some(Ok(b))) => b,
                Ok(None) => { report.complete = true; break; }
            };
            report.first_byte_ms.get_or_insert(started.elapsed().as_millis());
            report.capture(&chunk);
            if report.bytes > PROBE_BYTE_LIMIT {
                report.detail = "probe_byte_limit".into(); return Probe::Inconclusive;
            }
            if sse {
                observer.feed(&chunk);
                if observer.failed {
                    report.detail = "sse_error_frame".into();
                    return match observer.failure_status { Some(code @ (401 | 402)) => Probe::Definite(code), Some(429) => Probe::Throttled(delay), _ => Probe::Failed };
                }
                if observer.terminal { return Probe::Healthy; }
            } else if report.bytes > ERROR_CAPTURE_LIMIT {
                report.detail = "json_or_error_body_limit".into(); return Probe::Inconclusive;
            }
        }
        if sse {
            observer.finish();
            report.detail = if observer.failed { "sse_error_frame" } else { "sse_missing_terminal" }.into();
            return match observer.failure_status {
                Some(code @ (401 | 402)) => Probe::Definite(code), Some(429) => Probe::Throttled(delay),
                _ if observer.terminal && !observer.failed => Probe::Healthy, _ => Probe::Failed,
            };
        }
        if success && serde_json::from_slice(&report.body).is_ok_and(|v| crate::errors::valid_response(&v)) { return Probe::Healthy; }
        report.detail = if success { "invalid_response_or_error_envelope" } else { "upstream_http_error" }.into();
        match crate::errors::classify(status, &report.body) {
            code @ (401 | 402) => Probe::Definite(code), 429 => Probe::Throttled(delay),
            _ if status >= 500 => Probe::Definite(status), _ => Probe::Failed,
        }
    }).await;
    report.outcome = match result {
        Ok(outcome) => outcome,
        Err(_) => {
            report.detail = "probe_total_deadline".into();
            Probe::Inconclusive
        }
    };
    // A slow/truncated error body does not erase an explicit HTTP failure.
    if report.outcome == Probe::Inconclusive {
        report.outcome = match report.http {
            Some(code @ (401 | 402 | 500..=599)) => Probe::Definite(code),
            Some(429) => Probe::Throttled(300),
            _ => Probe::Inconclusive,
        };
    }
    report.total_ms = started.elapsed().as_millis();
    report
}

async fn probe_result(app: &App, c: &Channel, k: &Key, manual: bool) -> ProbeReport {
    let cfg = app.config.read().await.clone();
    let report = run_probe(
        &app.upstream_client(cfg.use_system_proxy),
        c,
        k,
        ProbeLimits::default(),
    )
    .await;
    if cfg.diagnostics_enabled && report.outcome != Probe::Healthy {
        let record = json!({"at":chrono::Utc::now().to_rfc3339(), "source":"service_probe",
            "rid":format!("probe_{:016x}",rand::random::<u64>()), "channel_id":c.id,"key_id":k.id,"manual":manual,
            "http":report.http,"stage":report.stage,"detail":report.detail,"result":format!("{:?}",report.outcome),
            "header_ms":report.header_ms,"first_byte_ms":report.first_byte_ms,"total_ms":report.total_ms,
            "response_headers":report.headers,"body":String::from_utf8_lossy(&report.body),
            "body_bytes":report.bytes,"captured_bytes":report.body.len(),"body_offset":report.bytes-report.body.len(),
            "truncated":report.bytes>report.body.len(),"body_complete":report.complete});
        if app.record_upstream_error(record).await.is_err() {
            app.state.lock().await.event(
                "diagnostic_error",
                "服务探测原文写盘失败，请检查数据目录空间和权限".into(),
            );
        }
    }
    report
}

pub async fn check(
    app: &Arc<App>,
    cfg: &Arc<Config>,
    c: &Channel,
    k: &Key,
    reset: bool,
    manual: bool,
) -> bool {
    let Some(_guard) = app.begin(format!("key-check:{}", k.id)) else {
        return false;
    };
    let Ok(_permit) = app.checks.clone().acquire_owned().await else {
        return false;
    };
    let revision = app
        .state
        .lock()
        .await
        .keys
        .get(&k.id)
        .map_or(0, |s| s.revision);
    let started = std::time::Instant::now();
    let report = probe_result(app, c, k, manual).await;
    let outcome = report.outcome;
    let ok = matches!(outcome, Probe::Healthy);
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return false;
    }
    let now = chrono::Utc::now().timestamp();
    let mut state = app.state.lock().await;
    let category = match outcome {
        Probe::Healthy => "healthy",
        Probe::Failed => "failed",
        Probe::Inconclusive => "inconclusive",
        Probe::Definite(401) => "credential",
        Probe::Definite(402) => "quota",
        Probe::Definite(_) => "http_service_error",
        Probe::Throttled(_) => "throttled",
    };
    let stale = state.keys.get(&k.id).map_or(0, |s| s.revision) != revision;
    state.diagnostic(
        cfg.diagnostics_enabled,
        "service_probe",
        format!(
            "key={} result={} elapsed_ms={} manual={} stale={} stage={} http={:?} header_ms={:?} first_byte_ms={:?} bytes={}",
            k.id,
            category,
            started.elapsed().as_millis(),
            manual,
            stale, report.stage, report.http, report.header_ms, report.first_byte_ms, report.bytes
        ),
    );
    let s = state.keys.entry(k.id.clone()).or_default();
    if s.revision != revision {
        return false;
    }
    s.service_next_at = crate::monitor::next(now, &cfg.service_schedule);
    let exhausted_before = s.probe_exhausted;
    let failed_before = s.service_failed;
    let suspect_before = s.suspect;
    if matches!(outcome, Probe::Inconclusive) {
        s.probe_inconclusive(now, s.service_next_at);
        let event = report.event_message(c, k, manual, &s.service_event_status(now));
        state.event("check", event);
        state.diagnostic(
            cfg.diagnostics_enabled,
            "service_state",
            format!(
                "key={} inconclusive=true next_at={}",
                k.id,
                crate::monitor::next(now, &cfg.service_schedule)
            ),
        );
        return true;
    }
    if manual {
        s.probe_exhausted = false;
        s.probe_attempts = 0;
    }
    if reset {
        s.last_incident = None;
        s.cooldown_until = 0;
        s.failure_cycles.clear();
        s.recovery_successes = 1;
        s.last_recovery_success = now - 60;
    }
    if let Probe::Throttled(delay) = outcome {
        s.recovery_successes = 0;
        s.retry_at = now + delay;
        s.revision += 1;
        s.reason = "恢复检查遇到限流，延后检查，不消耗故障探测预算".into();
        let event = report.event_message(c, k, manual, &s.service_event_status(now));
        state.event("rate_limit", event);
        return true;
    }
    if manual && ok {
        // Manual verification is an explicit operator decision: unlock immediately.
        s.suspect = false;
        s.service_failed = false;
        s.cooldown_until = 0;
        s.probe_exhausted = false;
        s.probe_attempts = 0;
        s.probe_step = 0;
        s.recovery_successes = 0;
        s.retry_at = 0;
        s.reason.clear();
        s.checked = true;
        s.credential_failed = false;
        s.revision += 1;
    }
    let new_failure = if ok {
        if s.service_failed {
            s.probe_result(now, true);
        } else {
            s.suspect = false;
            s.retry_at = 0;
            s.reason.clear();
            s.revision += 1;
        }
        s.checked = true;
        s.credential_failed = false;
        false
    } else if let Probe::Definite(code @ (401 | 402)) = outcome {
        s.suspect = false;
        s.revision += 1;
        if code == 401 {
            s.credential_failed = true;
            s.reason = "凭证失效".into();
        } else {
            s.request_exhausted = true;
            s.balance_checked = now;
            s.allowance.get_or_insert_with(Default::default).exhausted = true;
            s.reason = "额度耗尽".into();
        }
        false
    } else if s.suspect {
        s.reason = "网络异常确认中，继续可用；持续至少60秒且多次验证失败后隔离".into();
        // Legacy persisted incidents may not have a start timestamp.
        s.last_incident.get_or_insert(now);
        let failed = s.confirmation_failed(now);
        if failed {
            s.reason = "网络异常持续至少60秒，且多次最小 Responses 验证失败".into();
        }
        failed
    } else if !s.service_failed {
        // A routine probe failure alone must not evict a working channel.
        s.reason = "服务探测异常，自动确认中".into();
        s.suspect_service(now)
    } else {
        s.probe_result(now, false);
        false
    };
    let exhausted = s.probe_exhausted;
    if s.service_failed
        && !ok
        && !s.credential_failed
        && !s.allowance.as_ref().is_some_and(|a| a.unavailable(now))
    {
        s.retry_at = if s.probe_exhausted {
            s.service_next_at
        } else {
            s.retry_at.min(s.service_next_at)
        };
    }
    let recovered = ok && (failed_before || suspect_before) && !s.service_failed && !s.suspect;
    let event = report.event_message(c, k, manual, &s.service_event_status(now));
    let detail = format!(
        "key={} failed={} suspect={} attempts={} successes={} retry_at={} routine_at={}",
        k.id,
        s.service_failed,
        s.suspect,
        s.probe_attempts,
        s.recovery_successes,
        s.retry_at,
        s.service_next_at
    );
    state.diagnostic(cfg.diagnostics_enabled, "service_state", detail);
    if exhausted && !exhausted_before {
        state.event(
            "service",
            format!(
                "{} / {}：快速恢复探测达到12次，继续按服务时间段自动复查",
                c.name, k.label
            ),
        );
    }
    if !ok || manual || recovered || failed_before {
        state.event(
            "service",
            if recovered {
                format!("服务恢复 · {event}")
            } else {
                event
            },
        );
    }
    if new_failure && !cfg.webhook.is_empty() {
        state.notify(format!(
            "服务中断：{} / {}，将自动使用后续可用渠道。",
            c.name, k.label
        ));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_explains_real_http_cause_and_local_deadline() {
        let c: Channel = serde_json::from_value(json!({"id":"c","name":"渠道","base_url":"http://localhost","upstream_model":"test","adapter":"sub2_api","enabled":true,"keys":[]})).unwrap();
        let k = Key {
            id: "k".into(),
            label: "主Key".into(),
            secret: "not-for-events".into(),
            enabled: true,
        };
        let mut r = ProbeReport {
            outcome: Probe::Definite(503),
            http: Some(503),
            total_ms: 285,
            stage: "http_error_body",
            detail: "upstream_http_error".into(),
            body: br#"{"error":{"message":"Service temporarily unavailable"}}"#.to_vec(),
            ..Default::default()
        };
        let event = r.event_message(&c, &k, false, "确认中，暂不隔离");
        for text in [
            "HTTP 503",
            "0.28秒",
            "Service temporarily unavailable",
            "自动服务探测",
            "暂不隔离",
        ] {
            assert!(event.contains(text), "{event}");
        }
        assert!(!event.contains(&k.secret));
        r.outcome = Probe::Inconclusive;
        r.http = None;
        r.body.clear();
        r.detail = "response_headers_deadline".into();
        assert!(
            r.event_message(&c, &k, false, "保留状态")
                .contains("本次无结论")
        );
    }
    use axum::{Router, body::Body, response::Response, routing::post};

    async fn fixture(
        status: u16,
        head_delay: Duration,
        chunks: Vec<(Duration, String)>,
        limits: ProbeLimits,
    ) -> ProbeReport {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new().route(
            "/v1/responses",
            post(move || {
                let chunks = chunks.clone();
                async move {
                    tokio::time::sleep(head_delay).await;
                    let stream = async_stream::stream! {
                        for (delay, text) in chunks {
                            tokio::time::sleep(delay).await;
                            yield Ok::<_,std::io::Error>(text);
                        }
                    };
                    Response::builder()
                        .status(status)
                        .header(
                            "content-type",
                            if status == 200 {
                                "text/event-stream"
                            } else {
                                "application/json"
                            },
                        )
                        .header("x-request-id", "fixture-upstream-id")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let channel: Channel = serde_json::from_value(json!({"id":"test", "name":"test", "base_url":format!("http://{address}/v1"), "upstream_model":"test", "adapter":"sub2_api", "enabled":true,"keys":[]})).unwrap();
        let key = Key {
            id: "key".into(),
            label: "test".into(),
            secret: "test-only".into(),
            enabled: true,
        };
        let result = run_probe(&upstream::client(false).unwrap(), &channel, &key, limits).await;
        server.abort();
        result
    }
    fn quick() -> ProbeLimits {
        ProbeLimits {
            headers: Duration::from_millis(100),
            idle: Duration::from_millis(100),
            total: Duration::from_millis(300),
        }
    }
    fn completed() -> String {
        "data: {\"type\":\"response.completed\"}\n\n".into()
    }

    #[tokio::test]
    async fn response_slower_than_old_45_second_cutoff_is_healthy() {
        let r = fixture(
            200,
            Duration::from_secs(46),
            vec![(Duration::ZERO, completed())],
            ProbeLimits::default(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Healthy);
        assert!(r.header_ms.unwrap() >= 45000);
    }
    #[tokio::test]
    async fn header_and_idle_deadlines_are_distinct_and_inconclusive() {
        let r = fixture(200, Duration::from_millis(250), vec![], quick()).await;
        assert_eq!(r.outcome, Probe::Inconclusive);
        assert_eq!(r.detail, "response_headers_deadline");
        assert_eq!(r.http, None);
        let r = fixture(
            200,
            Duration::ZERO,
            vec![
                (
                    Duration::ZERO,
                    "data: {\"type\":\"response.created\"}\n\n".into(),
                ),
                (Duration::from_millis(250), completed()),
            ],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Inconclusive);
        assert_eq!(r.detail, "response_idle_deadline");
        assert!(r.first_byte_ms.is_some());
    }
    #[tokio::test]
    async fn heartbeat_stream_still_has_a_total_deadline() {
        let r = fixture(
            200,
            Duration::ZERO,
            vec![(Duration::from_millis(30), ": ping\n\n".into()); 30],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Inconclusive);
        assert_eq!(r.detail, "probe_total_deadline");
    }
    #[tokio::test]
    async fn http_and_sse_failures_keep_exact_evidence() {
        let body = r#"{"error":{"code":"upstream_ws_incomplete","message":"WebSocket closed before completion"}}"#;
        let r = fixture(
            502,
            Duration::ZERO,
            vec![(Duration::ZERO, body.into())],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Definite(502));
        assert_eq!(r.body, body.as_bytes());
        assert!(r.complete);
        assert!(
            r.headers
                .contains(&("x-request-id".into(), "fixture-upstream-id".into()))
        );
        let frame = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"insufficient_quota\"}}}\n\n";
        let r = fixture(
            200,
            Duration::ZERO,
            vec![(Duration::ZERO, frame.into())],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Definite(402));
        assert_eq!(r.body, frame.as_bytes());
        let r = fixture(
            401,
            Duration::ZERO,
            vec![(Duration::from_millis(250), body.into())],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Definite(401));
    }
    #[tokio::test]
    async fn unfinished_stream_is_not_a_success() {
        let r = fixture(
            200,
            Duration::ZERO,
            vec![(
                Duration::ZERO,
                "data: {\"type\":\"response.created\"}\n\n".into(),
            )],
            quick(),
        )
        .await;
        assert_eq!(r.outcome, Probe::Failed);
        assert_eq!(r.detail, "sse_missing_terminal");
    }
    #[test]
    fn diagnostic_tail_is_bounded_and_keeps_late_error() {
        let mut r = ProbeReport::default();
        r.capture(&vec![b'x'; ERROR_CAPTURE_LIMIT * 2]);
        r.capture(b"late error");
        assert_eq!(r.body.len(), ERROR_CAPTURE_LIMIT);
        assert!(r.body.ends_with(b"late error"));
        assert_eq!(r.bytes, ERROR_CAPTURE_LIMIT * 2 + 10);
    }
}
