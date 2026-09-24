use crate::{
    app::App,
    config::{self, Channel, Config, Key},
    upstream,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
/// Recovery requests use a tiny independent prompt, never replay company history.
#[derive(Clone, Copy)]
pub enum Probe {
    Healthy,
    Failed,
    Definite(u16),
    Throttled(i64),
}
pub async fn probe(app: &App, c: &Channel, k: &Key) -> bool {
    matches!(probe_result(app, c, k).await, Probe::Healthy)
}
async fn probe_result(app: &App, c: &Channel, k: &Key) -> Probe {
    use futures_util::StreamExt;
    let use_system_proxy = app.config.read().await.use_system_proxy;
    let result=tokio::time::timeout(Duration::from_secs(45),async {
        let response=app.upstream_client(use_system_proxy).post(config::responses_url(&c.base_url)).bearer_auth(&k.secret)
            .json(&json!({"model":c.upstream_model,"input":"Reply OK.","max_output_tokens":256,"reasoning":{"effort":"low"},"stream":true}))
            .send().await.ok()?;
        let status = response.status().as_u16();
        if !response.status().is_success(){
            let delay=response.headers().get("retry-after").and_then(|h|h.to_str().ok()).and_then(|s|s.parse::<i64>().ok()).unwrap_or(300).clamp(60,3600);
            let body = upstream::bounded_json(response).await.ok();
            let class = crate::errors::classify(status, &body.and_then(|v| serde_json::to_vec(&v).ok()).unwrap_or_default());
            return Some(if matches!(class, 401 | 402) { Probe::Definite(class) } else if class == 429 { Probe::Throttled(delay) } else if status >= 500 { Probe::Definite(status) } else { Probe::Failed });
        }
        if response.headers().get("content-type").and_then(|h|h.to_str().ok()).is_some_and(|h|h.starts_with("text/event-stream")) {
            let mut source=response.bytes_stream();let mut observer=crate::sse::Observer::default();let mut bytes=0;
            while let Some(chunk)=source.next().await {
                let Ok(chunk)=chunk else {return Some(Probe::Failed);};
                bytes+=chunk.len();if bytes>262144{return Some(Probe::Failed);}
                observer.feed(&chunk);if observer.failed{return Some(match observer.failure_status {Some(code @ (401 | 402)) => Probe::Definite(code), Some(429) => Probe::Throttled(60), _ => Probe::Failed});}
                if observer.terminal{return Some(Probe::Healthy);}
            }
            observer.finish();
            Some(if observer.terminal && !observer.failed {Probe::Healthy}else{Probe::Failed})
        } else {
            let v=upstream::bounded_json(response).await.ok()?;
            if !crate::errors::valid_response(&v) {
                let class = crate::errors::classify(200, &serde_json::to_vec(&v).ok()?);
                return Some(match class {401 | 402 => Probe::Definite(class), 429 => Probe::Throttled(60), _ => Probe::Failed});
            }
            Some(if crate::errors::valid_response(&v){Probe::Healthy}else{Probe::Failed})
        }
    }).await;
    result.ok().flatten().unwrap_or(Probe::Failed)
}

pub async fn check(
    app: &Arc<App>,
    cfg: &Arc<Config>,
    c: &Channel,
    k: &Key,
    reset: bool,
    manual: bool,
) -> bool {
    let Some(_guard) = app.begin(format!("service:{}", k.id)) else {
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
    let outcome = probe_result(app, c, k).await;
    let ok = matches!(outcome, Probe::Healthy);
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return false;
    }
    let now = chrono::Utc::now().timestamp();
    let mut state = app.state.lock().await;
    let s = state.keys.entry(k.id.clone()).or_default();
    if s.revision != revision {
        return false;
    }
    let failed_before = s.service_failed;
    let suspect_before = s.suspect;
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
        s.retry_at = now + delay;
        s.revision += 1;
        s.reason = "恢复检查遇到限流，延后检查，不消耗故障探测预算".into();
        if manual {
            state.event(
                "check",
                format!("{} / {}：上游限流，请稍后验证", c.name, k.label),
            );
        }
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
    } else if s.suspect && !matches!(outcome, Probe::Definite(_)) {
        s.reason = "网络异常确认中，继续可用；持续至少60秒且多次验证失败后隔离".into();
        // Legacy persisted incidents may not have a start timestamp.
        s.last_incident.get_or_insert(now);
        let failed = s.confirmation_failed(now);
        if failed {
            s.reason = "网络异常持续至少60秒，且多次最小 Responses 验证失败".into();
        }
        failed
    } else if !s.service_failed {
        s.reason = "最小 Responses 验证失败".into();
        s.fail_service(now)
    } else {
        s.probe_result(now, false);
        false
    };
    let exhausted = s.probe_exhausted;
    let recovered = ok && (failed_before || suspect_before) && !s.service_failed && !s.suspect;
    if exhausted {
        state.event(
            "service",
            format!(
                "{} / {}：恢复探测达到12次上限，已停止自动探测",
                c.name, k.label
            ),
        );
    }
    if recovered {
        state.event("service", format!("{} / {}：服务恢复", c.name, k.label));
    }
    if new_failure {
        state.event("service", format!("{} / {}：服务验证失败", c.name, k.label));
        if !cfg.webhook.is_empty() {
            state.notify(format!(
                "服务中断：{} / {}，将自动使用后续可用渠道。",
                c.name, k.label
            ));
        }
    }
    if manual && !new_failure && !recovered {
        state.event(
            "check",
            format!(
                "{} / {}：重新验证{}",
                c.name,
                k.label,
                if ok { "成功" } else { "失败" }
            ),
        );
    }
    true
}
