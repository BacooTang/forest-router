use crate::app::App;
use axum::{Json, extract::State, response::Html};
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn index() -> Html<&'static str> {
    Html(include_str!("../static/monitor.html"))
}

pub async fn state(State(app): State<Arc<App>>) -> Json<Value> {
    // Keep the configuration and quality snapshot consistent with saves/checks.
    let cfg = app.config.read().await;
    let state = app.state.lock().await;
    let checks = app.inflight.lock().unwrap();
    let monitors: Vec<_> = cfg
        .monitors
        .iter()
        .map(|m| {
            // Public fields are explicitly selected. In particular, upstream
            // errors may contain private data and must never be copied here.
            let quality = state.quality.get(&m.id).map(|q| {
                json!({
                    "verdict": q.verdict,
                    "last_definite": q.last_definite,
                    "checked_at": q.checked_at,
                    "valid_until": q.valid_until,
                    "next_at": q.next_at,
                    "response_ms": q.response_ms,
                    "history": q.history,
                })
            });
            json!({
                "id": m.id,
                "name": m.name,
                "model": m.model,
                "reasoning_effort": m.reasoning_effort,
                "knowledge_check": m.knowledge_check,
                "automatically_scheduled": m.automatically_scheduled(&cfg),
                "quality": quality,
                "check": checks.get(&format!("monitor:{}", m.id)),
            })
        })
        .collect();
    Json(json!({
        "updated_at": chrono::Utc::now().timestamp(),
        "schedule": cfg.monitor_schedule,
        "monitors": monitors,
    }))
}
