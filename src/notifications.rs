use crate::{app::App, upstream};
use serde_json::json;
use std::{sync::Arc, time::Duration};
pub fn spawn(app: Arc<App>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let cfg = app.config.read().await.clone();
            if cfg.webhook.is_empty() {
                continue;
            }
            let notice = app
                .state
                .lock()
                .await
                .notices
                .iter()
                .find(|n| n.attempts < 12 && n.next_at <= chrono::Utc::now().timestamp())
                .cloned();
            let Some(n) = notice else { continue };
            // Persist pending message before sending so a restart does not drop queued alerts.
            if app.persist().await.is_err() {
                continue;
            }
            let ok = async {
                let r = app
                    .client
                    .post(&cfg.webhook)
                    .json(&match &n.card {
                        Some(card) => json!({"msg_type":"interactive","card":card}),
                        None => json!({"msg_type":"text","content":{"text":n.message}}),
                    })
                    .timeout(Duration::from_secs(15))
                    .send()
                    .await
                    .ok()?;
                let v = upstream::bounded_json(r).await.ok()?;
                Some(v["code"].as_i64() == Some(0) || v["StatusCode"].as_i64() == Some(0))
            }
            .await
                == Some(true);
            {
                let mut state = app.state.lock().await;
                if ok {
                    state.notices.retain(|v| v.id != n.id);
                } else if let Some(item) = state.notices.iter_mut().find(|v| v.id == n.id) {
                    item.attempts = item.attempts.saturating_add(1);
                    item.next_at = chrono::Utc::now().timestamp()
                        + (30i64 * (1i64 << item.attempts.min(6))).min(1800);
                    if item.attempts >= 12 {
                        state.event(
                            "notification",
                            "飞书通知达到12次发送上限，停止自动投递；检查Webhook后重新配置以重试"
                                .into(),
                        );
                    } else if item.attempts == 1 {
                        state.event("notification", "飞书通知发送失败，将在后台重试".into());
                    }
                }
            }
            let _ = app.persist().await;
        }
    });
}
