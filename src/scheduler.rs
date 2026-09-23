use crate::{
    app::App,
    balance::Adapter,
    config::{Channel, Config, Key},
    health, monitor, upstream,
};
use futures_util::{StreamExt, stream};
use std::{sync::Arc, time::Duration};
pub fn spawn(app: Arc<App>) {
    let balances = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let cfg = balances.config.read().await.clone();
            let now = chrono::Utc::now().timestamp();
            let jobs = {
                let state = balances.state.lock().await;
                cfg.models
                    .iter()
                    .flat_map(|m| {
                        m.channels.iter().filter(|c| c.enabled).flat_map(|c| {
                            c.keys
                                .iter()
                                .filter(|k| k.enabled)
                                .filter(|k| {
                                    state
                                        .keys
                                        .get(&k.id)
                                        .is_none_or(|s| now - s.balance_checked >= 300)
                                })
                                .map(|k| (m.id.clone(), c.clone(), k.clone()))
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>()
            };
            stream::iter(jobs)
                .for_each_concurrent(4, |(model, c, k)| {
                    let app = balances.clone();
                    let cfg = cfg.clone();
                    async move {
                        balance_check(&app, &cfg, &model, &c, &k).await;
                    }
                })
                .await;
        }
    });
    let services = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let cfg = services.config.read().await.clone();
            let now = chrono::Utc::now().timestamp();
            let jobs = {
                let state = services.state.lock().await;
                cfg.models
                    .iter()
                    .flat_map(|m| {
                        m.channels.iter().filter(|c| c.enabled).flat_map(|c| {
                            c.keys
                                .iter()
                                .filter(|k| k.enabled)
                                .filter(|k| {
                                    state.keys.get(&k.id).is_some_and(|s| {
                                        (s.service_failed || s.suspect)
                                            && !s.probe_exhausted
                                            && s.retry_at <= now
                                            && !s.allowance.as_ref().is_some_and(|a| a.exhausted)
                                    })
                                })
                                .map(|k| (c.clone(), k.clone()))
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>()
            };
            stream::iter(jobs)
                .for_each_concurrent(2, |(c, k)| {
                    let app = services.clone();
                    let cfg = cfg.clone();
                    async move {
                        health::check(&app, &cfg, &c, &k, false, false).await;
                    }
                })
                .await;
        }
    });
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let cfg = app.config.read().await.clone();
            let now = chrono::Utc::now().timestamp();
            let monitors = {
                let state = app.state.lock().await;
                cfg.monitors
                    .iter()
                    .filter(|m| {
                        cfg.models.iter().any(|model| {
                            model.channels.iter().any(|c| {
                                c.enabled
                                    && c.keys.iter().any(|k| k.enabled)
                                    && c.monitor_id.as_ref() == Some(&m.id)
                            })
                        }) && state.quality.get(&m.id).is_none_or(|q| q.next_at <= now)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            monitor::batch(&app, &cfg, monitors).await;
        }
    });
}
pub async fn balance_check(
    app: &Arc<App>,
    cfg: &Arc<Config>,
    model: &str,
    c: &Channel,
    k: &Key,
) -> bool {
    let Some(_guard) = app.begin(format!("balance:{}", k.id)) else {
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
    let adapter = Adapter::for_model(model, c.adapter.clone());
    let mut result = upstream::allowance(
        &app.upstream_client(cfg.use_system_proxy),
        &c.base_url,
        &k.secret,
        adapter,
    )
    .await;
    let request_exhausted = app
        .state
        .lock()
        .await
        .keys
        .get(&k.id)
        .is_some_and(|s| s.request_exhausted);
    if request_exhausted && result.as_ref().is_ok_and(|(_, a)| a.unlimited) {
        // Unlimited token quota cannot prove that the shared account wallet was topped up.
        if !health::probe(app, c, k).await
            && let Ok((_, a)) = &mut result
        {
            a.exhausted = true;
        }
    }
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return false;
    }
    let mut state = app.state.lock().await;
    let s = state.keys.entry(k.id.clone()).or_default();
    if s.revision != revision {
        return false;
    }
    s.balance_checked = chrono::Utc::now().timestamp();
    s.revision += 1;
    match result {
        Ok((detected, allowance)) => {
            let old = s.allowance.as_ref().map(|a| a.exhausted);
            let exhausted = allowance.exhausted;
            s.request_exhausted = exhausted && s.request_exhausted;
            s.detected = Some(detected);
            s.allowance = Some(allowance);
            s.balance_error = None;
            // Balance access alone does not prove generation credentials/permissions recovered.
            if old.is_some_and(|o| o != exhausted) {
                let message = format!(
                    "{} / {}：额度{}",
                    c.name,
                    k.label,
                    if exhausted { "耗尽" } else { "恢复" }
                );
                state.event("balance", message.clone());
                if exhausted && !cfg.webhook.is_empty() {
                    state.notify(message);
                }
            }
        }
        Err(reason) => {
            let changed = s.balance_error.as_ref() != Some(&reason);
            s.balance_error = Some(reason.clone());
            if changed {
                state.event(
                    "check",
                    format!("{} / {}：额度查询异常：{}", c.name, k.label, reason),
                );
            }
        }
    }
    true
}
