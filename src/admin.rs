use crate::{app::App, config::Config, storage};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
};
use rand::RngCore;
use serde_json::{Value, json};
use std::sync::Arc;
pub async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}
pub async fn authorized(app: &App, h: &HeaderMap) -> bool {
    let token = h.get("cookie").and_then(|v| v.to_str().ok()).and_then(|v| {
        v.split(';')
            .find_map(|x| x.trim().strip_prefix("forest_session="))
    });
    let Some(token) = token else { return false };
    let now = chrono::Utc::now().timestamp();
    let mut sessions = app.sessions.lock().await;
    sessions.retain(|_, exp| *exp > now);
    sessions.contains_key(token)
}
pub async fn login(State(app): State<Arc<App>>, Json(v): Json<Value>) -> Response {
    let Ok(_guard) = app.login_slots.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    if app.login_gate.lock().await.1 > chrono::Utc::now().timestamp() {
        return (StatusCode::TOO_MANY_REQUESTS, "登录失败过多，请稍后重试").into_response();
    }
    let hash = app.config.read().await.admin_password_hash.clone();
    let password = v["password"].as_str().unwrap_or("").to_owned();
    if password.len() > 1024 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let hash_snapshot = hash.clone();
    let ok = tokio::task::spawn_blocking(move || {
        PasswordHash::new(&hash).is_ok_and(|h| {
            Argon2::default()
                .verify_password(password.as_bytes(), &h)
                .is_ok()
        })
    })
    .await
    .unwrap_or(false);
    if !ok {
        let mut gate = app.login_gate.lock().await;
        gate.0 = gate.0.saturating_add(1);
        if gate.0 >= 5 {
            gate.1 = chrono::Utc::now().timestamp() + (1i64 << (gate.0 - 5).min(6)).min(60);
        }
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let current = app.config.read().await;
    if current.admin_password_hash != hash_snapshot {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    *app.login_gate.lock().await = (0, 0);
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let mut sessions = app.sessions.lock().await;
    let now = chrono::Utc::now().timestamp();
    sessions.retain(|_, exp| *exp > now);
    if sessions.len() >= 16
        && let Some(k) = sessions
            .iter()
            .min_by_key(|(_, e)| **e)
            .map(|(k, _)| k.clone())
    {
        sessions.remove(&k);
    }
    sessions.insert(token.clone(), now + 12 * 3600);
    drop(current);
    (
        [(
            "set-cookie",
            format!("forest_session={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200"),
        )],
        Json(json!({"ok":true})),
    )
        .into_response()
}
pub async fn state(State(app): State<Arc<App>>, h: HeaderMap) -> Response {
    if !authorized(&app, &h).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let cfg = app.config.read().await.clone();
    let mut config = serde_json::to_value(&*cfg).unwrap();
    config
        .as_object_mut()
        .unwrap()
        .remove("admin_password_hash");
    config["_revision"] = json!(cfg.digest());
    let state = app.state.lock().await.clone();
    let usage = app.metrics.lock().unwrap().snapshot().client_usage;
    let routing: std::collections::HashMap<_, _> = cfg
        .models
        .iter()
        .map(|m| {
            let now = chrono::Utc::now().timestamp();
            let preferred = m
                .channels
                .iter()
                .find(|c| state.channel_ready(c, now))
                .map(|c| c.id.clone());
            (m.id.clone(), preferred)
        })
        .collect();
    (
        [("cache-control", "no-store")],
        Json(json!({"config":config,"state":state,"routing":routing,"traffic":app.metrics.lock().unwrap().view(),"client_usage":usage})),
    )
        .into_response()
}
pub async fn save(State(app): State<Arc<App>>, h: HeaderMap, Json(mut v): Json<Value>) -> Response {
    if !authorized(&app, &h).await || h.get("x-forest-admin").is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(_guard) =
        tokio::time::timeout(std::time::Duration::from_secs(5), app.saves.lock()).await
    else {
        return (StatusCode::CONFLICT, "另一个保存正在执行，请稍后重试").into_response();
    };
    let old = app.config.read().await.clone();
    if !v.is_object() {
        return (StatusCode::BAD_REQUEST, "配置必须为JSON对象").into_response();
    }
    let revision = v
        .as_object_mut()
        .and_then(|o| o.remove("_revision"))
        .and_then(|v| v.as_str().map(str::to_owned));
    if revision.as_deref() != Some(old.digest().as_str()) {
        return (
            StatusCode::CONFLICT,
            "配置已被修改或缺少版本号，请刷新后重新编辑",
        )
            .into_response();
    }
    if v.get("employee_keys").is_none() {
        v["employee_keys"] = json!(old.employee_keys);
    }
    v["admin_password_hash"] = json!(old.admin_password_hash);
    let password = v
        .as_object_mut()
        .and_then(|o| o.remove("new_password"))
        .and_then(|v| v.as_str().map(str::to_owned));
    let mut cfg: Config = match serde_json::from_value(v) {
        Ok(c) => c,
        Err(_) => return (StatusCode::BAD_REQUEST, "配置格式错误").into_response(),
    };
    if let Err(e) = cfg.validate() {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    if let Some(password) = password.filter(|p| !p.is_empty()) {
        if password.len() > 1024 {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let hash = tokio::task::spawn_blocking(move || {
            use argon2::{PasswordHasher, password_hash::SaltString};
            Argon2::default()
                .hash_password(
                    password.as_bytes(),
                    &SaltString::generate(&mut rand::rngs::OsRng),
                )
                .map(|s| s.to_string())
        })
        .await;
        match hash {
            Ok(Ok(hash)) => cfg.admin_password_hash = hash,
            _ => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    // Validate newly added or changed credentials server-side, not only in the page.
    let validation = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut verified = Vec::new();
        for model in &mut cfg.models {
            for channel in &mut model.channels {
                let mut actual = None;
                for key in &channel.keys {
                    let unchanged = old.models.iter().any(|m| {
                        m.channels.iter().any(|c| {
                            c.id == channel.id
                                && c.keys.iter().any(|k| {
                                    k.id == key.id
                                        && c.key_identity(&m.id, k)
                                            == channel.key_identity(&model.id, key)
                                })
                        })
                    });
                    if unchanged {
                        continue;
                    }
                    let Ok(_permit) = app.checks.clone().acquire_owned().await else {
                        return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
                    };
                    let kind = crate::balance::Adapter::for_model(
                        &model.id,
                        if channel.adapter == crate::balance::Adapter::Subscription {
                            crate::balance::Adapter::Subscription
                        } else {
                            crate::balance::Adapter::Auto
                        },
                    );
                    match crate::upstream::allowance(
                        &app.client,
                        &channel.base_url,
                        &key.secret,
                        kind,
                    )
                    .await
                    {
                        Ok((adapter, allowance)) => {
                            if actual.as_ref().is_some_and(|a| a != &adapter) {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    "同渠道的Key返回不同平台类型",
                                )
                                    .into_response());
                            }
                            actual = Some(adapter.clone());
                            verified.push((key.id.clone(), adapter, allowance));
                        }
                        Err(e) => {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                format!("{} / {} 验证失败：{}", channel.name, key.label, e),
                            )
                                .into_response());
                        }
                    }
                }
                if let Some(adapter) = actual {
                    channel.adapter = adapter;
                }
            }
        }
        Ok::<_, Response>(verified)
    })
    .await;
    let verified = match validation {
        Ok(Ok(v)) => v,
        Ok(Err(r)) => return r,
        Err(_) => {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                "配置验证超过60秒，未保存；请分批添加Key",
            )
                .into_response();
        }
    };
    if cfg.listen != old.listen {
        return (StatusCode::BAD_REQUEST, "监听地址需修改文件并重启服务").into_response();
    }
    let password_changed = old.admin_password_hash != cfg.admin_password_hash;
    let path = app.data_dir.join("config.json");
    let to_save = cfg.clone();
    let result = tokio::task::spawn_blocking(move || storage::write_json(&path, &to_save)).await;
    if !matches!(result, Ok(Ok(()))) {
        return (StatusCode::INTERNAL_SERVER_ERROR, "配置写盘失败").into_response();
    }
    {
        let mut current = app.config.write().await;
        let mut state = app.state.lock().await;
        if old.webhook != cfg.webhook {
            for n in &mut state.notices {
                n.attempts = 0;
                n.next_at = 0;
            }
        }
        state.reconcile(&cfg);
        if old.monitor_schedule != cfg.monitor_schedule {
            let now = chrono::Utc::now().timestamp();
            for q in state.quality.values_mut() {
                q.next_at = crate::monitor::next(now, &cfg.monitor_schedule);
            }
        }
        for (id, adapter, allowance) in verified {
            let entry = state.keys.entry(id).or_default();
            entry.detected = Some(adapter);
            entry.allowance = Some(allowance);
            entry.balance_checked = chrono::Utc::now().timestamp();
        }
        state.config_digest = cfg.digest();
        state.prune(&cfg);
        state.event("config", "配置已保存".into());
        *current = Arc::new(cfg);
    }
    if password_changed {
        app.sessions.lock().await.clear();
    }
    let _ = app.persist().await;
    Json(json!({"ok":true,"reauthenticate":password_changed,"revision":app.config.read().await.digest()})).into_response()
}

pub async fn verify(State(app): State<Arc<App>>, h: HeaderMap, Json(v): Json<Value>) -> Response {
    if !authorized(&app, &h).await || h.get("x-forest-admin").is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let cfg = app.config.read().await.clone();
    if v["all_monitors"].as_bool() == Some(true) {
        return match crate::monitor::batch(&app, &cfg, cfg.monitors.clone()).await {
            Some(completed) => {
                Json(json!({"ok":true,"completed":completed,"total":cfg.monitors.len()}))
                    .into_response()
            }
            None => (StatusCode::CONFLICT, "监控正在执行或配置已变化，请稍后重试").into_response(),
        };
    }
    let id = v["channel_id"].as_str().unwrap_or("");
    let reset = v["reset"].as_bool().unwrap_or(false);
    if let Some((model, c)) = cfg
        .models
        .iter()
        .find_map(|m| m.channels.iter().find(|c| c.id == id).map(|c| (m, c)))
    {
        let Some(_guard) = app.begin(format!("manual:{id}")) else {
            return (StatusCode::CONFLICT, "该渠道正在验证").into_response();
        };
        let mut complete = true;
        for k in &c.keys {
            complete &= crate::scheduler::balance_check(&app, &cfg, &model.id, c, k).await;
            complete &= crate::health::check(&app, &cfg, c, k, reset, true).await;
        }
        if let Some(m) = c
            .monitor_id
            .as_ref()
            .and_then(|id| cfg.monitors.iter().find(|m| m.id == *id))
        {
            complete &= crate::monitor::batch(&app, &cfg, vec![m.clone()]).await == Some(1);
        }
        {
            let current = app.config.read().await;
            complete &= Arc::ptr_eq(&current, &cfg);
        }
        let _ = app.persist().await;
        if !complete {
            return (
                StatusCode::CONFLICT,
                "部分检查正在执行或配置已变化；已完成部分保留，请稍后重试",
            )
                .into_response();
        }
        return Json(json!({"ok":true})).into_response();
    }
    if let Some(m) = cfg
        .monitors
        .iter()
        .find(|m| Some(m.id.as_str()) == v["monitor_id"].as_str())
    {
        if crate::monitor::batch(&app, &cfg, vec![m.clone()]).await != Some(1) {
            return (StatusCode::CONFLICT, "监控正在执行或配置已变化，请稍后重试").into_response();
        }
        let _ = app.persist().await;
        return Json(json!({"ok":true})).into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}
pub async fn detect(State(app): State<Arc<App>>, h: HeaderMap, Json(v): Json<Value>) -> Response {
    if !authorized(&app, &h).await || h.get("x-forest-admin").is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(_permit) = app.checks.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let base = v["base_url"].as_str().unwrap_or("");
    let key = v["key"].as_str().unwrap_or("");
    let model = v["model"].as_str().unwrap_or("");
    if base.len() > 2048 || key.len() > 4096 || key.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let configured = if v["subscription"].as_bool() == Some(true) {
        crate::balance::Adapter::Subscription
    } else {
        crate::balance::Adapter::Auto
    };
    match crate::upstream::allowance(
        &app.client,
        base,
        key,
        crate::balance::Adapter::for_model(model, configured),
    )
    .await
    {
        Ok((adapter, allowance)) => {
            Json(json!({"adapter":adapter,"allowance":allowance})).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({"error":e}))).into_response(),
    }
}

pub async fn limit(
    State(app): State<Arc<App>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Ok(_permit) = app.admin_requests.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        axum::http::HeaderValue::from_static("nosniff"),
    );
    response
}
