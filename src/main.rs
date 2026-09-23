mod admin;
mod app;
mod balance;
mod config;
mod errors;
mod health;
mod monitor;
mod notifications;
mod proxy;
mod scheduler;
mod sse;
mod state;
mod storage;
mod telemetry;
mod upstream;

use app::App;
use axum::{
    Router,
    routing::{get, post},
};
use config::Config;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock, Semaphore};

fn parse_port(args: impl IntoIterator<Item = String>) -> Result<Option<u16>, String> {
    let mut args = args.into_iter();
    let mut port = None;
    while let Some(arg) = args.next() {
        let value = if arg == "--port" {
            args.next().ok_or("--port 缺少端口值")?
        } else if let Some(value) = arg.strip_prefix("--port=") {
            value.to_owned()
        } else {
            return Err(format!(
                "未知参数：{arg}；用法：forest-router [--port 8119]"
            ));
        };
        if port.is_some() {
            return Err("--port 不能重复指定".into());
        }
        port = Some(
            value
                .parse::<u16>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or("端口必须为1–65535的整数")?,
        );
    }
    Ok(port)
}

#[tokio::main(worker_threads = 4)]
async fn main() {
    let port = match parse_port(std::env::args().skip(1)) {
        Ok(port) => port,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let data_dir = std::env::var_os("FOREST_ROUTER_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".runtime"));
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let cfg_path = data_dir.join("config.json");
    let mut cfg = if cfg_path.exists() {
        let loaded = (|| -> Result<Config, String> {
            let metadata = std::fs::metadata(&cfg_path).map_err(|e| e.to_string())?;
            if metadata.len() > 1024 * 1024 {
                return Err("配置超过1MiB".into());
            }
            let raw = std::fs::read(&cfg_path).map_err(|e| e.to_string())?;
            serde_json::from_slice::<Config>(&raw).map_err(|_| "配置JSON或结构无效".into())
        })();
        match loaded {
            Ok(c) => c,
            Err(e) => {
                eprintln!("无法读取配置：{e}；请修复config.json，原文件未修改");
                std::process::exit(1)
            }
        }
    } else {
        use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
        let password = std::env::var("FOREST_ADMIN_PASSWORD")
            .expect("首次启动请设置 FOREST_ADMIN_PASSWORD（创建配置后可移除）");
        let api_key = std::env::var("FOREST_API_KEY").expect("首次启动请设置 FOREST_API_KEY");
        let salt = SaltString::generate(&mut rand::rngs::OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .expect("hash password")
            .to_string();
        let c = Config {
            listen: std::env::var("FOREST_LISTEN").unwrap_or_else(|_| "0.0.0.0:8119".into()),
            api_key,
            admin_password_hash: hash,
            webhook: String::new(),
            notify_all_monitors: false,
            models: vec![],
            monitors: vec![],
            monitor_schedule: config::default_monitor_schedule(),
        };
        c.validate().expect("invalid bootstrap configuration");
        storage::write_json(&cfg_path, &c).expect("write initial configuration");
        c
    };
    if let Some(port) = port {
        cfg.listen = format!("0.0.0.0:{port}");
    }
    cfg.validate().expect("invalid config");
    let state_path = data_dir.join("state.json");
    let mut state = match storage::read_state(&state_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("无法安全读取运行状态：{e}；请修复/恢复state.json，原文件未修改");
            std::process::exit(1)
        }
    };
    state.reconcile(&cfg);
    let app = Arc::new(App {
        metrics: Arc::new(std::sync::Mutex::new(std::mem::take(&mut state.telemetry))),
        inflight: std::sync::Mutex::new(Default::default()),
        persist: Mutex::new(()),
        config: RwLock::new(Arc::new(cfg.clone())),
        state: Mutex::new(state),
        client: upstream::client().expect("http client"),
        requests: Arc::new(Semaphore::new(128)),
        request_bytes: Arc::new(Semaphore::new(65536)),
        admin_requests: Arc::new(Semaphore::new(8)),
        checks: Arc::new(Semaphore::new(4)),
        login_slots: Arc::new(Semaphore::new(1)),
        login_gate: Mutex::new((0, 0)),
        sessions: Mutex::new(std::collections::HashMap::new()),
        data_dir: data_dir.clone(),
        saves: Mutex::new(()),
    });
    scheduler::spawn(app.clone());
    notifications::spawn(app.clone());
    let persist = app.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            ticker.tick().await;
            if persist.persist().await.is_err() {
                eprintln!("state persistence failed");
            }
        }
    });
    let app_shutdown = app.clone();
    let admin_router = Router::new()
        .route("/", get(admin::index))
        .route("/admin/api/login", post(admin::login))
        .route("/admin/api/state", get(admin::state))
        .route("/admin/api/save", post(admin::save))
        .route("/admin/api/verify", post(admin::verify))
        .route("/admin/api/detect", post(admin::detect))
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            admin::limit,
        ));
    let router = Router::new()
        .merge(admin_router)
        .route("/v1/responses", post(proxy::responses))
        .route("/v1/models", get(proxy::models))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .expect("bind");
    eprintln!("forest-router listening on {}", cfg.listen);
    let shutdown = app_shutdown.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("signal handler");
                tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            let _ = shutdown.persist().await;
        })
        .await
        .expect("server");
    let _ = app_shutdown.persist().await;
}

#[cfg(test)]
mod cli_tests {
    use super::parse_port;
    #[test]
    fn port_arguments() {
        let parse = |args: &[&str]| parse_port(args.iter().map(|s| s.to_string()));
        assert_eq!(parse(&[]).unwrap(), None);
        assert_eq!(parse(&["--port", "80"]).unwrap(), Some(80));
        assert_eq!(parse(&["--port=8119"]).unwrap(), Some(8119));
        for args in [
            vec!["--port"],
            vec!["--port", "0"],
            vec!["--port=65536"],
            vec!["--port=abc"],
            vec!["--port=80", "--port=81"],
            vec!["--unknown"],
        ] {
            assert!(parse(&args).is_err());
        }
    }
}
