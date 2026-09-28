use crate::{config::Config, state::State};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock, Semaphore};
pub struct App {
    pub metrics: Arc<std::sync::Mutex<crate::telemetry::Metrics>>,
    pub inflight: std::sync::Mutex<HashSet<String>>,
    pub persist: Mutex<()>,
    pub config: RwLock<Arc<Config>>,
    pub state: Mutex<State>,
    pub client: reqwest::Client,
    pub system_client: std::sync::RwLock<reqwest::Client>,
    pub admin_requests: Arc<Semaphore>,
    pub checks: Arc<Semaphore>,
    pub login_slots: Arc<Semaphore>,
    pub login_gate: Mutex<(u32, i64)>,
    pub sessions: Mutex<HashMap<String, i64>>,
    pub data_dir: PathBuf,
    pub saves: Mutex<()>,
}

pub struct CheckGuard<'a> {
    app: &'a App,
    id: String,
}
impl Drop for CheckGuard<'_> {
    fn drop(&mut self) {
        self.app.inflight.lock().unwrap().remove(&self.id);
    }
}
impl App {
    /// Keep upstream failure evidence separately from the bounded state snapshot.
    /// Two rotating files, at most roughly 32 MiB total. No request body is copied.
    pub async fn record_upstream_error(&self, record: serde_json::Value) -> Result<(), String> {
        let _guard = self.persist.lock().await;
        let dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let path = dir.join("upstream-errors.jsonl");
            if std::fs::metadata(&path).is_ok_and(|m| m.len() >= 16 * 1024 * 1024) {
                std::fs::rename(&path, dir.join("upstream-errors.previous.jsonl"))
                    .map_err(|e| e.to_string())?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(path).map_err(|e| e.to_string())?;
            serde_json::to_writer(&mut file, &record).map_err(|e| e.to_string())?;
            file.write_all(b"\n").map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }
    pub fn upstream_client(&self, system_proxy: bool) -> reqwest::Client {
        if system_proxy {
            self.system_client.read().unwrap().clone()
        } else {
            self.client.clone()
        }
    }

    pub fn begin(&self, id: String) -> Option<CheckGuard<'_>> {
        if !self.inflight.lock().unwrap().insert(id.clone()) {
            return None;
        }
        Some(CheckGuard { app: self, id })
    }
    pub async fn persist(&self) -> Result<(), String> {
        let _guard = self.persist.lock().await;
        let mut snapshot = {
            let mut state = self.state.lock().await;
            state.prune_diagnostics(chrono::Utc::now().timestamp());
            state.clone()
        };
        snapshot.telemetry = self.metrics.lock().unwrap().snapshot();
        let path = self.data_dir.join("state.json");
        tokio::task::spawn_blocking(move || crate::storage::write_json(&path, &snapshot))
            .await
            .map_err(|_| "状态写入任务失败")?
    }
}
