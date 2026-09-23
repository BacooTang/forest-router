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
    pub requests: Arc<Semaphore>,
    pub request_bytes: Arc<Semaphore>,
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
    pub fn begin(&self, id: String) -> Option<CheckGuard<'_>> {
        if !self.inflight.lock().unwrap().insert(id.clone()) {
            return None;
        }
        Some(CheckGuard { app: self, id })
    }
    pub async fn persist(&self) -> Result<(), String> {
        let _guard = self.persist.lock().await;
        let mut snapshot = self.state.lock().await.clone();
        snapshot.telemetry = self.metrics.lock().unwrap().snapshot();
        let path = self.data_dir.join("state.json");
        tokio::task::spawn_blocking(move || crate::storage::write_json(&path, &snapshot))
            .await
            .map_err(|_| "状态写入任务失败")?
    }
}
