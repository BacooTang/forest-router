//! Bounded metadata only: no headers, credentials, upstream URLs or request/answer bodies.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Counts {
    pub requests: u64,
    pub success: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub unknown: u64,
    pub switches: u64,
    pub key_switches: u64,
    pub provider_switches: u64,
    pub route_switches: u64,
    pub returns: u64,
    pub first_ms_total: u64,
    pub first_samples: u64,
}
impl Counts {
    fn add(&mut self, other: &Self) {
        self.requests += other.requests;
        self.success += other.success;
        self.failed += other.failed;
        self.cancelled += other.cancelled;
        self.unknown += other.unknown;
        self.switches += other.switches;
        self.first_ms_total += other.first_ms_total;
        self.first_samples += other.first_samples;
    }
    fn result(&mut self, outcome: &str, first: Option<u64>) {
        match outcome {
            "success" => self.success += 1,
            "failed" => self.failed += 1,
            "cancelled" => self.cancelled += 1,
            _ => self.unknown += 1,
        }
        if let Some(ms) = first {
            self.first_ms_total += ms;
            self.first_samples += 1;
        }
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Bucket {
    pub minute: i64,
    pub channels: BTreeMap<String, Provider>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Provider {
    pub name: String,
    #[serde(flatten)]
    pub counts: Counts,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Metrics {
    pub day: i64,
    pub today: Counts,
    pub hours: VecDeque<Bucket>,
    pub failures: VecDeque<Report>,
    #[serde(skip)]
    pub active: u64,
}
impl Metrics {
    fn roll(&mut self, now: i64) {
        let day = (now + 8 * 3600).div_euclid(86400);
        if self.day != day {
            self.day = day;
            self.today = Counts::default();
        }
        let minute = now.div_euclid(60);
        self.hours
            .retain(|b| b.minute > minute - 60 && b.minute <= minute);
        self.hours.truncate(60);
        self.failures.truncate(200);
    }
    pub fn route_switch(&mut self, returning: bool) {
        self.roll(chrono::Utc::now().timestamp());
        self.today.route_switches += 1;
        self.today.returns += u64::from(returning);
    }
    pub fn snapshot(&mut self) -> Self {
        self.roll(chrono::Utc::now().timestamp());
        self.clone()
    }
    pub fn view(&mut self) -> serde_json::Value {
        let snapshot = self.snapshot();
        let mut providers: BTreeMap<String, Provider> = BTreeMap::new();
        for bucket in &snapshot.hours {
            for (id, p) in &bucket.channels {
                let total = providers.entry(id.clone()).or_default();
                total.name = p.name.clone();
                total.counts.add(&p.counts);
            }
        }
        serde_json::json!({"active":snapshot.active,"today":snapshot.today,"providers":providers,"failures":snapshot.failures})
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Attempt {
    pub channel_id: String,
    pub channel: String,
    pub key_id: String,
    pub status: Option<u16>,
    pub reason: String,
    pub elapsed_ms: u64,
    pub first_ms: Option<u64>,
    pub outcome: String,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Report {
    pub id: String,
    pub at: i64,
    pub model: String,
    pub status: u16,
    pub outcome: String,
    pub reason: String,
    pub elapsed_ms: u64,
    pub first_ms: Option<u64>,
    pub output_started: bool,
    pub attempts: Vec<Attempt>,
}
struct Progress {
    report: Report,
    start: Instant,
    attempt_start: Instant,
    counted: bool,
}
#[derive(Clone)]
pub struct Trace(Arc<Mutex<Progress>>);
pub struct Guard {
    trace: Trace,
    metrics: Arc<Mutex<Metrics>>,
    finished: bool,
}
impl Trace {
    pub fn new(metrics: Arc<Mutex<Metrics>>) -> (Self, Guard) {
        let now = Instant::now();
        let trace = Self(Arc::new(Mutex::new(Progress {
            report: Report {
                id: format!("fr_{:032x}", rand::random::<u128>()),
                at: chrono::Utc::now().timestamp(),
                ..Default::default()
            },
            start: now,
            attempt_start: now,
            counted: false,
        })));
        let guard = Guard {
            trace: trace.clone(),
            metrics,
            finished: false,
        };
        (trace, guard)
    }
    pub fn id(&self) -> String {
        self.0.lock().unwrap().report.id.clone()
    }
    pub fn begin(&self, metrics: &Mutex<Metrics>) {
        self.0.lock().unwrap().counted = true;
        let mut m = metrics.lock().unwrap();
        m.roll(chrono::Utc::now().timestamp());
        m.active += 1;
        m.today.requests += 1;
    }
    pub fn model(&self, model: &str) {
        self.0.lock().unwrap().report.model = model.chars().take(256).collect();
    }
    pub fn attempt(&self, channel: &crate::config::Channel, key: &crate::config::Key) {
        let mut p = self.0.lock().unwrap();
        p.attempt_start = Instant::now();
        p.report.attempts.push(Attempt {
            channel_id: channel.id.clone(),
            channel: channel.name.clone(),
            key_id: key.id.clone(),
            ..Default::default()
        });
    }
    pub fn status(&self, status: u16) {
        let mut p = self.0.lock().unwrap();
        if let Some(a) = p.report.attempts.last_mut() {
            a.status = Some(status);
        }
    }
    pub fn first(&self) {
        let mut p = self.0.lock().unwrap();
        let ms = p.start.elapsed().as_millis() as u64;
        let attempt_ms = p.attempt_start.elapsed().as_millis() as u64;
        p.report.first_ms.get_or_insert(ms);
        if let Some(a) = p.report.attempts.last_mut() {
            a.first_ms.get_or_insert(attempt_ms);
        }
    }
    pub fn result(&self, outcome: &str, reason: &str) {
        let mut p = self.0.lock().unwrap();
        p.report.outcome = outcome.into();
        p.report.reason = reason.into();
        let elapsed = p.attempt_start.elapsed().as_millis() as u64;
        if let Some(a) = p.report.attempts.last_mut()
            && a.outcome.is_empty()
        {
            a.outcome = outcome.into();
            a.reason = reason.into();
            a.elapsed_ms = elapsed;
        }
    }
    pub fn retry(&self, reason: &str) {
        self.result("failed", reason);
        let mut p = self.0.lock().unwrap();
        p.report.outcome.clear();
        p.report.reason.clear();
        p.report.first_ms = None;
    }
    pub fn response(&self, status: u16, reason: Option<&str>) {
        self.0.lock().unwrap().report.status = status;
        if status >= 400 {
            self.result("failed", reason.unwrap_or("upstream_input_error"));
        }
    }
    pub fn output(&self) {
        self.0.lock().unwrap().report.output_started = true;
    }
}
impl Guard {
    pub fn finish(&mut self, cancelled: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut p = self.trace.0.lock().unwrap();
        if !p.counted {
            return;
        }
        if cancelled || p.report.outcome.is_empty() {
            p.report.outcome = if cancelled { "cancelled" } else { "unknown" }.into();
            p.report.reason = if cancelled {
                "client_cancelled"
            } else {
                "unrecognized_terminal"
            }
            .into();
            let elapsed = p.attempt_start.elapsed().as_millis() as u64;
            let outcome = p.report.outcome.clone();
            let reason = p.report.reason.clone();
            if let Some(a) = p.report.attempts.last_mut()
                && a.outcome.is_empty()
            {
                a.outcome = outcome;
                a.reason = reason;
                a.elapsed_ms = elapsed;
            }
        }
        p.report.elapsed_ms = p.start.elapsed().as_millis() as u64;
        let r = &p.report;
        let now = chrono::Utc::now().timestamp();
        let mut m = self.metrics.lock().unwrap();
        m.roll(now);
        m.active = m.active.saturating_sub(1);
        // Daily outcomes belong to request start day, so yesterday's long streams do not skew today's rate.
        if (r.at + 8 * 3600).div_euclid(86400) == m.day {
            m.today.result(&r.outcome, r.first_ms);
            m.today.switches += r.attempts.len().saturating_sub(1) as u64;
            for pair in r.attempts.windows(2) {
                if pair[0].channel_id == pair[1].channel_id {
                    m.today.key_switches += 1;
                } else {
                    m.today.provider_switches += 1;
                }
            }
        }
        if m.hours
            .back()
            .is_none_or(|b| b.minute != now.div_euclid(60))
        {
            m.hours.push_back(Bucket {
                minute: now.div_euclid(60),
                ..Default::default()
            });
        }
        let bucket = m.hours.back_mut().unwrap();
        for a in &r.attempts {
            if bucket.channels.len() >= 256 && !bucket.channels.contains_key(&a.channel_id) {
                continue;
            }
            let v = bucket.channels.entry(a.channel_id.clone()).or_default();
            v.name = a.channel.clone();
            v.counts.requests += 1;
            v.counts.result(&a.outcome, a.first_ms);
        }
        if r.outcome != "success" {
            m.failures.push_front(r.clone());
            m.failures.truncate(200);
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.finish(true);
    }
}
pub fn failure_reason(status: u16) -> &'static str {
    match status {
        401 => "credential_failed",
        402 => "quota_exhausted",
        429 => "rate_limited",
        0 => "connection_or_timeout",
        _ => "upstream_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_bounded_and_restart_safe() {
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        for _ in 0..205 {
            let (trace, mut guard) = Trace::new(metrics.clone());
            trace.begin(&metrics);
            trace.response(503, Some("no_available_channel"));
            guard.finish(false);
        }
        let (trace, guard) = Trace::new(metrics.clone());
        trace.begin(&metrics);
        assert_eq!(metrics.lock().unwrap().active, 1);
        drop(guard);
        let m = metrics.lock().unwrap();
        assert_eq!(m.active, 0);
        assert_eq!(m.today.failed, 205);
        assert_eq!(m.today.cancelled, 1);
        assert_eq!(m.failures.len(), 200);
        let mut restored: Metrics =
            serde_json::from_value(serde_json::to_value(&*m).unwrap()).unwrap();
        assert_eq!(restored.active, 0);
        assert_eq!(restored.today.requests, 206);
        restored.roll(chrono::Utc::now().timestamp() + 86400);
        assert_eq!(restored.today.requests, 0);
        assert!(restored.hours.is_empty());
    }
    #[test]
    fn retry_keeps_each_cause_and_counts_once() {
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let channel:crate::config::Channel=serde_json::from_value(serde_json::json!({"id":"c","name":"provider","base_url":"http://localhost","upstream_model":"m","adapter":"auto","enabled":true,"keys":[]})).unwrap();
        let key = crate::config::Key {
            enabled: true,
            id: "k".into(),
            label: "key".into(),
            secret: "PRIVATE-CREDENTIAL".into(),
        };
        let (trace, mut guard) = Trace::new(metrics.clone());
        trace.begin(&metrics);
        trace.attempt(&channel, &key);
        trace.status(503);
        trace.retry("upstream_error");
        trace.attempt(&channel, &key);
        trace.status(429);
        trace.retry("rate_limited");
        trace.response(503, Some("no_available_channel"));
        guard.finish(false);
        guard.finish(false);
        let mut m = metrics.lock().unwrap();
        assert_eq!(m.today.switches, 1);
        assert_eq!(m.today.failed, 1);
        assert_eq!(m.failures[0].attempts[1].reason, "rate_limited");
        assert_eq!(m.view()["providers"]["c"]["failed"], 2);
        assert!(
            !serde_json::to_string(&*m)
                .unwrap()
                .contains("PRIVATE-CREDENTIAL")
        );
    }
}
