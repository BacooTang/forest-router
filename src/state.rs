use crate::balance::Allowance;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct State {
    pub telemetry: crate::telemetry::Metrics,
    pub config_digest: String,
    pub identities: HashMap<String, String>,
    pub monitor_identities: HashMap<String, String>,
    pub notices: VecDeque<Notice>,
    pub keys: HashMap<String, KeyState>,
    pub quality: HashMap<String, Quality>,
    pub events: VecDeque<Event>,
    pub last_used: HashMap<String, String>,
    #[serde(skip)]
    pub round_robin: HashMap<String, usize>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyState {
    pub detected: Option<crate::balance::Adapter>,
    pub checked: bool,
    pub protocol_warning: bool,
    pub request_exhausted: bool,
    pub allowance: Option<Allowance>,
    pub balance_checked: i64,
    pub balance_error: Option<String>,
    pub service_failed: bool,
    pub suspect: bool,
    pub confirmation_at: i64,
    pub confirmation_failures: u32,
    pub last_incident: Option<i64>,
    pub credential_failed: bool,
    pub cooldown_until: i64,
    pub retry_at: i64,
    pub probe_step: u32,
    pub probe_attempts: u32,
    pub probe_exhausted: bool,
    pub recovery_successes: u32,
    pub last_recovery_success: i64,
    pub failure_cycles: VecDeque<i64>,
    pub revision: u64,
    pub reason: String,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Quality {
    pub response_ms: Option<u64>,
    pub last_definite: String,
    pub history: VecDeque<QualityPoint>,
    pub error: String,
    pub verdict: String,
    pub checked_at: i64,
    pub valid_until: i64,
    pub next_at: i64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Event {
    pub at: i64,
    pub kind: String,
    pub message: String,
}
impl State {
    pub fn event(&mut self, kind: &str, message: String) {
        while self.events.len() >= 200 {
            self.events.pop_front();
        }
        self.events.push_back(Event {
            at: chrono::Utc::now().timestamp(),
            kind: kind.into(),
            message: message.chars().take(1000).collect(),
        });
    }
}
impl State {
    pub fn channel_ready(&self, c: &crate::config::Channel, now: i64) -> bool {
        c.enabled
            && c.keys
                .iter()
                .any(|k| k.enabled && self.keys.get(&k.id).is_none_or(|s| s.eligible(now)))
            && c.monitor_id.as_ref().is_none_or(|id| {
                self.quality
                    .get(id)
                    .is_some_and(|q| q.last_definite == "healthy" && q.valid_until > now)
            })
    }
}
impl KeyState {
    pub fn eligible(&self, now: i64) -> bool {
        !self.probe_exhausted
            && !self.service_failed
            && !self.credential_failed
            && self.retry_at <= now
            && !self.allowance.as_ref().is_some_and(|a| a.unavailable(now))
    }
    /// Multiple in-flight failures during the same incident do not escalate it.
    pub fn suspect_service(&mut self, now: i64) -> bool {
        if self.suspect || self.service_failed {
            return false;
        }
        self.last_incident = Some(now);
        self.suspect = true;
        self.confirmation_at = now + 5;
        self.confirmation_failures = 0;
        self.revision += 1;
        false
    }
    pub fn confirmation_failed(&mut self, now: i64) -> bool {
        self.confirmation_failures = self.confirmation_failures.saturating_add(1);
        if self.confirmation_failures >= 3
            && self.last_incident.is_some_and(|start| now - start >= 60)
        {
            return self.fail_service(now);
        }
        self.confirmation_at = now + 15;
        self.revision += 1;
        false
    }
    pub fn fail_service(&mut self, now: i64) -> bool {
        self.suspect = false;
        let transition = !self.service_failed;
        if transition {
            self.failure_cycles.retain(|t| now - *t < 600);
            self.failure_cycles.push_back(now);
            while self.failure_cycles.len() > 8 {
                self.failure_cycles.pop_front();
            }
        }
        self.service_failed = true;
        if transition {
            self.recovery_successes = 0;
            self.probe_step = 0;
            self.probe_attempts = 0;
            self.probe_exhausted = false;
            self.retry_at = now + 30;
        }
        self.revision += 1;
        transition
    }
    pub fn probe_result(&mut self, now: i64, ok: bool) {
        self.revision += 1;
        self.probe_attempts = self.probe_attempts.saturating_add(1);
        if ok {
            if self.reason == "连续恢复探测预算已耗尽，等待手动验证或修改配置"
            {
                self.reason.clear();
            }
            if self.recovery_successes == 0 || now - self.last_recovery_success >= 60 {
                self.recovery_successes += 1;
                self.last_recovery_success = now;
            }
            if self.recovery_successes >= 2 {
                self.service_failed = false;
                self.probe_attempts = 0;
                self.probe_exhausted = false;
                self.retry_at = 0;
                self.probe_step = 0;
                self.reason.clear();
                return;
            }
            self.retry_at = now + 60;
        } else {
            self.recovery_successes = 0;
            self.probe_step = (self.probe_step + 1).min(4);
            self.retry_at = now + 60 * (self.probe_step as i64 + 1);
        }
        if self.probe_attempts >= 12 {
            self.probe_exhausted = true;
            self.retry_at = 0;
            self.reason = "连续恢复探测预算已耗尽，等待手动验证或修改配置".into();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_incident_confirmation_and_parallel_failures() {
        let mut s = KeyState::default();
        assert!(!s.suspect_service(100));
        assert!(s.suspect);
        assert!(!s.service_failed);
        let rev = s.revision;
        assert!(!s.suspect_service(101));
        assert_eq!(s.revision, rev);
        assert!(s.eligible(100));
        assert!(!s.confirmation_failed(105));
        assert!(!s.confirmation_failed(120));
        assert!(!s.confirmation_failed(135));
        assert!(!s.confirmation_failed(150));
        assert!(s.confirmation_failed(165));
        assert!(!s.eligible(165));
    }
    #[test]
    fn recovered_incident_gets_a_new_confirmation_window() {
        let mut s = KeyState::default();
        s.suspect_service(100);
        s.suspect = false;
        s.retry_at = 0;
        assert!(!s.suspect_service(150));
        assert!(!s.service_failed);
        assert!(s.suspect);
        assert_eq!(s.last_incident, Some(150));
    }
    #[test]
    fn slow_probes_still_require_three_failures_and_hard_faults_skip_grace() {
        let mut s = KeyState::default();
        s.suspect_service(100);
        assert!(!s.confirmation_failed(160));
        assert!(!s.confirmation_failed(220));
        assert!(s.eligible(220));
        assert!(s.confirmation_failed(280));
        let mut s = KeyState::default();
        s.suspect_service(100);
        assert!(s.fail_service(101));
        assert!(!s.suspect);
        assert!(!s.eligible(101));
    }
    #[test]
    fn degraded_quality_excludes_all_keys_and_legacy_hold_is_ignored() {
        let channel:crate::config::Channel=serde_json::from_value(serde_json::json!({"id":"a","name":"A","base_url":"http://localhost","upstream_model":"m","adapter":"auto","enabled":true,"monitor_id":"q","keys":[{"id":"k1","label":"1","secret":"fixture"},{"id":"k2","label":"2","secret":"fixture"}]})).unwrap();
        let mut state:State=serde_json::from_value(serde_json::json!({"sticky_routes":{"m":{"channel_id":"b","recovered":{},"revision":1}}})).unwrap();
        assert!(
            !serde_json::to_value(&state)
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("sticky_routes")
        );
        state.quality.insert(
            "q".into(),
            Quality {
                verdict: "degraded".into(),
                last_definite: "degraded".into(),
                valid_until: 999,
                ..Default::default()
            },
        );
        assert!(!state.channel_ready(&channel, 100));
        state.quality.get_mut("q").unwrap().last_definite = "healthy".into();
        state.quality.get_mut("q").unwrap().verdict = "healthy".into();
        assert!(state.channel_ready(&channel, 100));
        assert!(!state.channel_ready(&channel, 1000));
    }
    #[test]
    fn recovery_budget_stops_permanent_failures() {
        let mut s = KeyState::default();
        s.fail_service(0);
        for _ in 0..12 {
            let now = s.retry_at;
            s.probe_result(now, false);
        }
        assert!(s.probe_exhausted);
        assert_eq!(s.retry_at, 0);
        assert!(!s.eligible(999999));
    }
    #[test]
    fn first_probe_after_thirty_seconds_then_two_three_four_five_minutes() {
        let mut s = KeyState::default();
        s.fail_service(0);
        assert_eq!(s.retry_at, 30);
        s.fail_service(10);
        assert_eq!(s.retry_at, 30);
        for (t, expected) in [(30, 150), (150, 330), (330, 570), (570, 870), (870, 1170)] {
            s.probe_result(t, false);
            assert_eq!(s.retry_at, expected);
        }
    }
    #[test]
    fn immediate_repeated_success_cannot_bypass_recovery_window() {
        let mut s = KeyState::default();
        s.fail_service(0);
        s.probe_result(30, true);
        assert_eq!(s.retry_at, 90);
        s.probe_result(31, true);
        assert!(s.service_failed);
        s.probe_result(90, true);
        assert!(!s.service_failed);
    }
    #[test]
    fn successful_probe_clears_stale_budget_reason() {
        let mut s = KeyState::default();
        s.fail_service(0);
        s.reason = "连续恢复探测预算已耗尽，等待手动验证或修改配置".into();
        s.probe_attempts = 4;
        s.probe_result(30, true);
        assert!(s.service_failed);
        assert!(!s.probe_exhausted);
        assert!(s.reason.is_empty());
    }

    #[test]
    fn concurrent_failures_are_one_cycle() {
        let mut s = KeyState::default();
        assert!(s.fail_service(100));
        assert!(!s.fail_service(101));
        assert_eq!(s.failure_cycles.len(), 1);
    }
    #[test]
    fn recovery_never_blocks_routing_with_cooldown() {
        let mut s = KeyState::default();
        s.fail_service(100);
        s.cooldown_until = 999999;
        s.probe_result(160, true);
        s.probe_result(220, true);
        assert!(s.eligible(220));
    }
    #[test]
    fn events_bounded() {
        let mut s = State::default();
        for _ in 0..1000 {
            s.event("test", "hello".into());
        }
        assert_eq!(s.events.len(), 200);
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QualityPoint {
    pub at: i64,
    pub verdict: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Notice {
    pub id: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub card: Option<serde_json::Value>,
    pub attempts: u32,
    pub next_at: i64,
}
impl State {
    pub fn notify(&mut self, message: String) {
        if self.notices.len() >= 200 {
            self.notices.pop_front();
            self.event("notification", "通知队列已满，移除最早待发送通知".into());
        }
        self.notices.push_back(Notice {
            id: format!("{:016x}", rand::random::<u64>()),
            message,
            card: None,
            attempts: 0,
            next_at: 0,
        });
    }
    /// State identity excludes display names, order, listener, password and webhook.
    pub fn reconcile(&mut self, c: &crate::config::Config) {
        let legacy = self.identities.is_empty() && self.config_digest == c.digest();
        let mut ids = HashMap::new();
        for m in &c.models {
            for ch in &m.channels {
                for k in &ch.keys {
                    let identity = ch.key_identity(&m.id, k);
                    if !legacy && self.identities.get(&k.id) != Some(&identity) {
                        self.keys.remove(&k.id);
                    }
                    ids.insert(k.id.clone(), identity);
                }
            }
        }
        self.identities = ids;
        let mut mids = HashMap::new();
        for m in &c.monitors {
            let identity = m.identity();
            if !legacy && self.monitor_identities.get(&m.id) != Some(&identity) {
                self.quality.remove(&m.id);
            }
            mids.insert(m.id.clone(), identity);
        }
        self.monitor_identities = mids;
        self.config_digest = c.digest();
        self.prune(c);
    }
    pub fn prune(&mut self, c: &crate::config::Config) {
        let keys = c
            .models
            .iter()
            .flat_map(|m| &m.channels)
            .flat_map(|c| &c.keys)
            .map(|k| k.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        self.keys.retain(|id, _| keys.contains(id.as_str()));
        self.quality
            .retain(|id, _| c.monitors.iter().any(|m| m.id == *id));
        self.last_used.retain(|id, ch| {
            c.models
                .iter()
                .any(|m| m.id == *id && m.channels.iter().any(|c| c.id == *ch))
        });
        self.round_robin.retain(|id, _| {
            c.models
                .iter()
                .any(|m| m.channels.iter().any(|c| c.id == *id))
        });
        self.events.truncate(200);
        self.notices.truncate(200);
        for q in self.quality.values_mut() {
            while q.history.len() > 72 {
                q.history.pop_front();
            }
        }
    }
}
