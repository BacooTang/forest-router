//! Per-client daily usage. No credentials or response content are retained.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub fn day(at: i64) -> i64 {
    (at + 8 * 3600).div_euclid(86400)
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Tokens {
    pub input: u64,
    pub cached: u64,
    pub output: u64,
}
impl Tokens {
    pub fn parse(v: &serde_json::Value) -> Option<Self> {
        Some(Self {
            input: v.get("input_tokens")?.as_u64()?,
            output: v.get("output_tokens")?.as_u64()?,
            cached: v
                .pointer("/input_tokens_details/cached_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        })
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Daily {
    pub requests: u64,
    pub missing: u64,
    pub input: u64,
    pub cached: u64,
    pub output: u64,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Client {
    pub name: String,
    pub last_used: i64,
    pub days: BTreeMap<i64, Daily>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Ledger {
    pub clients: BTreeMap<String, Client>,
}
impl Ledger {
    pub fn prune(&mut self, now: i64) {
        let today = day(now);
        self.clients.retain(|_, c| {
            c.days.retain(|d, _| *d >= today - 4 && *d <= today);
            !c.days.is_empty()
        });
    }
    pub fn record(
        &mut self,
        id: &str,
        name: &str,
        at: i64,
        model: &str,
        attempts: &[crate::telemetry::Attempt],
    ) {
        let now = chrono::Utc::now().timestamp();
        self.prune(now);
        if day(at) < day(now) - 4 {
            return;
        }
        let c = self.clients.entry(id.into()).or_default();
        c.name = name.into();
        c.last_used = c.last_used.max(at);
        let d = c.days.entry(day(at)).or_default();
        d.requests += 1;
        // These model families are always excluded from token accounting.
        // Keep request activity, but do not report intentionally ignored usage as missing.
        let model = model.to_ascii_lowercase();
        if model.contains("glm") || model.contains("deepseek") {
            return;
        }
        if attempts.is_empty() || attempts.iter().any(|a| a.usage.is_none()) {
            d.missing += 1;
        }
        for t in attempts.iter().filter_map(|a| a.usage.as_ref()) {
            d.input = d.input.saturating_add(t.input);
            d.cached = d.cached.saturating_add(t.cached);
            d.output = d.output.saturating_add(t.output);
        }
    }
}

// Observe just the small /response/usage or /usage object, even in very large SSE frames.
#[derive(Default)]
pub struct Scanner {
    invalid: bool,
    stack: Vec<(String, String)>,
    token: Vec<u8>,
    string: bool,
    escape: bool,
    was_string: bool,
    pending: String,
    capture: Option<(usize, Vec<u8>)>,
    pub found: Option<Tokens>,
}
impl Scanner {
    pub fn byte(&mut self, b: u8) {
        if self.invalid {
            return;
        }
        if self.stack.len() > 128 {
            self.invalid = true;
            self.capture = None;
            self.stack.clear();
            return;
        }
        if let Some((_, v)) = &mut self.capture {
            if v.len() < 16384 {
                v.push(b)
            } else {
                self.capture = None;
            }
        }
        if self.string {
            if self.escape {
                self.escape = false;
                if self.token.len() < 128 {
                    self.token.push(b)
                };
                return;
            }
            if b == b'\\' {
                self.escape = true;
                return;
            }
            if b == b'"' {
                self.string = false;
                self.was_string = true;
                self.pending = String::from_utf8_lossy(&self.token).into();
            } else if self.token.len() < 128 {
                self.token.push(b)
            }
            return;
        }
        match b {
            b'"' => {
                self.string = true;
                self.token.clear();
            }
            b':' => {
                if self.was_string
                    && let Some(f) = self.stack.last_mut()
                {
                    f.1 = std::mem::take(&mut self.pending)
                }
                self.was_string = false;
            }
            b'{' | b'[' => {
                let key = self.stack.last().map(|f| f.1.clone()).unwrap_or_default();
                let target = b == b'{'
                    && key == "usage"
                    && (self.stack.len() == 1
                        || (self.stack.len() == 2 && self.stack[1].0 == "response"));
                self.stack.push((key, String::new()));
                if target {
                    self.capture = Some((self.stack.len(), vec![b]));
                }
                self.was_string = false;
            }
            b'}' | b']' => {
                if self
                    .capture
                    .as_ref()
                    .is_some_and(|(depth, _)| *depth == self.stack.len())
                {
                    let (_, raw) = self.capture.take().unwrap();
                    if let Ok(v) = serde_json::from_slice(&raw) {
                        self.found = Tokens::parse(&v);
                    }
                }
                self.stack.pop();
                self.was_string = false;
            }
            b',' => {
                if let Some(f) = self.stack.last_mut() {
                    f.1.clear()
                }
                self.was_string = false;
            }
            _ => {}
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn large_frame_usage_and_nested_decoy() {
        let raw = format!(
            r#"{{"response":{{"output":[{{"usage":{{"input_tokens":999,"output_tokens":999}}}}],"pad":"{}","usage":{{"input_tokens":20,"output_tokens":3,"input_tokens_details":{{"cached_tokens":10}}}}}}}}"#,
            "x".repeat(70000)
        );
        let mut s = Scanner::default();
        for b in raw.bytes() {
            s.byte(b)
        }
        let t = s.found.unwrap();
        assert_eq!((t.input, t.cached, t.output), (20, 10, 3));
    }
    #[test]
    fn attempts_missing_and_persistence() {
        let now = chrono::Utc::now().timestamp();
        let attempts = vec![
            crate::telemetry::Attempt {
                usage: Some(Tokens {
                    input: 20,
                    cached: 10,
                    output: 3,
                }),
                ..Default::default()
            },
            crate::telemetry::Attempt {
                usage: Some(Tokens {
                    input: 5,
                    cached: 0,
                    output: 2,
                }),
                ..Default::default()
            },
            crate::telemetry::Attempt::default(),
        ];
        let mut l = Ledger::default();
        l.record("employee", "员工", now, "gpt-6", &attempts);
        let restored: Ledger = serde_json::from_slice(&serde_json::to_vec(&l).unwrap()).unwrap();
        let d = &restored.clients["employee"].days[&day(now)];
        assert_eq!(
            (d.requests, d.missing, d.input, d.cached, d.output),
            (1, 1, 25, 10, 5)
        );
    }
    #[test]
    fn excluded_models_keep_activity_without_tokens_or_missing_usage() {
        let now = chrono::Utc::now().timestamp();
        let mut ledger = Ledger::default();
        let attempts = [
            crate::telemetry::Attempt {
                usage: Some(Tokens {
                    input: 20,
                    cached: 10,
                    output: 3,
                }),
                ..Default::default()
            },
            crate::telemetry::Attempt::default(),
        ];
        for model in [
            "GLM-5.3",
            "zhipu/gLm-5",
            "DeepSeek-V4",
            "vendor/DEEPSEEK-flash",
        ] {
            ledger.record("employee", "员工", now, model, &attempts);
            ledger.record("employee", "员工", now, model, &[]);
        }
        let c = &ledger.clients["employee"];
        let d = &c.days[&day(now)];
        assert_eq!(c.last_used, now);
        assert_eq!(
            (d.requests, d.missing, d.input, d.cached, d.output),
            (8, 0, 0, 0, 0)
        );
        ledger.record("employee", "员工", now, "gpt-6", &attempts);
        let d = &ledger.clients["employee"].days[&day(now)];
        assert_eq!(
            (d.requests, d.missing, d.input, d.cached, d.output),
            (9, 1, 20, 10, 3)
        );
    }
    #[test]
    fn scanner_depth_is_bounded() {
        let mut s = Scanner::default();
        for _ in 0..10000 {
            s.byte(b'[');
        }
        assert!(s.stack.len() <= 129);
        assert!(s.found.is_none());
    }
    #[test]
    fn midnight_and_retention() {
        assert_eq!(day(16 * 3600 - 1), 0);
        assert_eq!(day(16 * 3600), 1);
        let mut l = Ledger::default();
        let c = l.clients.entry("k".into()).or_default();
        for d in 0..7 {
            c.days.insert(d, Daily::default());
        }
        l.prune(6 * 86400);
        assert_eq!(l.clients["k"].days.len(), 5);
        assert!(!l.clients["k"].days.contains_key(&1));
    }
}
