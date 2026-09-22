use crate::balance::Adapter;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub listen: String,
    pub api_key: String,
    pub admin_password_hash: String,
    #[serde(default)]
    pub webhook: String,
    #[serde(default)]
    pub models: Vec<Model>,
    #[serde(default)]
    pub monitors: Vec<Monitor>,
    #[serde(default = "default_monitor_schedule")]
    pub monitor_schedule: Vec<MonitorPeriod>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MonitorPeriod {
    pub start: u16,
    pub end: u16,
    pub interval_minutes: u16,
}
pub fn default_monitor_schedule() -> Vec<MonitorPeriod> {
    vec![
        MonitorPeriod {
            start: 540,
            end: 1380,
            interval_minutes: 5,
        },
        MonitorPeriod {
            start: 1380,
            end: 240,
            interval_minutes: 20,
        },
        MonitorPeriod {
            start: 240,
            end: 420,
            interval_minutes: 60,
        },
        MonitorPeriod {
            start: 420,
            end: 540,
            interval_minutes: 20,
        },
    ]
}
impl MonitorPeriod {
    pub fn contains(&self, minute: u16) -> bool {
        if self.start < self.end {
            minute >= self.start && minute < self.end
        } else {
            minute >= self.start || minute < self.end
        }
    }
}
pub fn validate_monitor_schedule(periods: &[MonitorPeriod]) -> Result<(), String> {
    if periods.len() > 24 {
        return Err("最多24个检测时间段".into());
    }
    let mut occupied = [false; 1440];
    for p in periods {
        if p.start >= 1440 || p.end >= 1440 || p.interval_minutes == 0 || p.interval_minutes > 1440
        {
            return Err("时间范围或检测间隔无效（1–1440分钟）".into());
        }
        for (minute, used) in occupied.iter_mut().enumerate() {
            if p.contains(minute as u16) {
                if *used {
                    return Err("检测时间段不能重叠".into());
                }
                *used = true;
            }
        }
    }
    Ok(())
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub channels: Vec<Channel>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub upstream_model: String,
    pub adapter: Adapter,
    pub enabled: bool,
    #[serde(default)]
    pub monitor_id: Option<String>,
    pub keys: Vec<Key>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Key {
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    pub id: String,
    pub label: String,
    pub secret: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Monitor {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub key: String,
    pub model: String,
}
fn enabled_by_default() -> bool {
    true
}
impl Config {
    pub fn digest(&self) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).expect("serialize config"))
        )
    }
    pub fn validate(&self) -> Result<(), String> {
        validate_monitor_schedule(&self.monitor_schedule)?;
        if self.api_key.is_empty() {
            return Err("公司 API Key 不能为空".into());
        }
        if self.models.len() > 100 || self.monitors.len() > 200 {
            return Err("配置数量超限".into());
        }
        if self.api_key.len() > 4096 || self.webhook.len() > 4096 {
            return Err("设置字段过长".into());
        }
        if self.listen.parse::<std::net::SocketAddr>().is_err() {
            return Err("监听地址必须为IP:端口".into());
        }
        for m in &self.monitors {
            if m.id.len() > 128
                || m.name.len() > 256
                || m.key.is_empty()
                || m.key.len() > 4096
                || m.model.is_empty()
                || m.model.len() > 256
            {
                return Err("监控字段为空或过长".into());
            }
        }
        for m in &self.models {
            if m.id.len() > 256 {
                return Err("模型名过长".into());
            }
            for c in &m.channels {
                if c.id.len() > 128 || c.name.len() > 256 || c.upstream_model.len() > 256 {
                    return Err("渠道字段过长".into());
                }
                for k in &c.keys {
                    if k.id.len() > 128
                        || k.label.len() > 256
                        || k.secret.len() > 4096
                        || k.secret.contains(['\r', '\n'])
                    {
                        return Err("Key字段非法".into());
                    }
                }
            }
        }
        let mut models = HashSet::new();
        let mut channels = HashSet::new();
        let mut keys = HashSet::new();
        let mut monitors = HashSet::new();
        for monitor in &self.monitors {
            if monitor.id.is_empty() || !monitors.insert(&monitor.id) {
                return Err("监控 ID 重复或为空".into());
            }
            validate_url(&monitor.base_url)?;
        }
        for m in &self.models {
            if m.id.is_empty() || !models.insert(&m.id) || m.channels.len() > 100 {
                return Err("模型重复、为空或渠道过多".into());
            }
            for c in &m.channels {
                if c.id.is_empty()
                    || !channels.insert(&c.id)
                    || c.keys.is_empty()
                    || c.keys.len() > 50
                {
                    return Err("渠道 ID 重复或 Key 数量非法".into());
                }
                validate_url(&c.base_url)?;
                if c.name.is_empty() || c.upstream_model.is_empty() {
                    return Err("上游模型不能为空".into());
                }
                if c.monitor_id
                    .as_ref()
                    .is_some_and(|id| !monitors.contains(id))
                {
                    return Err("监控 ID 不存在".into());
                }
                for k in &c.keys {
                    if k.id.is_empty() || k.secret.is_empty() || !keys.insert(&k.id) {
                        return Err("Key ID 重复或为空".into());
                    }
                }
            }
        }
        if keys.len() > 512 || channels.len() > 256 {
            return Err("最多512把Key、256个渠道".into());
        }
        if !self.webhook.is_empty() {
            validate_url(&self.webhook)?;
        }
        Ok(())
    }
}
fn validate_url(raw: &str) -> Result<(), String> {
    if raw.len() > 2048 {
        return Err("URL过长".into());
    }
    let u = reqwest::Url::parse(raw).map_err(|_| "URL 无效")?;
    if !matches!(u.scheme(), "http" | "https")
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
    {
        return Err("URL 格式不支持".into());
    }
    Ok(())
}
pub fn root(base: &str) -> String {
    let mut s = base.trim_end_matches('/');
    if let Some(v) = s.strip_suffix("/responses") {
        s = v;
    }
    if let Some(v) = s.strip_suffix("/v1") {
        s = v;
    }
    s.to_owned()
}
pub fn responses_url(base: &str) -> String {
    let s = base.trim_end_matches('/');
    if s.ends_with("/responses") {
        s.to_owned()
    } else if reqwest::Url::parse(s).is_ok_and(|u| u.path().is_empty() || u.path() == "/") {
        format!("{s}/v1/responses")
    } else {
        format!("{s}/responses")
    }
}

fn fingerprint(value: impl Serialize) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).expect("fingerprint"))
    )
}
impl Channel {
    pub fn key_identity(&self, model: &str, key: &Key) -> String {
        fingerprint((
            &self.base_url,
            &self.upstream_model,
            &key.secret,
            Adapter::for_model(model, self.adapter.clone()),
        ))
    }
}
impl Monitor {
    pub fn identity(&self) -> String {
        fingerprint((&self.base_url, &self.model, &self.key))
    }
}
