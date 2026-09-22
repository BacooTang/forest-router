use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Adapter {
    Auto,
    NewApi,
    Sub2Api,
    Subscription,
    Deepseek,
    Glm,
}
impl Adapter {
    pub fn for_model(model: &str, configured: Self) -> Self {
        let m = model.to_ascii_lowercase();
        if m.contains("deepseek") {
            Self::Deepseek
        } else if m.contains("glm") {
            Self::Glm
        } else {
            configured
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Allowance {
    pub remaining: Option<f64>,
    #[serde(default)]
    pub expires_at: Option<i64>,
    pub unit: String,
    pub unlimited: bool,
    pub exhausted: bool,
    pub windows: Vec<Window>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    pub label: String,
    pub remaining_percent: Option<f64>,
    pub resets_at: Option<String>,
}
fn number(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str()?.parse().ok())
        .filter(|v| v.is_finite())
}
/// Require recognizable schemas; a 200 HTML page is never platform evidence.
pub fn parse(adapter: &Adapter, v: &Value) -> Result<Allowance, &'static str> {
    let mut a = Allowance::default();
    match adapter {
        Adapter::NewApi => {
            let d = &v["data"];
            let unlimited = d["unlimited_quota"]
                .as_bool()
                .ok_or("missing unlimited_quota")?;
            let remaining = number(&d["total_available"]).ok_or("missing token quota")?;
            a.unlimited = unlimited;
            a.remaining = (!unlimited).then_some(remaining);
            a.unit = "quota".into(); // Never assume a deployment's currency conversion.
            a.expires_at = d["expires_at"].as_i64().filter(|t| *t > 0);
            a.exhausted = !unlimited && remaining <= 0.;
        }
        Adapter::Sub2Api => {
            if v["mode"].as_str().is_none() {
                return Err("missing usage mode");
            }
            let remaining = number(&v["remaining"]).ok_or("missing remaining")?;
            let wallet = number(&v["balance"]);
            a.remaining = Some(wallet.map_or(remaining, |b| b.min(remaining)));
            a.unit = v["unit"].as_str().unwrap_or("USD").to_owned();
            a.exhausted = a.remaining.is_some_and(|n| n <= 0.);
            a.expires_at = v["subscription"]["expires_at"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| t.timestamp());
            if let Some(limits) = v["rate_limits"].as_object() {
                for limit in limits.values() {
                    if number(&limit["remaining"]).is_some_and(|n| n <= 0.) {
                        a.exhausted = true;
                    }
                }
            }
        }
        Adapter::Deepseek => {
            let available = v["is_available"].as_bool().ok_or("missing availability")?;
            let entries = v["balance_infos"]
                .as_array()
                .ok_or("missing balance_infos")?;
            let entry = entries
                .iter()
                .find(|b| b["currency"] == "CNY")
                .or(entries.first())
                .ok_or("empty balances")?;
            a.remaining = number(&entry["total_balance"]);
            a.unit = entry["currency"].as_str().unwrap_or("unknown").into();
            a.exhausted = !available;
        }
        Adapter::Subscription => {
            let entries = v["data"].as_array().ok_or("missing subscriptions")?;
            let now = chrono::Utc::now();
            let mut total = 0.;
            let mut known = false;
            let mut active = 0;
            let mut unknown = false;
            let mut latest_expiry = None;
            let mut unbounded_expiry = false;
            for e in entries {
                if let Some(exp) = e["subscription"]["expires_at"]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                {
                    if exp <= now {
                        continue;
                    }
                    latest_expiry = Some(latest_expiry.unwrap_or(0).max(exp.timestamp()));
                } else {
                    unbounded_expiry = true;
                }
                active += 1;
                if let Some(n) = number(&e["progress"]["weekly"]["remaining_usd"]) {
                    total += n.max(0.);
                    known = true;
                } else {
                    unknown = true;
                }
            }
            a.remaining = known.then_some(total);
            a.unit = "USD".into();
            a.exhausted = active == 0 || (known && !unknown && total <= 0.);
            a.expires_at = if unbounded_expiry {
                None
            } else {
                latest_expiry
            };
        }
        Adapter::Glm => {
            let limits = v["data"]["limits"]
                .as_array()
                .ok_or("missing coding plan limits")?;
            for item in limits {
                if item["type"] != "TOKENS_LIMIT" {
                    continue;
                }
                let label = match (item["unit"].as_u64(), item["number"].as_u64()) {
                    (Some(3), Some(5)) => "5小时",
                    (Some(6), Some(1)) => "每周",
                    _ => continue,
                };
                let used = number(&item["percentage"]).filter(|x| (0.0..=100.0).contains(x));
                let remaining = used.map(|n| 100. - n);
                if remaining.is_some_and(|n| n <= 0.) {
                    a.exhausted = true;
                }
                let reset = item["nextResetTime"]
                    .as_i64()
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map(|t| t.to_rfc3339());
                a.windows.push(Window {
                    label: label.into(),
                    remaining_percent: remaining,
                    resets_at: reset,
                });
            }
            if a.windows.is_empty() {
                return Err("unrecognized coding plan periods");
            }
            a.unit = "percent".into();
        }
        Adapter::Auto => return Err("platform must be detected first"),
    }
    Ok(a)
}

impl Allowance {
    pub fn unavailable(&self, now: i64) -> bool {
        self.exhausted || self.expires_at.is_some_and(|t| t <= now)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn expired_subscription_is_unavailable() {
        let a=parse(&Adapter::Subscription,&json!({"data":[{"subscription":{"expires_at":"2020-01-01T00:00:00Z"},"progress":{"weekly":{"remaining_usd":50}}}]})).unwrap();
        assert!(a.exhausted);
    }
    #[test]
    fn glm_used_percent_and_periods() {
        let a=parse(&Adapter::Glm,&json!({"data":{"limits":[{"type":"TIME_LIMIT","percentage":100},{"type":"TOKENS_LIMIT","unit":3,"number":5,"percentage":15},{"type":"TOKENS_LIMIT","unit":6,"number":1,"percentage":100}]}})).unwrap();
        assert_eq!(a.windows.len(), 2);
        assert_eq!(a.windows[0].remaining_percent, Some(85.));
        assert!(a.exhausted);
    }
    #[test]
    fn official_selection_is_case_insensitive() {
        assert_eq!(
            Adapter::for_model("DeepSeek-v4", Adapter::NewApi),
            Adapter::Deepseek
        );
        assert_eq!(Adapter::for_model("GLM-5", Adapter::Sub2Api), Adapter::Glm);
        assert_eq!(Adapter::for_model("gpt-6", Adapter::Auto), Adapter::Auto);
    }
    #[test]
    fn unlimited_negative_token_quota_is_not_empty() {
        let a = parse(
            &Adapter::NewApi,
            &json!({"data":{"unlimited_quota":true,"total_available":-24529151}}),
        )
        .unwrap();
        assert!(!a.exhausted);
        assert!(a.unlimited);
        assert_eq!(a.remaining, None);
    }
    #[test]
    fn wallet_can_limit_an_independently_funded_key() {
        let a = parse(
            &Adapter::Sub2Api,
            &json!({"mode":"quota_limited","remaining":400,"balance":0}),
        )
        .unwrap();
        assert!(a.exhausted);
    }
    #[test]
    fn unknown_is_not_zero() {
        assert!(parse(&Adapter::Sub2Api, &json!({"mode":"unrestricted"})).is_err());
        assert!(parse(&Adapter::NewApi, &json!({"data":{}})).is_err());
    }
}
