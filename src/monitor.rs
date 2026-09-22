use crate::{
    app::App,
    config::{Config, Monitor},
    state::QualityPoint,
    upstream,
};
use chrono::Timelike;
use serde_json::json;
use std::{sync::Arc, time::Duration};
// Exact prompt and independent-number criterion from codex-test/core.py.
const CANDY: &str = "不使用任何外部工具回答以下问题：\n\n在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。现已知不同口味的糖和不同形状的数量统计如下表。参赛者需要在活动前决定摸出的糖果数目，那么，最少取出多少个糖果才能保证手中同时拥有不同形状的苹果味和桃子味的糖？（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）\n\n        苹果味  桃子味  西瓜味\n圆形       7      9      8\n五角星形   7      6      4\n";
pub fn judge(text: &str) -> bool {
    let c = text.chars().collect::<Vec<_>>();
    c.windows(2).enumerate().any(|(i, w)| {
        w == ['2', '1']
            && (i == 0 || !c[i - 1].is_numeric())
            && (i + 2 == c.len() || !c[i + 2].is_numeric())
    })
}
pub fn next(now: i64, periods: &[crate::config::MonitorPeriod]) -> i64 {
    for t in (now.div_euclid(60) + 1..=now.div_euclid(60) + 1441).map(|m| m * 60) {
        let dt = chrono::DateTime::from_timestamp(t, 0)
            .unwrap()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
        let minute = (dt.hour() * 60 + dt.minute()) as u16;
        let due = if let Some(p) = periods.iter().find(|p| p.contains(minute)) {
            ((minute + 1440 - p.start) % 1440).is_multiple_of(p.interval_minutes)
        } else {
            minute.is_multiple_of(20)
        };
        if due {
            return t;
        }
    }
    now + 86400
}
pub async fn check(app: &Arc<App>, cfg: &Arc<Config>, m: &Monitor) -> bool {
    let Some(_guard) = app.begin(format!("monitor:{}", m.id)) else {
        return false;
    };
    let Ok(_permit) = app.checks.clone().acquire_owned().await else {
        return false;
    };
    let mut root = m.base_url.trim_end_matches('/').to_owned();
    for suffix in ["/chat/completions", "/responses"] {
        if root.ends_with(suffix) {
            root.truncate(root.len() - suffix.len());
        }
    }
    if !root.ends_with("/v1") {
        root.push_str("/v1");
    }
    let outcome=async {
        let response=app.client.post(format!("{root}/chat/completions")).bearer_auth(&m.key)
            .json(&json!({"model":m.model,"messages":[{"role":"user","content":CANDY}],"stream":false,"thinking":{"type":"enabled"},"reasoning_effort":"low"}))
            .timeout(Duration::from_secs(180)).send().await.map_err(|_|"检测请求失败".to_owned())?;
        let v=upstream::bounded_json(response).await?;
        let text=v["choices"][0]["message"]["content"].as_str().filter(|s|!s.is_empty()).ok_or("检测响应缺少答案")?;
        if v["choices"][0]["finish_reason"]=="length" {return Err("检测答案被截断".into());}
        Ok::<_,String>(judge(text))
    }.await;
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return false;
    }
    let now = chrono::Utc::now().timestamp();
    let mut state = app.state.lock().await;
    let q = state.quality.entry(m.id.clone()).or_default();
    let was = q.verdict.clone();
    let previously_healthy = q.last_definite == "healthy";
    let verdict = match &outcome {
        Ok(true) => "healthy",
        Ok(false) => "degraded",
        Err(_) => "unknown",
    };
    q.verdict = verdict.into();
    q.checked_at = now;
    q.next_at = next(now, &cfg.monitor_schedule);
    if outcome.is_ok() {
        q.last_definite = verdict.into();
        q.valid_until = q.next_at + 300;
    }
    q.error = outcome.err().unwrap_or_default();
    while q.history.len() >= 72 {
        q.history.pop_front();
    }
    q.history.push_back(QualityPoint {
        at: now,
        verdict: verdict.into(),
    });
    if was != verdict {
        state.event(
            "quality",
            format!(
                "监控 {}：{}",
                m.name,
                match verdict {
                    "healthy" => "满血",
                    "degraded" => "降智",
                    _ => "检测异常，保留未过期的上次结论",
                }
            ),
        );
    }
    if previously_healthy && verdict == "degraded" && !cfg.webhook.is_empty() {
        state.notify(format!(
            "降智提醒：{}（{}）已从满血变为降智，关联渠道暂停使用。",
            m.name, m.id
        ));
    }
    true
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn answer_boundaries() {
        for t in ["21", "答案为21。", "x21x"] {
            assert!(judge(t));
        }
        for t in ["121", "210", "２０２１", ""] {
            assert!(!judge(t));
        }
    }
    #[test]
    fn custom_schedule_and_validation() {
        use crate::config::{MonitorPeriod, validate_monitor_schedule};
        let periods = vec![MonitorPeriod {
            start: 1410,
            end: 90,
            interval_minutes: 15,
        }];
        assert!(validate_monitor_schedule(&periods).is_ok());
        for (from, to) in [
            ("2026-09-22T23:31:00+08:00", "2026-09-22T23:45:00+08:00"),
            ("2026-09-22T23:59:00+08:00", "2026-09-23T00:00:00+08:00"),
            ("2026-09-23T01:30:00+08:00", "2026-09-23T01:40:00+08:00"),
        ] {
            let ts = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp();
            assert_eq!(next(ts(from), &periods), ts(to));
        }
        let mut overlap = periods.clone();
        overlap.push(MonitorPeriod {
            start: 0,
            end: 60,
            interval_minutes: 5,
        });
        assert!(validate_monitor_schedule(&overlap).is_err());
        assert!(
            validate_monitor_schedule(&[MonitorPeriod {
                start: 0,
                end: 0,
                interval_minutes: 0
            }])
            .is_err()
        );
        assert!(
            validate_monitor_schedule(&[MonitorPeriod {
                start: 0,
                end: 0,
                interval_minutes: 1440
            }])
            .is_ok()
        );
    }
    #[test]
    fn schedule_boundaries() {
        for (s, e) in [
            ("2026-09-22T08:59:00+08:00", "2026-09-22T09:00:00+08:00"),
            ("2026-09-22T22:59:00+08:00", "2026-09-22T23:00:00+08:00"),
            ("2026-09-22T04:00:00+08:00", "2026-09-22T05:00:00+08:00"),
            ("2026-09-22T06:59:00+08:00", "2026-09-22T07:00:00+08:00"),
        ] {
            let t = chrono::DateTime::parse_from_rfc3339(s).unwrap().timestamp();
            assert_eq!(
                next(t, &crate::config::default_monitor_schedule()),
                chrono::DateTime::parse_from_rfc3339(e).unwrap().timestamp()
            );
        }
    }
}
