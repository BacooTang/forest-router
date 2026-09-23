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
    let started = std::time::Instant::now();
    let outcome=async {
        let response=app.upstream_client(cfg.use_system_proxy).post(format!("{root}/chat/completions")).bearer_auth(&m.key)
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
    q.response_ms = outcome
        .as_ref()
        .ok()
        .map(|_| started.elapsed().as_millis() as u64);
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
    if previously_healthy
        && verdict == "degraded"
        && !cfg.webhook.is_empty()
        && !cfg.notify_all_monitors
    {
        state.notify(format!(
            "降智提醒：{}（{}）已从满血变为降智，关联渠道暂停使用。",
            m.name, m.id
        ));
    }
    true
}
// One batch produces one summary, including manual runs. Never publish stale config results.
pub async fn batch(app: &Arc<App>, cfg: &Arc<Config>, monitors: Vec<Monitor>) -> Option<usize> {
    use futures_util::{StreamExt, stream};
    if monitors.is_empty() {
        return Some(0);
    }
    let _guard = app.begin("monitor-round".into())?;
    let done = stream::iter(monitors)
        .map(|m| async move {
            if check(app, cfg, &m).await {
                Some(m.id)
            } else {
                None
            }
        })
        .buffer_unordered(2)
        .filter_map(|id| async move { id })
        .collect::<Vec<_>>()
        .await;
    let current = app.config.read().await;
    if !Arc::ptr_eq(&current, cfg) {
        return None;
    }
    if !done.is_empty() && cfg.notify_all_monitors && !cfg.webhook.is_empty() {
        let mut state = app.state.lock().await;
        for card in summary_cards(cfg, &state, &done, chrono::Utc::now().timestamp()) {
            state.notify("降智监控全量检测报告".into());
            state.notices.back_mut().unwrap().card = Some(card);
        }
    }
    drop(current);
    let _ = app.persist().await;
    Some(done.len())
}
fn summary_cards(
    cfg: &Config,
    state: &crate::state::State,
    done: &[String],
    now: i64,
) -> Vec<serde_json::Value> {
    let mut counts = [0usize; 4];
    let mut rows = Vec::new();
    for m in &cfg.monitors {
        let q = state.quality.get(&m.id);
        let checked = done.contains(&m.id);
        let (index, label) = if !checked {
            (2, "⚪ 本轮未检测")
        } else {
            match q.map(|q| q.verdict.as_str()) {
                Some("healthy") => (0, "🟢 满血"),
                Some("degraded") => (1, "🔴 非满血"),
                _ => (3, "🟠 验证异常"),
            }
        };
        counts[index] += 1;
        let elapsed = q
            .filter(|_| checked)
            .and_then(|q| q.response_ms)
            .map(|ms| format!("{:.1}s", ms as f64 / 1000.))
            .unwrap_or("—".into());
        rows.push(json!({"tag":"div","fields":[
            {"is_short":true,"text":{"tag":"plain_text","content":m.name}},
            {"is_short":true,"text":{"tag":"plain_text","content":format!("{label} · 响应 {elapsed}")}}
        ]}));
    }
    let (color, title) = if counts[1] > 0 {
        ("red", "满血检测 · 存在非满血渠道")
    } else if counts[2] + counts[3] > 0 {
        ("orange", "满血检测 · 部分渠道尚不能判定")
    } else {
        ("green", "满血检测 · 全部满血")
    };
    let fields = ["满血", "非满血", "本轮未检测", "验证异常"].iter().enumerate().map(|(i, label)|
        json!({"is_short":true,"text":{"tag":"plain_text","content":format!("{label}\n{} 个", counts[i])}})
    ).collect::<Vec<_>>();
    let mut cards = Vec::new();
    // Feishu cards have a payload limit: preserve full names and paginate large configurations.
    for (page, chunk) in rows.chunks(20).enumerate() {
        let mut elements = vec![json!({"tag":"div","fields":fields}), json!({"tag":"hr"})];
        elements.extend_from_slice(chunk);
        let time = chrono::DateTime::from_timestamp(now, 0)
            .unwrap()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
            .format("%m-%d %H:%M")
            .to_string();
        elements.push(json!({"tag":"hr"}));
        elements.push(json!({"tag":"note","elements":[{"tag":"plain_text","content":format!("{time} · 共 {} 个监控 · 本轮检测 {} 个 · 糖果判据\n详细结果请到降智监控页面查看", cfg.monitors.len(), done.len())}]}));
        cards.push(json!({"config":{"wide_screen_mode":true},"header":{"template":color,"title":{"tag":"plain_text","content":format!("{title} · {}/{}", page+1, rows.len().div_ceil(20))}},"elements":elements}));
    }
    cards
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn summary_keeps_unknown_and_stale_results_separate() {
        let cfg: Config = serde_json::from_value(json!({"listen":"0.0.0.0:8119","api_key":"test","admin_password_hash":"",
            "monitors":[{"id":"a","name":"A","base_url":"http://localhost","key":"secret","model":"test"},
                        {"id":"b","name":"B","base_url":"http://localhost","key":"secret","model":"test"}]})).unwrap();
        assert!(!cfg.notify_all_monitors);
        let mut state = crate::state::State::default();
        state.quality.entry("a".into()).or_default().verdict = "healthy".into();
        state.quality.entry("b".into()).or_default().verdict = "healthy".into();
        let cards = summary_cards(&cfg, &state, &["a".into()], 0);
        assert_eq!(cards[0]["header"]["template"], "orange");
        assert!(cards[0].to_string().contains("本轮未检测"));
        assert!(!cards[0].to_string().contains("secret"));
        state.quality.get_mut("b").unwrap().verdict = "degraded".into();
        assert_eq!(
            summary_cards(&cfg, &state, &["a".into(), "b".into()], 0)[0]["header"]["template"],
            "red"
        );
        state.quality.get_mut("b").unwrap().verdict = "unknown".into();
        assert_eq!(
            summary_cards(&cfg, &state, &["a".into(), "b".into()], 0)[0]["header"]["template"],
            "orange"
        );
    }
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
