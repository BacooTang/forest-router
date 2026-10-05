use crate::{app::App, config::Monitor, upstream};
use serde::Serialize;
use serde_json::json;
use std::time::{Duration, Instant};

pub const PROMPT: &str = include_str!("knowledge_prompt.txt");
pub const EFFORTS: [&str; 3] = ["medium", "high", "xhigh"];
const GROUPS: [(&str, &str, usize, usize); 7] = [
    ("GPT-4o", "2023-10-01", 1, 1),
    ("GPT-4.1", "2024-06-01", 2, 3),
    ("GPT-5 / GPT-5.1", "2024-09-30", 4, 5),
    ("GPT-5.2 / GPT-5.4", "2025-08-31", 6, 10),
    ("GPT-5.5", "2025-12-01", 11, 15),
    ("GPT-5.6", "2026-02-16", 16, 20),
    ("GPT-6 Astra", "2026-04-30", 21, 25),
];

#[derive(Serialize)]
pub struct Question {
    id: String,
    answer: Option<String>,
    answered: bool,
    reason: &'static str,
}
#[derive(Serialize)]
pub struct Group {
    model: &'static str,
    cutoff: &'static str,
    answered: usize,
    total: usize,
}
#[derive(Serialize)]
pub struct Result {
    effort: String,
    pub verdict: &'static str,
    pub answered: usize,
    total: usize,
    questions: Vec<Question>,
    groups: Vec<Group>,
    text: String,
    pub error: String,
    elapsed_ms: u64,
}

// Judge coverage only, never factual correctness or the language of an answer.
fn non_answer(text: &str) -> Option<&'static str> {
    let compact: String = text
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    if compact.is_empty() {
        return Some("空白或占位符");
    }
    if [
        "不清楚",
        "不知道",
        "不确定",
        "不知晓",
        "无法确定",
        "无法确认",
        "无法回答",
        "无法提供",
        "没有答案",
        "未回答",
        "未作答",
        "暂无",
        "不详",
        "未知",
        "idontknow",
        "idonotknow",
        "dontknow",
        "donotknow",
        "notsure",
        "imunsure",
        "cannotdetermine",
        "cantdetermine",
        "unabletodetermine",
        "cannotanswer",
        "cantanswer",
        "unabletoanswer",
        "unanswered",
        "noanswer",
        "notavailable",
        "notknown",
    ]
    .iter()
    .any(|s| compact.contains(s))
    {
        return Some("未明确回答");
    }
    if [
        "na",
        "none",
        "null",
        "nil",
        "tbd",
        "todo",
        "pending",
        "unknown",
        "unsure",
        "uncertain",
        "unavailable",
        "answer",
        "答案",
        "略",
        "无",
    ]
    .contains(&compact.as_str())
    {
        return Some("空白或占位符");
    }
    None
}

fn answer_line(line: &str) -> Option<(usize, &str)> {
    let line = line.trim_start_matches(|c: char| {
        c.is_whitespace() || matches!(c, '"' | '\'' | '“' | '*' | '`' | '#' | '>')
    });
    let bytes = line.as_bytes();
    if bytes.len() < 3 || bytes[0] != b'Q' || !bytes[1..3].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let number = ((bytes[1] - b'0') * 10 + bytes[2] - b'0') as usize;
    if !(1..=25).contains(&number) {
        return None;
    }
    let rest = line[3..].trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '*' | '`'));
    // Empty numbered lines count as empty answers; tolerate harmless delimiter differences.
    let answer = if rest.is_empty() {
        rest
    } else {
        rest.strip_prefix(['｜', '|', '：', ':'])?
    };
    Some((
        number,
        answer
            .trim()
            .trim_matches(['"', '\'', '“', '”', '*', '`'])
            .trim(),
    ))
}

pub fn assess(text: String, effort: &str, error: String, elapsed_ms: u64) -> Result {
    let mut answers: [Option<String>; 25] = std::array::from_fn(|_| None);
    let mut duplicates = [false; 25];
    for (number, answer) in text.lines().filter_map(answer_line) {
        let slot = &mut answers[number - 1];
        if slot.is_some() {
            duplicates[number - 1] = true;
        } else {
            *slot = Some(answer.to_owned());
        }
    }
    let questions: Vec<_> = answers
        .into_iter()
        .enumerate()
        .map(|(i, answer)| {
            let reason = if duplicates[i] {
                "题号重复"
            } else {
                answer
                    .as_deref()
                    .map_or(Some("缺少题号或答案格式无法识别"), non_answer)
                    .unwrap_or("")
            };
            Question {
                id: format!("Q{:02}", i + 1),
                answered: reason.is_empty(),
                answer,
                reason,
            }
        })
        .collect();
    let answered = questions.iter().filter(|q| q.answered).count();
    let groups = GROUPS
        .iter()
        .map(|&(model, cutoff, start, end)| Group {
            model,
            cutoff,
            answered: questions[start - 1..end]
                .iter()
                .filter(|q| q.answered)
                .count(),
            total: end - start + 1,
        })
        .collect();
    Result {
        effort: effort.into(),
        verdict: if !error.is_empty() {
            "unknown"
        } else if answered == 25 {
            "healthy"
        } else {
            "degraded"
        },
        answered,
        total: 25,
        questions,
        groups,
        text,
        error,
        elapsed_ms,
    }
}

pub async fn check(app: &App, m: &Monitor, use_system_proxy: bool, effort: &str) -> Result {
    let started = Instant::now();
    let outcome = async {
        let mut root = m.base_url.trim_end_matches('/').to_owned();
        for suffix in ["/chat/completions", "/responses"] {
            if root.ends_with(suffix) {
                root.truncate(root.len() - suffix.len());
            }
        }
        if !root.ends_with("/v1") {
            root.push_str("/v1");
        }
        let response = upstream::control_request(
            &app.upstream_client(use_system_proxy),
            reqwest::Method::POST,
            &format!("{root}/chat/completions"),
        )
        .bearer_auth(&m.key)
        .json(
            &json!({"model":m.model,"messages":[{"role":"user","content":PROMPT}],
            "stream":false,"thinking":{"type":"enabled"},"reasoning_effort":effort}),
        )
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                "检测请求超时（600秒）"
            } else {
                "检测请求失败"
            }
            .to_owned()
        })?;
        let value = upstream::bounded_json(response).await?;
        let choice = &value["choices"][0];
        let text = choice["message"]["content"]
            .as_str()
            .ok_or("检测响应缺少答案字段")?
            .to_owned();
        let error = match choice["finish_reason"].as_str() {
            Some("length") => "检测答案被截断",
            Some("content_filter") => "检测答案被上游过滤",
            Some("tool_calls" | "function_call") => "上游请求调用工具，未完成内部知识测试",
            _ => "",
        };
        Ok::<_, String>((text, error.to_owned()))
    }
    .await;
    let (text, error) = outcome.unwrap_or_else(|error| (String::new(), error));
    assess(text, effort, error, started.elapsed().as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn complete() -> String {
        (1..=25)
            .map(|i| {
                format!(
                    "Q{i:02}｜{}",
                    if i % 2 == 0 {
                        "English response"
                    } else {
                        "中文回答"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
    #[test]
    fn accepts_coverage_without_checking_facts_or_language() {
        let result = assess(complete(), "medium", String::new(), 0);
        assert_eq!(result.verdict, "healthy");
        assert_eq!(result.answered, 25);
        assert!(result.groups.iter().all(|g| g.answered == g.total));
        assert!(PROMPT.contains("Q25｜"));
        assert_eq!(PROMPT.lines().filter(|l| l.starts_with("—— ")).count(), 7);
    }
    #[test]
    fn rejects_unknown_empty_placeholders_and_missing_independently() {
        for invalid in [
            "不清楚",
            "我不知道",
            "不确定",
            "I don't know",
            "Unknown",
            "N/A",
            "—",
            "---",
            "...",
            "",
            "答案",
        ] {
            let text = complete().replace("Q24｜English response", &format!("Q24｜{invalid}"));
            let result = assess(text, "high", String::new(), 0);
            assert_eq!(result.verdict, "degraded", "{invalid}");
            assert_eq!(result.answered, 24, "{invalid}");
            assert!(!result.questions[23].answered);
            assert!(result.questions[24].answered);
        }
        let result = assess(
            complete().lines().take(24).collect::<Vec<_>>().join("\n"),
            "high",
            String::new(),
            0,
        );
        assert_eq!(result.answered, 24);
        assert!(!result.questions[24].answered);
        assert_eq!(
            assess(String::new(), "medium", String::new(), 0).verdict,
            "degraded"
        );
    }
    #[test]
    fn accepts_punctuation_but_rejects_duplicates_and_does_not_use_headings() {
        let text = complete().replace("Q01｜中文回答", "**Q01**｜中文回答");
        // Markdown around the identifier does not change coverage.
        assert_eq!(assess(text, "high", String::new(), 0).answered, 25);
        let text = format!("{}\nQ01｜不清楚", complete());
        assert_eq!(assess(text, "high", String::new(), 0).answered, 24);
        assert_eq!(answer_line("\"Q25 | 某位车手\""), Some((25, "某位车手")));
        assert_eq!(answer_line("Q01｜—"), Some((1, "—")));
        assert_eq!(answer_line("Q01"), Some((1, "")));
        assert!(answer_line("—— GPT-4o｜Knowledge cutoff: 2023-10-01 ——").is_none());
    }
    #[test]
    fn incomplete_transport_is_unknown_even_when_all_answers_are_present() {
        let result = assess(complete(), "xhigh", "检测答案被截断".into(), 100);
        assert_eq!(result.verdict, "unknown");
        assert_eq!(result.answered, 25);
    }
}
