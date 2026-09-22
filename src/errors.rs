//! Classification uses error-envelope fields only, never generated answer text.
use serde_json::Value;

/// Internal classes reuse HTTP numbers: 400=request error, 401=credential,
/// 402=balance, 429=throttle, 503=service/unknown. They are not raw upstream status.
pub fn classify(status: u16, body: &[u8]) -> u16 {
    let parsed = serde_json::from_slice::<Value>(body).ok();
    let mut fields = Vec::new();
    if let Some(v) = &parsed {
        for object in [v, &v["error"], &v["response"]["error"]] {
            if object.is_string() {
                fields.push(object.as_str().unwrap().to_lowercase());
            }
            for key in ["code", "type", "message", "msg", "reason"] {
                if let Some(text) = object[key].as_str() {
                    fields.push(text.to_lowercase());
                } else if let Some(n) = object[key].as_i64() {
                    fields.push(n.to_string());
                }
            }
        }
    }
    let text = fields.join(" ");
    let has = |patterns: &[&str]| patterns.iter().any(|p| text.contains(p));
    if has(&[
        "insufficient_quota",
        "insufficient_user_quota",
        "insufficient_balance",
        "insufficient account balance",
        "insufficient balance",
        "balance is not enough",
        "余额不足",
        "额度不足",
        "额度已用尽",
        "subscription exhausted",
        "subscription_expired",
        "weekly_limit_exceeded",
        "daily_limit_exceeded",
        "monthly_limit_exceeded",
        "quota_exhausted",
    ]) {
        return 402;
    }
    if status == 401
        || has(&[
            "invalid_api_key",
            "authentication_error",
            "authentication fails",
            "invalid token",
            "invalid api key",
            "令牌已过期或验证不正确",
            "无效的apikey",
            "api_key_expired",
            "api_key_disabled",
        ])
        || parsed
            .as_ref()
            .is_some_and(|v| v["success"] == false && (v["code"] == 401 || v["code"] == "401"))
    {
        return 401;
    }
    if status == 402 {
        return 402;
    }
    if status == 429
        || has(&[
            "rate_limit_exceeded",
            "rate_limit_error",
            "too many requests",
            "concurrency_limit",
            "queue_full",
        ])
    {
        return 429;
    }
    // Unknown parameter validation formats are still caller errors, not Key outages.
    if matches!(status, 400 | 413 | 422) {
        return 400;
    }
    // Unknown 403/404/200 business errors are quarantined, not declared invalid keys.
    503
}

fn nonempty_error(e: &Value) -> bool {
    match e {
        Value::Null => false,
        Value::Object(o) => !o.is_empty(),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}
fn response_shape(v: &Value) -> bool {
    matches!(v["status"].as_str(), Some("completed" | "incomplete")) && v["output"].is_array()
}
pub fn business_error(v: &Value) -> bool {
    if v["success"] == false
        || v.get("error").is_some_and(nonempty_error)
        || v["status"]
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case("failed"))
    {
        return true;
    }
    if response_shape(v) {
        return false;
    }
    v.get("code").is_some_and(|c| match c {
        Value::Number(n) => n.as_i64().is_some_and(|n| n != 0 && n != 200),
        Value::String(s) => !matches!(
            s.to_ascii_lowercase().as_str(),
            "0" | "200" | "success" | "ok"
        ),
        _ => false,
    })
}
pub fn valid_response(v: &Value) -> bool {
    response_shape(v) && !business_error(v)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_invalid_keys() {
        for (status, raw) in [
            (
                401,
                r#"{"code":"INVALID_API_KEY","message":"Invalid API key"}"#,
            ),
            (
                401,
                r#"{"error":{"code":"","message":"Invalid token (request id: x)","type":"new_api_error"}}"#,
            ),
            (
                200,
                r#"{"code":401,"msg":"令牌已过期或验证不正确","success":false}"#,
            ),
            (
                401,
                r#"{"error":{"type":"authentication_error","code":"invalid_request_error"}}"#,
            ),
        ] {
            assert_eq!(classify(status, raw.as_bytes()), 401);
        }
    }
    #[test]
    fn scopes_and_unknowns() {
        assert_eq!(
            classify(403, br#"{"error":{"code":"insufficient_user_quota"}}"#),
            402
        );
        assert_eq!(
            classify(
                429,
                br#"{"code":"USAGE_LIMIT_EXCEEDED","message":"WEEKLY_LIMIT_EXCEEDED"}"#
            ),
            402
        );
        assert_eq!(
            classify(
                429,
                br#"{"code":"USAGE_LIMIT_EXCEEDED","message":"concurrent request limit exceeded"}"#
            ),
            429
        );
        assert_eq!(
            classify(400, br#"{"error":{"code":"context_length_exceeded"}}"#),
            400
        );
        assert_eq!(classify(200, br#"{"success":false,"code":937}"#), 503);
        assert_eq!(classify(403, b"<html>firewall</html>"), 503);
        assert_eq!(
            classify(404, br#"{"error":{"code":"model_not_found"}}"#),
            503
        );
    }
    #[test]
    fn answer_text_is_not_an_error() {
        let v = serde_json::json!({"status":"completed","output":[{"content":[{"text":"invalid_api_key insufficient_quota"}]}]});
        assert!(valid_response(&v));
        assert!(!business_error(&v));
    }
}
