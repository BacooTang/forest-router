//! Bounded passive SSE observer. Never rewrites or repairs forwarded bytes.
//! The top-level `type` scanner skips large JSON values without retaining them.
#[derive(Default)]
struct TypeScanner {
    depth: usize,
    string: bool,
    escape: bool,
    token: Vec<u8>,
    want_key: bool,
    key: String,
    value: Option<String>,
}
impl TypeScanner {
    fn byte(&mut self, b: u8) {
        if self.string {
            if self.escape {
                self.escape = false;
                if self.token.len() < 128 {
                    self.token.push(b);
                }
                return;
            }
            if b == b'\\' {
                self.escape = true;
                if self.token.len() < 128 {
                    self.token.push(b);
                }
                return;
            }
            if b == b'"' {
                self.string = false;
                if self.depth == 1 {
                    if self.want_key {
                        self.key = String::from_utf8_lossy(&self.token).into_owned();
                        self.want_key = false;
                    } else if self.key == "type" {
                        self.value = Some(String::from_utf8_lossy(&self.token).into_owned());
                    }
                }
                self.token.clear();
            } else if self.depth == 1 && self.token.len() < 128 {
                self.token.push(b);
            }
            return;
        }
        match b {
            b'"' => {
                self.string = true;
                self.token.clear();
            }
            b'{' | b'[' => {
                self.depth = self.depth.saturating_add(1);
                if self.depth == 1 && b == b'{' {
                    self.want_key = true;
                }
            }
            b'}' | b']' => self.depth = self.depth.saturating_sub(1),
            b',' if self.depth == 1 => self.want_key = true,
            _ => {}
        }
    }
}
#[derive(Default)]
pub struct Observer {
    line: Vec<u8>,
    data: Vec<u8>,
    event: String,
    overflow: bool,
    line_size: usize,
    data_line: bool,
    previous_cr: bool,
    scanner: TypeScanner,
    usage_scanner: crate::usage::Scanner,
    pub usage: Option<crate::usage::Tokens>,
    pub terminal: bool,
    pub failed: bool,
    pub failure_status: Option<u16>,
    pub meaningful: bool,
    pub output_text: bool,
    /// Set on the first error frame, based on preceding frames, not TCP chunks.
    pub retryable_failure: bool,
}
impl Observer {
    fn kind(&mut self, kind: &str) {
        if kind == "response.output_text.delta" {
            self.output_text = true;
        }
        match kind {
            "response.completed" | "response.incomplete" | "response.done" | "done" => {
                self.terminal = true;
                self.meaningful = true;
            }
            "response.failed" | "error" => {
                if !self.failed {
                    self.retryable_failure = !self.meaningful;
                }
                self.failed = true;
                self.terminal = true;
            }
            "response.created" | "response.in_progress" | "" => {}
            _ => self.meaningful = true,
        }
    }
    fn frame(&mut self) {
        let event = std::mem::take(&mut self.event);
        let kind = self.scanner.value.take().unwrap_or_default();
        if self.data.trim_ascii() == b"[DONE]" {
            self.terminal = true;
            self.meaningful = true;
        }
        self.kind(&event);
        self.kind(&kind);
        if !self.overflow
            && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&self.data)
        {
            self.kind(v["type"].as_str().unwrap_or(""));
            if self.failed {
                self.failure_status = Some(crate::errors::classify(503, &self.data));
            }
        }
        if let Some(t) = self.usage_scanner.found.take() {
            self.usage = Some(t);
        }
        self.usage_scanner = crate::usage::Scanner::default();
        self.scanner = TypeScanner::default();
        self.data.clear();
        self.overflow = false;
    }
    fn end_line(&mut self) {
        if self.line_size == 0 {
            self.frame();
        } else {
            if let Some(value) = self.line.strip_prefix(b"event:") {
                self.event = String::from_utf8_lossy(value)
                    .trim()
                    .chars()
                    .take(128)
                    .collect();
            }
            if self.data_line {
                self.scanner.byte(b'\n');
                self.usage_scanner.byte(b'\n');
                if self.line_size <= 65536 && self.data.len() + self.line.len() < 65536 {
                    self.data.extend_from_slice(&self.line[5..]);
                    self.data.push(b'\n');
                } else {
                    self.overflow = true;
                }
            }
        }
        self.line.clear();
        self.line_size = 0;
        self.data_line = false;
    }
    pub fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            if b == b'\n' && self.previous_cr {
                self.previous_cr = false;
                continue;
            }
            self.previous_cr = b == b'\r';
            if b == b'\r' || b == b'\n' {
                self.end_line();
                continue;
            }
            self.line_size = self.line_size.saturating_add(1);
            if self.line.len() < 65536 {
                self.line.push(b);
            }
            if self.line_size == 5 {
                self.data_line = self.line == b"data:";
            }
            if self.data_line && self.line_size > 5 {
                self.scanner.byte(b);
                self.usage_scanner.byte(b);
            }
        }
    }
    pub fn finish(&mut self) {
        if self.line_size > 0 {
            self.end_line();
        }
        if !self.data.is_empty() || !self.event.is_empty() || self.overflow {
            self.frame();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_frames() {
        let mut o = Observer::default();
        for b in b"data: {\"type\":\"response.completed\"}\n\n" {
            o.feed(&[*b]);
        }
        assert!(o.terminal);
        assert!(!o.failed);
    }
    #[test]
    fn bounds() {
        let mut o = Observer::default();
        o.feed(&vec![b'x'; 300000]);
        assert!(o.line.len() <= 65536);
    }
    #[test]
    fn large_data_only_terminal() {
        for last in [false, true] {
            let mut o = Observer::default();
            let mut v=serde_json::json!({"response":{"pad":"x".repeat(100000)},"type":"response.completed"}).to_string();
            if !last {
                v = format!(
                    "{{\"type\":\"response.completed\",\"response\":{{\"pad\":\"{}\"}}}}",
                    "x".repeat(100000)
                );
            }
            for b in format!("data: {v}\n\n").as_bytes().chunks(111) {
                o.feed(b);
            }
            assert!(o.terminal);
            assert!(!o.failed);
        }
    }
    #[test]
    fn multiline_and_cr() {
        for newline in ["\r\n", "\r", "\n"] {
            let mut o = Observer::default();
            o.feed(
                format!("data: {{{newline}data: \"type\":\"response.failed\"}}{newline}{newline}")
                    .as_bytes(),
            );
            assert!(o.failed);
        }
    }
    #[test]
    fn aliases() {
        for text in [
            "data: [DONE]\n\n",
            "data: {\"type\":\"response.done\"}\n\n",
            "event: done\n\n",
        ] {
            let mut o = Observer::default();
            o.feed(text.as_bytes());
            assert!(o.terminal);
        }
    }
    #[test]
    fn frame_order_not_chunks() {
        let v=b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.failed\"}\n\n";
        for size in [1, 7, 10000] {
            let mut o = Observer::default();
            for chunk in v.chunks(size) {
                o.feed(chunk);
            }
            assert!(o.failed);
            assert!(!o.retryable_failure);
        }
    }
    #[test]
    fn nested_type_does_not_spoof_terminal() {
        let mut o = Observer::default();
        o.feed(b"data: {\"type\":\"response.output_text.delta\",\"nested\":{\"type\":\"response.completed\"}}\n\n");
        assert!(!o.terminal);
    }
}
