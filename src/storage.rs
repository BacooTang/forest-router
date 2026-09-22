use serde::Serialize;
use std::{io::Write, path::Path};
/// Serialize writes through the caller; tmp and target stay on the same filesystem.
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| "序列化失败")?;
    let temp = path.with_extension("json.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp).map_err(|_| "无法写入临时文件")?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| "写盘失败")?;
    std::fs::rename(&temp, path).map_err(|_| "原子替换失败")?;
    #[cfg(unix)]
    {
        std::fs::File::open(path.parent().unwrap_or_else(|| Path::new(".")))
            .and_then(|d| d.sync_all())
            .map_err(|_| "目录同步失败，配置可能已替换")?;
    }
    Ok(())
}

pub fn read_state(path: &Path) -> Result<crate::state::State, String> {
    match std::fs::metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(_) => return Err("读取状态文件元信息失败".into()),
        Ok(m) if m.len() > 8 * 1024 * 1024 => return Err("状态文件超过8MiB".into()),
        _ => {}
    }
    let raw = std::fs::read(path).map_err(|_| "状态文件读取失败")?;
    serde_json::from_slice(&raw).map_err(|_| "状态文件不是有效JSON或结构损坏".into())
}
