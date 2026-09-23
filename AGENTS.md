# Forest Router 开发约定

## 项目与目录

Rust单实例Responses网关，Axum/Tokio/Reqwest，无数据库。
- `src/proxy.rs`：Responses透传、候选选择；`src/errors.rs` / `sse.rs`：错误与流观察。
- `src/config.rs` / `state.rs` / `storage.rs`：配置、独立Key状态、原子持久化。
- `src/admin.rs` / `static/index.html`：管理API与内嵌管理页。
- `src/scheduler.rs` / `health.rs` / `monitor.rs` / `balance.rs`：额度、恢复、质量检测。
- `scripts/check_*.py` / `check_ui.cjs`：隔离回归；`docs/`：运行规则、历史验收及审查。

## 必须保留的行为

- 仅代理Responses，不注入业务提示词、不转换协议；GET /v1/models仅返回对外模型。
- 渠道正常按优先级；故障切换后保持可用渠道，高优先级恢复观察10分钟再回切，当前不可用立即兜底。组内可用Key等量轮询；订阅也不使用加权调度。
- 提供者关闭整组跳过，关闭的Key不入队，全部Key关闭跳过提供者。开关不清空故障/额度状态。
- 质量、额度、凭证、服务状态独立。查询失败不等同额度耗尽。
- 每请求最多8候选/90秒切换预算；已提交不拼接其他渠道，不因本地资源限制重放。
- SSE响应字节不改写，观察器必须有界；兼容格式差异不得制造空HTTP响应或误封健康Key。
- 恢复探测、冷却及通知有界；变更状态前保持配置快照/版本保护。
- 配置中心显示模型侧栏，其他Tab不显示；管理页保持浅色紧凑布局。渠道拖拽排序，开关在右侧。
- 监控时间段全局配置、上海时区；默认规则见README。不要将未知、降智、过期混为成功。

## 隐私与操作范围

- API Key明文保存/管理员页面回显是明确产品要求；管理员密码必须哈希。不得将真实配置或Key提交。
- `.runtime/`、`data/`、config/state、私有测试结果及截图均属本地数据，不提交。
- 禁止在日志、报告、提交信息中输出密码、Webhook、账号余额或真实请求正文。
- 普通回归用随机端口、临时数据目录和本地假上游。真实测试需要明确授权，显式环境变量不等于用户授权。
- 不操作其他代理服务。部署、重启、推送遵守用户当前任务授权；没有要求时不自动commit/push。
- 管理页由include_str!嵌入二进制，修改HTML后需重新构建才能生效。

## 检查与运行

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --locked
cargo build --locked
python3 scripts/check_http.py
```

UI变更可用 `PLAYWRIGHT_MODULE=/path/to/node_modules/playwright node scripts/check_ui.cjs`；流/内存变更再运行发布版内存回归。真实脚本默认拒绝运行，详见README。

默认初始化监听0.0.0.0:8119；`--port 80`覆盖本次监听，PM2配置默认显式传8119。数据目录由FOREST_ROUTER_HOME指定；不要多实例共用同一目录。

## CodeGraph

若仓库根目录存在`.codegraph/`，理解/定位代码时先使用codegraph_explore或`codegraph explore`；未索引则使用rg等工具，不自行创建索引。
