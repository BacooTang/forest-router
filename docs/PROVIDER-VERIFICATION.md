# 额度适配与验证边界

- New API：识别 `/api/usage/token/` 兼容结构。无限Token不代表账户钱包充足，负额度不直接判定耗尽。
- Sub2API：识别 `/v1/usage` 兼容结构，同时考虑钱包和Key限额。
- 订阅：`/v1/subscriptions` 返回周额度、重置和到期信息；不能与按量钱包混用。
- GLM Coding Plan：解析5小时/周额度，percentage表示已使用比例。
- DeepSeek：解析官方余额接口的is_available和balance_infos。

HTTP 200但返回HTML不代表平台识别成功。接口结构兼容不保证供应商使用未经修改的开源版本。公开仓库不收录账号、实际余额、客户归属或真实调用结果。真实验证脚本需显式提供FOREST_PROVIDER_CONFIG及执行授权；日常回归只用本地假上游。
