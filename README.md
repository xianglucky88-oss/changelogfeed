# changelogfeed — 订阅多源 release/changelog/advisory，语义版本归一，关联本地依赖做变更预警。

运行栈：Python 3 + 标准库 `sqlite3` + `httpx` + `pytest`（测试全离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线、缓存/失败分支与幂等性检查
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`advisory_map`, `subscribe`, `semver`, `feed_pull`, `dedup`, `relate`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli / transport / feedcache`。

## feed 拉取、缓存与失败恢复（feed_pull）
带 `feed_url` 且 `kind ∈ {release, changelog, advisory}` 的记录由 `normalizer`
显式生成 `_feed_intent`，随后进入条件请求链；无 `feed_url` 的旧输入行为完全不变。

### 三条互斥的顶层分支
| 分支 | 状态值 | 是否下游可见 |
| --- | --- | --- |
| 成功确认 | `fresh / cache_hit / stale / recovered / empty` | 是（带 `_feed_events`） |
| 下载损坏 | `corrupt`（摘要不符、截断空流、非法 JSON、装配摘要不符） | 否 |
| 暂时拿不到 | `unavailable / rate_limited / bad_intent / not_modified_missing` | 否 |
| 撤回 | `withdrawn`（HTTP 410 墓碑，页面恢复后为 `recovered`） | 否 |

失败分支绝不携带确认事件（旧缓存内容单独放在 `stale_events`/`_feed_stale_count`），
因此 `semver` 不可能把失败误判成版本更新；各下游阶段对未确认记录显式打
`skipped:<status>` 标记。

### 关键机制
- **条件请求**：按来源带 `If-None-Match` / `If-Modified-Since`；304 仅刷新新鲜度，
  304 但缓存缺失/损坏走独立的 `not_modified_missing` 分支。
- **分页与续拉**：未整批确认的分页只写 `stage/` 暂存区（对读取不可见）；
  断线后下次从最后游标续拉，续拉本身失败则弃用暂存、整批重来。
- **原子替换与完整性证据**：缓存替换只经“临时文件 + `os.replace`”，
  每次提交落 sha256；读取强制复核，损坏即移入 `quarantine/` 并按缓存缺失处理。
- **速率限制与退避**：429 尊重有界 `Retry-After`，5xx/传输错误指数退避，
  重试预算耗尽后分类返回，不会无限挂起。
- **重试不产生重复**：`dedup` 通过 sqlite 事件账本（稳定事件键）幂等记账，
  同一事件无论重试/重跑多少次，下游只写一次。
- **日志纪律**：只记录来源、缓存键、状态、事件/页数与短诊断摘要；
  请求体、缓存正文与 URL 查询串中的令牌不进日志、不进磁盘路径。

路径可由 `CHANGELOGFEED_CACHE` / `CHANGELOGFEED_DB` / `CHANGELOGFEED_TTL` 等环境变量覆盖。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；`events` 表为跨运行的事件账本。
离线测试用 `httpx.MockTransport` 脚本化 304/410/429/5xx、断线、截断与分页续拉，夹具位于测试目录。
