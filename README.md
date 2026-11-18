# changelogfeed — 订阅多源 release/changelog/advisory，语义版本归一，关联本地依赖做变更预警。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线和幂等性检查
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`advisory_map`, `subscribe`, `semver`, `feed_pull`, `dedup`, `relate`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`。

## 模块说明
业务模块按独立阶段组织，通用模块负责加载、规范化、主键、存储和编排。
流水线按配置顺序调用处理阶段，输入输出均遵循项目定义的记录结构。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。

## 订阅状态模型（subscribe 阶段）
`subscribe.py` 在事件库同一 SQLite 连接内维护三张表，把**订阅配置版本、来源游标、已投递事件**一起持久化：

| 表 | 粒度 | 关键内容 |
| --- | --- | --- |
| `subscriptions` | 每来源一行 | `config_ver`（过滤条件版本）、`filters`(JSON)、`status`(active/paused/invalid)、`cursor_ts`+`cursor_id`（已处理边界，位置按 `(ts,id)` 字典序）、`backfill_limit`、`catchup_pending`、`backfill_done` |
| `deliveries` | 每来源每事件一行 | 通知记录，主键 `(src,event_id)`，`INSERT OR IGNORE` 实现每事件最多通知一次 |
| `sub_audit` | 只增审计 | 状态变化、配置变化、回补/恢复跳过、窗口边界，让每次游标推进都可解释 |

边界语义（明确选择，不靠隐式行为）：

- **首次回补多深**：显式注册（`upsert_subscription` 先于拉取）的订阅只回补最近 `backfill_limit` 条，更老事件不投递、游标先推到窗口起点并写 `backfill_skip`；边界在处理事件前算好并随事务落库，半批重启不会二次截断。流水线中自动建立的订阅首批全量投递（首批即订阅时刻的“现在”）。
- **条件变化后重看哪些区间**：不回看历史。`update_filters` 只让 `config_ver +1`，新条件仅对游标之后的新事件生效；通知行带版本号，可区分每条报警对应哪一版过滤条件。
- **暂停/恢复**：暂停期事件照常入库、不投递、游标冻结；恢复后从冻结点无界补看，漏掉的匹配事件逐条补通知，`deliveries` 主键保证已投事件不重报。
- **来源失效/恢复**：`feed_pull` 健康表判定来源不可用即拦下事件并把订阅置 `invalid`、游标不动；恢复置 `catchup_pending`（落库），下轮仅在游标之后按 `backfill_limit` 取有限窗口，窗口外历史写 `recovery_skip` 后跳过。两个来源独立失效/恢复、互不影响。
- **半批提交**：每批把“事件入库 + 通知 + 游标推进”放在同一事务；崩溃后未提交批次整体重做，已提交部分靠 `deliveries` 去重，做到不漏、不重、无不可解释的游标跳跃。

测试见 `tests/test_subscribe.py`：覆盖有限首次回补、暂停/恢复补报、过滤条件版本化、双源独立失效/恢复、半批崩溃重启，以及事件库 × 通知记录的交叉核对（每个未通知事件都能在审计里找到解释）。
