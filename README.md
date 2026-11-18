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

### advisory_map：安全公告影响语义（供 relate 消费）
不另造一套变更条目关联流程，本模块只产出公告侧事实与匹配依据，`relate.process(record, con=...)` 负责消费：

- 规范包身份：生态别名归一（`pypi/npm/maven/...`）、包名大小写与 Maven 坐标规整；公告包别名经 `am_aliases` / 内联 `aliases` 解析，同名不同生态不跨生态关联。
- 受影响区间：支持 OSV 事件序列（`introduced/fixed/last_affected`）、约束串（`>=,<,=,...`）与无版本约束（无界区间）；内置语义版本比较，预发布按 semver precedence 处理临界版本。
- 证据状态：正文/撤回事件全部留痕；先收到撤回再收到正文进待判定（`am_pending`），先正文后撤回则把既有受影响判断有据更新为 `withdrawn`；多来源冲突时逐来源保留区间证据并打 `source_conflict`。
- 匹配依据：每个 `(本地依赖, 公告)` 一行 `advisory_match`，含身份解析、逐来源区间判定、结论（`affected/not_affected/withdrawn/pending`）、冲突与事件历史，稳定 `match_key` 幂等落 `am_matches`。
- 持久化：独立表 `am_advisories/am_aliases/am_pending/am_matches`，复用 sqlite 契约，重复运行不增行，重启后 `matches_for/pending_records` 结果一致。
- 旧入口 `advisory_map.process(record)` 保留：对单条公告做无 IO 的规范/解析；非公告记录透传。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
