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

### semver 版本归一化
把 `v1.2`、`1.2.3-rc1` 等写法归一为 `major.minor.patch`，版本范围统一为
`>=1.0.0` 形式并排序去重，结果写入 `version_norm` / `ranges_norm`，作为 dedup 的比较输入。

### dedup 多源去重
同一发布在官方 release、变更日志、镜像公告各出现一次，dedup 用四类身份键归并：
规范 URL（含页面改版别名与跟踪参数剥离）、发布标识 `rid`、组件+版本范围、
标题/内容摘要（镜像缺字段时以标题键兜底）。状态机区分四类事件：

- `publish` 首次发现与来源补登记（`source_add` 不触发下游）
- `revise` 同来源内容或版本修订，保留修订证据链与有序版本链
- `withdraw` 撤回留痕，撤回后重复撤回不重复触发
- 原样重传（同事件编号/同来源同指纹）直接抑制

状态持久化在与 `records` 表同一 sqlite 连接的四张表里：
`dedup_groups`（归并状态）、`dedup_aliases`（身份键映射，半写入可 heal）、
`dedup_journal`（仅追加事件流水，规则升级可按流水回放重建）、
`dedup_emitted`（下游触发台账，重跑不重复触发）。乱序到达（如撤回先于发布）
最终状态与顺序处理一致；事务回滚后重放结果与不中断处理一致。
去重后的规范记录以稳定 `group_id` 为身份，携带来源关系与修订信息交给 relate / store。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
