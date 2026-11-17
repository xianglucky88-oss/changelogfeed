# changelogfeed — 订阅多源 release/changelog/advisory，语义版本归一，关联本地依赖做变更预警。

难度：**困难**　|　子方向：数据采集 / 爬虫与 RPA　|　运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 基础版代码（起点，能跑）
读 `fixtures/input.json` → `loader` → `normalizer` → 其余已实现变换 → `keyer` → 幂等落 `sqlite`。
```
make run     # 跑通 happy-path，打印 {fed, rows}
make test    # happy-path + 幂等 + 缺口桩断言
```

## 单一职责约定
每个模块 = 一个文件 = 一个聚焦函数；`pipeline.py` 只做顺序编排、不含业务判断。
业务模块：`feed_pull, semver, advisory_map, subscribe, dedup, relate` ＋ 通用 `loader / normalizer / keyer / store / pipeline / cli`。

## 故意留空（功能开发面）
- `advisory_map.py` → `process()` 抛 `NotImplementedError`（对记录做单一职责变换：advisory_map；功能开发目标，未接入 happy-path）
- `subscribe.py` → `process()` 抛 `NotImplementedError`（对记录做单一职责变换：subscribe；功能开发目标，未接入 happy-path）

## 朴素实现短板（功能优化面）
- `semver.py` → 朴素实现 + docstring 标注短板（对记录做单一职责变换：semver；功能优化目标）
- `feed_pull.py` → 朴素实现 + docstring 标注短板（对记录做单一职责变换：feed_pull；功能优化目标）

> 缺口模块刻意未接入 happy-path，以保证基线可运行；它们正是出题要补的 0→1 / 1→n 功能。
