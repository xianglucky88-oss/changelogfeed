"""薄编排：loader → normalizer→feed_pull→semver→dedup→relate→subscribe → keyer → store。

不含业务判断：subscribe 阶段的所有状态/游标/投递语义都在 ``subscribe.py``，
本模块只负责把同一连接、同一引擎按顺序接到现有拉取与落库流程上，并保证
“事件入库 + 通知 + 游标推进”按批次共用一个事务提交。
"""
import importlib

from . import store as _store
from . import loader as _loader
from . import subscribe as _subscribe
from .keyer import key_of
from .models import Record

# subscribe 接在 relate 之后、keyer/store 之前：此时记录已规范化、去重、关联。
CHAIN_NAMES = ['normalizer', 'feed_pull', 'semver', 'dedup', 'relate', 'subscribe']
_PRE_CHAIN = ['normalizer', 'feed_pull', 'semver', 'dedup', 'relate']


def run(con=None, engine=None, batch_size=50, run_id="run"):
    own = con is None
    con = con or _store.connect()
    raws = list(_loader.load())
    pre_chain = [importlib.import_module("." + n, __package__).process
                 for n in _PRE_CHAIN]
    own_engine = engine is None
    if own_engine:
        engine = _subscribe.SubscriptionEngine(
            con, batch_size=batch_size, run_id=run_id)

    # 第一遍：跑拉取及前置阶段，按来源汇总本轮可用事件，识别失效来源。
    pulled: dict[str, list[dict]] = {}
    down_sources: set[str] = set()
    staged: list[tuple[dict | None, str]] = []
    for raw in raws:
        rec = raw
        for fn in pre_chain:
            rec = fn(rec)
            if rec is None:
                # feed_pull 判定来源失效：本轮回放拉不到，后续阶段不再处理。
                break
        src = str(raw.get("src", "")).strip().lower()
        if rec is None:
            if src:
                down_sources.add(src)
            staged.append((None, src))
            continue
        pulled.setdefault(src, []).append(rec)
        staged.append((rec, src))

    # 来源失效/恢复通知：本轮拉不到的来源置 invalid；已知且本轮恢复的置恢复。
    known = {r[0] for r in con.execute("SELECT src FROM subscriptions")}
    for src in down_sources:
        engine.note_source_down(src)
    for src in known & set(pulled) - down_sources:
        engine.note_source_up(src)

    # 第二遍前：确定首次回补/恢复回补的有限窗口（游标先推到窗口起点）。
    engine.prepare_sources(pulled)

    # 第二遍：subscribe 状态推进 + 幂等落库，按批次共用事务提交。
    token = _subscribe.bind_engine(engine)
    fed = 0
    try:
        for rec, _src in staged:
            if rec is None:
                continue
            rec = engine.handle(rec)
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        # finalize 提交最后一批；事件 upsert 与通知/游标在同一事务内。
        stats = engine.finalize()
    finally:
        _subscribe.reset_engine(token)
    rows = _store.count(con)
    if own:
        con.close()
    return {"fed": fed, "rows": rows, "sub_stats": stats}
