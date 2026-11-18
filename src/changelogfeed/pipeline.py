"""薄编排：loader → normalizer→feed_pull→semver→dedup→relate → keyer → store。不含业务判断。"""
from . import store as _store
from . import loader as _loader
from . import normalizer, feed_pull, semver, dedup as _dedup, relate
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'feed_pull', 'semver', 'dedup', 'relate']
# dedup 之前的纯变换阶段（输入/输出均为 dict）
_PRE = [normalizer.process, feed_pull.process, semver.process]


def run(con=None, fixture=None):
    own = con is None
    con = con or _store.connect()
    try:
        # 显式引用同一批模块对象，避免动态导入产生第二份模块实例，
        # 导致 dedup.bind() 绑定的状态对流水线阶段不可见。
        # dedup 的持久化状态与 records 表共用同一连接/事务，
        # 随最终 commit 一起落盘；异常时随 rollback 整体撤销（半写入可恢复）。
        _dedup.bind(con)
        fed = written = suppressed = triggered = 0
        try:
            for raw in _loader.load(fixture):
                fed += 1
                rec = raw
                for fn in _PRE:
                    rec = fn(rec)
                rec, did_trigger = _dedup.process(rec)
                if rec is None:        # 原样重传：跳过全部下游
                    suppressed += 1
                    continue
                rec = relate.process(rec)
                _store.upsert(con, Record(key=key_of(rec), data=rec))
                written += 1           # 写入/刷新归并记录（按稳定主键幂等）
                if did_trigger:
                    triggered += 1     # 首见/修订/撤回：真正的下游触发
            _store.commit(con)
        except BaseException:
            con.rollback()
            raise
        return {
            "fed": fed,
            "emitted": triggered,   # 下游触发次数（重跑为 0）
            "written": written,     # 落库 upsert 次数（来源补登记也算刷新）
            "suppressed": suppressed,
            "rows": _store.count(con),
        }
    finally:
        _dedup.close()
        if own:
            con.close()
