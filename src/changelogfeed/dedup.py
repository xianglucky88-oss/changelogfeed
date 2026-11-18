"""单一职责：去重。为确认事件派生稳定键并过事件账本，重试不制造重复。

确认链第三棒：只处理 feed_pull 已确认、semver 已解析的事件；
失败/撤回记录没有事件，直接标注跳过。账本是跨运行的 sqlite 表，
同一事件无论拉取/重试多少次，下游写入只发生一次。
"""
import hashlib
import time

from . import config, store

_SESSION_CON = None
_SESSION_CLOCK = time.time


def reset_session(con=None, clock=time.time):
    """测试钩子：注入临时 DB 连接与可控时钟。"""
    global _SESSION_CON, _SESSION_CLOCK
    _SESSION_CON = con
    _SESSION_CLOCK = clock


def _con():
    global _SESSION_CON
    if _SESSION_CON is None:
        _SESSION_CON = store.connect()
    return _SESSION_CON


def event_key(ev):
    basis = f"{ev.get('source','')}|{ev.get('kind','')}|{ev.get('eid','')}"
    return "ev-" + hashlib.sha256(
        basis.encode("utf-8")).hexdigest()[:16]


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_dedup", "ok")
    if "_feed_intent" not in rec:
        return rec
    if not rec.get("_feed_confirmed"):
        rec["_stage_dedup"] = "skipped:" + str(rec.get("_feed_status", "?"))
        return rec
    con = _con()
    now = _SESSION_CLOCK()
    unique, dup = [], 0
    for ev in rec.get("_feed_events", []):
        ev = dict(ev)
        ekey = event_key(ev)
        ev["event_key"] = ekey
        if store.record_event(con, ekey, ev.get("source", ""),
                              ev.get("kind", ""), now):
            unique.append(ev)
        else:
            dup += 1
    store.commit(con)
    rec["_feed_events"] = unique
    rec["_feed_new_count"] = len(unique)
    rec["_feed_dup_count"] = dup
    rec["_stage_dedup"] = "ok"
    return rec
