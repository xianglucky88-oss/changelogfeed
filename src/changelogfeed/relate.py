"""单一职责：关联。把 advisory 事件关联到同源 release/changelog 事件。

确认链末棒：仅消费 dedup 后的新事件；失败记录在此之前已被各阶段跳过，
因此暂时错误/损坏/撤回不会产生任何关联写入。
"""
from .semver import parse_version


def _affected(ev, ver_tuple):
    """advisory 自带 affected 范围时做简单包含判断，否则按来源弱关联。"""
    if not ver_tuple:
        return True
    affected = ev.get("affected")
    if not isinstance(affected, list) or not affected:
        return True
    for spec in affected:
        lo = parse_version(str(spec.get("from", "0.0.0"))) or (0, 0, 0)
        hi_text = spec.get("to")
        hi = parse_version(str(hi_text)) if hi_text else None
        if lo <= ver_tuple and (hi is None or ver_tuple <= hi):
            return True
    return False


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_relate", "ok")
    if "_feed_intent" not in rec:
        return rec
    if not rec.get("_feed_confirmed"):
        rec["_stage_relate"] = "skipped:" + str(rec.get("_feed_status", "?"))
        return rec
    kind = rec["_feed_intent"]["kind"]
    events = rec.get("_feed_events", [])
    if kind == "advisory":
        for ev in events:
            ev["relation"] = "advisory:" + str(ev.get("source", ""))
            ev["affected_match"] = _affected(ev, ev.get("semver"))
    else:
        for ev in events:
            ev["relation"] = f"{kind}:{ev.get('source', '')}"
    rec["_feed_events"] = events
    rec["_stage_relate"] = "ok"
    return rec
