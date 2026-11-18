"""单一职责：语义版本解析。只处理 feed_pull 已确认的事件。

失败/撤回/损坏分支没有 ``_feed_events``，这里显式跳过并标注原因，
保证“暂时拿不到内容”绝不会被下游当成新版本更新。
"""
import re

_SEMVER = re.compile(
    r"v?(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z.-]+)?$")


def parse_version(text):
    """返回 (major, minor, patch) 或 None；无法诊断时不抛异常。"""
    if text is None:
        return None
    m = _SEMVER.match(str(text).strip())
    if not m:
        return None
    return tuple(int(g) for g in m.groups())


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_semver", "naive")
    if "_feed_intent" not in rec:
        return rec
    if not rec.get("_feed_confirmed"):
        # 明确分类：失败不产生版本解析，避免失败被误判为更新
        rec["_stage_semver"] = "skipped:" + str(rec.get("_feed_status", "?"))
        return rec
    parsed = []
    for ev in rec.get("_feed_events", []):
        ev = dict(ev)
        ev["semver"] = parse_version(ev.get("version"))
        parsed.append(ev)
    rec["_feed_events"] = parsed
    rec["_stage_semver"] = "ok"
    return rec
