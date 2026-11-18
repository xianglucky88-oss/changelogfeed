"""单一职责：对记录做单一职责变换：relate。（作为流水线阶段）

消费 ``advisory_map`` 的公告影响语义，为本地依赖记录附加匹配判定；
关联流程本身由 advisory_map 负责，这里只做消费，不另造一套变更条目关联流程。
"""
from . import advisory_map


def process(record, con=None):
    rec = dict(record)
    rec.setdefault("_stage_relate", "ok")
    if con is not None and "ecosystem" in rec and "package" in rec:
        matches = advisory_map.evaluate_dependency(con, rec)
        rec["_advisory_matches"] = [
            {"advisory_key": m["advisory"]["advisory_key"],
             "verdict": m["verdict"],
             "match_key": m["match_key"]}
            for m in matches
        ]
    return rec
