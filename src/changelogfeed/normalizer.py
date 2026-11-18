"""单一职责：字段规整/规范化（对记录做单一职责变换：normalizer）。规范阶段。

通用基线字段（src/name/value）保持历史行为；发布域字段仅做字符串规整，
保留空串等“镜像源缺字段”信号，交由 dedup 阶段识别补登记。
"""
def process(record):
    rec = dict(record)
    rec["src"] = str(rec.get("src", "")).strip().lower()
    rec["name"] = str(rec.get("name", "")).strip()
    if rec.get("value") is not None:
        rec["value"] = int(rec.get("value", 0))
    for f in ("id", "component", "rid", "url", "version", "event", "ts"):
        if f in rec and rec[f] is not None:
            rec[f] = str(rec[f]).strip()
    # 标题/正文保留原文用于展示，另给去重比较用的规整形式（去空白+小写），
    # 镜像源排版/大小写差异不应产生新的发布身份。
    for f in ("title", "body"):
        if f in rec and rec[f] is not None:
            text = str(rec[f]).strip()
            rec[f] = text
            rec[f + "_norm"] = " ".join(text.lower().split())
    if "ranges" in rec and isinstance(rec["ranges"], str):
        rec["ranges"] = [rec["ranges"]]
    return rec
