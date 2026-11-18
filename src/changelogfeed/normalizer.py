"""单一职责：字段规整/规范化，并识别 feed 拉取意图（确认链的第一棒）。

旧输入（无 feed_url 的本地记录）行为与历史完全一致；
带 feed_url 的 release/changelog/advisory 记录被显式打上 ``_feed_intent``，
后续阶段只认 feed_pull 确认过的内容，临时/失败内容不会进入版本解析。
"""
_FEED_KINDS = {"release", "changelog", "advisory"}


def process(record):
    rec = dict(record)
    rec["src"] = str(rec.get("src", "")).strip().lower()
    rec["name"] = str(rec.get("name", "")).strip()
    rec["value"] = int(rec.get("value", 0))

    url = str(rec.get("feed_url", "")).strip()
    kind = str(rec.get("kind", "")).strip().lower()
    if url and kind in _FEED_KINDS:
        rec["_feed_intent"] = {
            "kind": kind,
            "url": url,
            "source": rec["src"] or rec["name"] or kind,
            "ttl": rec.get("ttl"),
        }
    return rec
