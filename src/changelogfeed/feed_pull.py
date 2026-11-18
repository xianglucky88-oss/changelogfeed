"""单一职责：拉取阶段。记录来源是否可用（离线健康表，供订阅失效/恢复判断），
不可用来源的记录在本阶段被拦下（返回 None），不进入后续阶段。"""

# 离线健康表：src -> 是否可拉取。缺省视为可用（fail-open，仅在测试/显式置位时失效）。
_HEALTH: dict[str, bool] = {}


def set_source_health(src: str, available: bool) -> None:
    _HEALTH[str(src).strip().lower()] = bool(available)


def is_available(src: str) -> bool:
    return _HEALTH.get(str(src).strip().lower(), True)


def reset_health() -> None:
    _HEALTH.clear()


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_feed_pull", "naive")
    rec["_source_available"] = is_available(rec.get("src", ""))
    if not rec["_source_available"]:
        # 来源失效：本轮回放中该来源拉不到任何事件，交由编排层跳过落库。
        return None
    return rec
