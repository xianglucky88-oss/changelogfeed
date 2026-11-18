"""端到端：确认链在 normalizer/feed_pull/semver/dedup/relate 间的衔接，
失败分类不产生下游写入，重试不制造重复事件，旧输入完全兼容。"""
import json

import pytest

from src.changelogfeed import (
    config, dedup, feed_pull, normalizer, pipeline, semver, store, relate)
from src.changelogfeed.transport import TransportError
from tests.offline_mock import make_transport

BASE = "https://feed.test/v1"
URLS = {
    "release": BASE + "/release",
    "changelog": BASE + "/changelog",
    "advisory": BASE + "/advisory",
}


def _entries(ids_versions):
    return json.dumps(
        {"entries": [{"id": i, "version": v} for i, v in ids_versions]}
    ).encode()


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("CHANGELOGFEED_DB", str(db))
    monkeypatch.setenv("CHANGELOGFEED_CACHE", str(cache_dir))
    # 模块已在收集期导入，直接补丁运行时路径常量
    monkeypatch.setattr(store, "DB_PATH", db)
    monkeypatch.setattr(config, "DB_PATH", db)
    monkeypatch.setattr(config, "CACHE_DIR", cache_dir)
    con = store.connect()
    dedup.reset_session(con=con, clock=lambda: 1000.0)
    yield {"con": con, "cache_dir": cache_dir, "db": db}
    feed_pull.reset_session(None)
    dedup.reset_session(None)
    con.close()


def _install_puller(routes, cache_dir=None, **kw):
    tr, handler = make_transport(routes)
    from src.changelogfeed.feedcache import FeedCache
    # cache_dir=None 时 FeedCache 读已被 fixture 补丁的 config.CACHE_DIR
    puller = feed_pull.FeedPuller(
        tr, FeedCache(cache_dir), sleep=lambda s: None,
        clock=lambda: 1000.0, **kw)
    feed_pull.reset_session(puller)
    return handler


def _feed_record(kind, src="alpha", ttl=0):
    # 先过 normalizer，与真实流水线一致地生成 _feed_intent
    return normalizer.process({
        "id": f"rec-{kind}", "src": src, "name": "Demo",
        "value": 1, "kind": kind, "feed_url": URLS[kind], "ttl": ttl})

# --------------------------------------------------------------------------
# 旧输入兼容：没有 feed_url 的记录走原路径，阶段标记保持 naive/ok
# --------------------------------------------------------------------------
def test_legacy_records_pass_through_unchanged(isolated):
    from src.changelogfeed import normalizer
    raw = {"id": "r-001", "src": " Alpha ", "name": " N ", "value": "5"}
    rec = normalizer.process(raw)
    assert rec["src"] == "alpha" and rec["value"] == 5
    assert "_feed_intent" not in rec
    out = feed_pull.process(rec)
    assert out["_stage_feed_pull"] == "naive"
    assert semver.process(out)["_stage_semver"] == "naive"
    assert dedup.process(out)["_stage_dedup"] == "ok"
    assert relate.process(out)["_stage_relate"] == "ok"


# --------------------------------------------------------------------------
# 确认链：只有 confirmed 记录被 semver/dedup/relate 消费
# --------------------------------------------------------------------------
def test_confirmed_chain_flows_through_all_stages(isolated):
    _install_puller({
        URLS["release"]: [(200, {"etag": '"e"'},
                           _entries([("v1", "1.0.0"), ("v2", "2.0.0")]))],
    })
    rec = feed_pull.process(_feed_record("release"))
    assert rec["_feed_confirmed"] and rec["_feed_status"] == "fresh"
    rec = semver.process(rec)
    assert rec["_stage_semver"] == "ok"
    assert [e["semver"] for e in rec["_feed_events"]] == \
        [(1, 0, 0), (2, 0, 0)]
    rec = dedup.process(rec)
    assert rec["_feed_new_count"] == 2 and rec["_feed_dup_count"] == 0
    rec = relate.process(rec)
    assert all(e["relation"].startswith("release:")
               for e in rec["_feed_events"])
    # 账本落了两个事件
    assert store.count_events(isolated["con"]) == 2


def test_failure_branches_are_skipped_downstream_and_write_nothing(isolated):
    _install_puller({URLS["release"]: [TransportError("offline")]},
                    max_retries=1)
    rec = feed_pull.process(_feed_record("release"))
    assert rec["_feed_status"] == "unavailable"
    assert not rec["_feed_confirmed"] and "_feed_events" not in rec
    rec = semver.process(rec)
    assert rec["_stage_semver"] == "skipped:unavailable"
    rec = dedup.process(rec)
    assert rec["_stage_dedup"] == "skipped:unavailable"
    rec = relate.process(rec)
    assert rec["_stage_relate"] == "skipped:unavailable"
    assert store.count_events(isolated["con"]) == 0  # 零下游写入


def test_corrupt_branch_is_distinct_and_diagnosable(isolated):
    from src.changelogfeed.feedcache import sha256_hex
    bad = "sha-256=" + sha256_hex(b"nope")
    _install_puller({URLS["changelog"]: [
        (200, {"digest": bad}, _entries([("x", "1.0.0")]))]})
    rec = feed_pull.process(_feed_record("changelog"))
    assert rec["_feed_status"] == "corrupt"
    assert rec["_feed_meta"]["detail"] == "content digest mismatch"
    assert not rec["_feed_confirmed"]


def test_withdrawn_branch_skips_downstream_then_recovers(isolated):
    routes = {URLS["advisory"]: [
        (410, {}, b"gone"),
        (200, {}, _entries([("adv-1", "1.2.3")])),
    ]}
    handler = _install_puller(routes, isolated["cache_dir"])
    rec1 = feed_pull.process(_feed_record("advisory"))
    assert rec1["_feed_status"] == "withdrawn" and rec1["_feed_withdrawn"]
    assert relate.process(dedup.process(semver.process(rec1)))[
        "_stage_relate"] == "skipped:withdrawn"
    assert store.count_events(isolated["con"]) == 0
    # 页面恢复：recovered 事件正常进入下游
    rec2 = feed_pull.process(_feed_record("advisory"))
    assert rec2["_feed_status"] == "recovered" and rec2["_feed_confirmed"]
    rec2 = relate.process(dedup.process(semver.process(rec2)))
    assert store.count_events(isolated["con"]) == 1
    ev = rec2["_feed_events"][0]
    assert ev["relation"] == "advisory:alpha"
    assert ev["semver"] == (1, 2, 3)
    assert handler.call_count == 2


# --------------------------------------------------------------------------
# 重试/重跑不制造重复事件：账本幂等 + 缓存命中零请求
# --------------------------------------------------------------------------
def test_repeated_pulls_do_not_duplicate_events(isolated):
    handler = _install_puller({
        URLS["release"]: [
            (200, {"etag": '"e"'}, _entries([("v1", "1.0.0")])),
            (304, {}, b""),
        ],
    })
    r1 = relate.process(dedup.process(semver.process(
        feed_pull.process(_feed_record("release", ttl=100)))))
    assert r1["_feed_new_count"] == 1
    # 第二跑：TTL 内缓存命中，完全不发请求，事件已在账本
    r2 = relate.process(dedup.process(semver.process(
        feed_pull.process(_feed_record("release", ttl=100)))))
    assert r2["_feed_status"] == "cache_hit"
    assert r2["_feed_new_count"] == 0 and r2["_feed_dup_count"] == 1
    assert handler.call_count == 1
    assert store.count_events(isolated["con"]) == 1
    # 第三跑：强制过期 -> 304 再确认，账本仍无重复
    r3 = feed_pull.process(_feed_record("release", ttl=0))
    assert r3["_feed_status"] == "stale" and handler.call_count == 2
    assert store.count_events(isolated["con"]) == 1


def test_partial_success_then_resume_writes_once_per_event(isolated):
    page2 = URLS["release"] + "?cursor=p2"
    routes = {
        URLS["release"]: [
            (200, {"link": f'<{page2}>; rel="next"'},
             _entries([("e1", "1.0.0")])),
        ],
        page2: [
            TransportError("broken pipe"),
            (200, {}, _entries([("e2", "2.0.0")])),
        ],
    }
    handler = _install_puller(routes, isolated["cache_dir"], max_retries=1)
    o1 = feed_pull.process(_feed_record("release"))
    assert o1["_feed_status"] == "unavailable"
    assert store.count_events(isolated["con"]) == 0  # 未确认 => 零写入
    o2 = feed_pull.process(_feed_record("release"))
    assert o2["_feed_status"] == "fresh" and o2["_feed_meta"].get("resumed")
    full = relate.process(dedup.process(semver.process(o2)))
    assert full["_feed_new_count"] == 2
    assert store.count_events(isolated["con"]) == 2
    # base URL 全程只请求一次，续拉边界正确
    assert handler.urls().count(URLS["release"]) == 1
    # 再跑一次整批确认：304 再验证 + 账本去重，无重复事件
    routes[URLS["release"]].append((304, {}, b""))
    o3 = feed_pull.process(_feed_record("release"))
    assert o3["_feed_status"] == "stale"
    again = dedup.process(semver.process(o3))
    assert again["_feed_new_count"] == 0 and again["_feed_dup_count"] == 2
    assert store.count_events(isolated["con"]) == 2


# --------------------------------------------------------------------------
# 完整 pipeline.run：编排层不加业务判断，失败记录仍落库但不产生事件
# --------------------------------------------------------------------------
def test_pipeline_run_with_mixed_feed_records(isolated, monkeypatch):
    _install_puller({
        URLS["release"]: [
            (200, {}, _entries([("v1", "1.0.0"), ("v2", "2.0.0")]))],
        URLS["advisory"]: [TransportError("offline")],
    }, max_retries=1)
    records = [
        {"id": "legacy-1", "src": "x", "name": "L", "value": 1},
        _feed_record("release"),
        _feed_record("advisory"),
    ]
    monkeypatch.setattr(
        "src.changelogfeed.loader.load", lambda: records)
    stats = pipeline.run(con=isolated["con"])
    assert stats["fed"] == 3
    rows = dict(store.all_records(isolated["con"]))
    rel = rows["rec-release"]
    assert rel["_feed_confirmed"] and rel["_stage_semver"] == "ok"
    adv = rows["rec-advisory"]
    assert adv["_feed_status"] == "unavailable"
    assert adv["_stage_semver"] == "skipped:unavailable"
    assert store.count_events(isolated["con"]) == 2  # 仅确认事件入账
