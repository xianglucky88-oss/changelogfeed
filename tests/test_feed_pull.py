"""feed_pull 离线场景：缓存命中/损坏/暂时错误三分支、条件请求、分页、
速率限制、空响应、撤回恢复、断线续拉、重试不重复。"""
import json

import httpx
import pytest

from src.changelogfeed import feed_pull as fp
from src.changelogfeed.feedcache import FeedCache, sha256_hex
from src.changelogfeed.transport import TransportError
from tests.offline_mock import make_transport

BASE = "https://feed.test/v1/releases"
PAGE2 = BASE + "?cursor=p2"
# 固定时钟下默认 ttl=0：首次填充、后续一律走条件请求，便于隔离各分支
INTENT = {"kind": "release", "url": BASE, "source": "alpha", "ttl": 0}


def _puller(tmp_path, routes, *, max_retries=3, sleeps=None):
    tr, handler = make_transport(routes)
    sleeps = sleeps if sleeps is not None else []
    cache = FeedCache(tmp_path / "cache")
    puller = fp.FeedPuller(
        tr, cache, max_retries=max_retries,
        sleep=lambda s: sleeps.append(s), clock=lambda: 1000.0)
    return puller, handler, sleeps


def _body(entries, **extra):
    doc = {"entries": entries}
    doc.update(extra)
    return json.dumps(doc).encode()


# --------------------------------------------------------------------------
# 三大分支互斥：命中 / 损坏 / 暂时拿不到
# --------------------------------------------------------------------------
def test_fresh_then_cache_hit_is_distinct_branch(tmp_path):
    entries = [{"id": "e1", "version": "1.2.3"}]
    routes = {BASE: [(200, {"etag": '"v1"'}, _body(entries))]}
    puller, handler, _ = _puller(tmp_path, routes)

    o1 = puller.pull({**INTENT, "ttl": 100})
    assert o1.status == fp.FRESH and o1.confirmed
    assert [e["eid"] for e in o1.events] == ["e1"]
    assert o1.digest == sha256_hex(_body(entries))

    # 第二次：TTL 内零请求命中缓存
    o2 = puller.pull({**INTENT, "ttl": 100})
    assert o2.status == fp.CACHE_HIT and o2.confirmed
    assert handler.call_count == 1
    assert o2.digest == o1.digest and len(o2.events) == 1


def test_corrupt_download_never_replaces_cache_and_is_own_branch(tmp_path):
    good = _body([{"id": "e1", "version": "1.0.0"}])
    bad_digest = sha256_hex(b"something-else")
    routes = {
        BASE: [
            (200, {"etag": '"v1"'}, good),
            (200, {"digest": f"sha-256={bad_digest}"}, good),
        ]
    }
    puller, _, _ = _puller(tmp_path, routes)
    o1 = puller.pull(INTENT)
    assert o1.status == fp.FRESH
    o2 = puller.pull(INTENT)  # ttl=0，强制过期重拉
    assert o2.status == fp.CORRUPT
    assert not o2.confirmed and o2.events == []  # 失败不携带确认事件
    assert [e["eid"] for e in o2.stale_events] == ["e1"]  # 降级内容单独存放
    # 旧缓存完好保留，摘要证据未被污染
    kept = puller.cache.read(fp.cache_key(BASE), now=1000.0)
    assert kept.body == good and kept.digest == o1.digest


def test_truncated_empty_stream_is_corrupt_not_empty(tmp_path):
    routes = {BASE: [(200, {}, b"")]}  # 零字节 = 传输截断
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull(INTENT)
    assert out.status == fp.CORRUPT and not out.confirmed


def test_temporary_unavailable_degrades_without_fake_update(tmp_path):
    routes = {BASE: [
        (200, {}, _body([{"id": "e1", "version": "1.0.0"}])),
        TransportError("connection reset"),
    ]}
    puller, _, sleeps = _puller(tmp_path, routes, max_retries=2)
    assert puller.pull(INTENT).status == fp.FRESH
    out = puller.pull(INTENT)
    assert out.status == fp.UNAVAILABLE and not out.confirmed
    assert out.attempts == 2
    assert "stale cache" in out.detail  # 有旧缓存时显式标注降级
    assert out.events == []  # 失败分支绝不携带确认事件
    assert [e["eid"] for e in out.stale_events] == ["e1"]  # 旧内容单独降级
    assert out.digest  # 仍带完整性证据
    assert sleeps  # 退避确实发生
    # 无缓存时的暂时错误：无事件、无伪装更新（独立路由表，避免脚本被消费）
    p2, _, _ = _puller(tmp_path / "n2",
                       {BASE: [TransportError("offline")]}, max_retries=1)
    out2 = p2.pull(INTENT)
    assert out2.status == fp.UNAVAILABLE and out2.events == []


# --------------------------------------------------------------------------
# 条件请求：304 续用 / 304 缓存缺失是独立分支
# --------------------------------------------------------------------------
def test_304_revalidates_stale_cache_with_conditional_headers(tmp_path):
    routes = {BASE: [
        (200, {"etag": '"v1"', "last-modified": "Mon, 17 Nov 2026 00:00:00 GMT"},
         _body([{"id": "e1"}])),
        (304, {}, b""),
    ]}
    puller, handler, _ = _puller(tmp_path, routes)
    puller.pull(INTENT)
    out = puller.pull(INTENT)
    assert out.status == fp.STALE and out.confirmed
    sent = handler.header_sets()[-1]
    assert sent["if-none-match"] == '"v1"'
    assert sent["if-modified-since"].endswith("GMT")


def test_304_without_cache_is_not_modified_missing_branch(tmp_path):
    routes = {BASE: [(304, {}, b"")]}
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull(INTENT)
    assert out.status == fp.NOT_MODIFIED_MISSING and not out.confirmed


# --------------------------------------------------------------------------
# 速率限制：有界重试，耗尽后分类，不会无限挂起
# --------------------------------------------------------------------------
def test_rate_limit_retry_then_success(tmp_path):
    routes = {BASE: [
        (429, {"retry-after": "0"}, b""),
        (200, {}, _body([{"id": "e1"}])),
    ]}
    puller, handler, sleeps = _puller(tmp_path, routes, max_retries=2)
    out = puller.pull(INTENT)
    assert out.status == fp.FRESH and out.attempts == 2
    assert sleeps == [0.0]  # Retry-After=0 被尊重


def test_rate_limit_exhaustion_is_classified(tmp_path):
    routes = {BASE: [(429, {"retry-after": "0"}, b"")] * 2}
    puller, _, sleeps = _puller(tmp_path, routes, max_retries=2)
    out = puller.pull(INTENT)
    assert out.status == fp.RATE_LIMITED and not out.confirmed
    assert out.attempts == 2 and len(sleeps) == 1


def test_5xx_backoff_then_recovers(tmp_path):
    routes = {BASE: [
        (503, {}, b""), (500, {}, b""),
        (200, {}, _body([{"id": "e1"}])),
    ]}
    puller, _, sleeps = _puller(tmp_path, routes, max_retries=3)
    out = puller.pull(INTENT)
    assert out.status == fp.FRESH and out.attempts == 3
    assert len(sleeps) == 2  # 指数退避两次


# --------------------------------------------------------------------------
# 空响应 / 非法 JSON / 撤回页面及恢复
# --------------------------------------------------------------------------
def test_empty_json_response_is_empty_branch(tmp_path):
    routes = {BASE: [(200, {}, b'{"entries": []}')]}
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull(INTENT)
    assert out.status == fp.EMPTY and out.confirmed and out.events == []


def test_invalid_json_with_digest_header_is_corrupt(tmp_path):
    routes = {BASE: [(200, {"digest": "sha-256=" + sha256_hex(b"{broken")},
                     b"{broken")]}
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull(INTENT)
    assert out.status == fp.CORRUPT and not out.confirmed


def test_withdrawn_410_then_recovery(tmp_path):
    routes = {BASE: [
        (200, {}, _body([{"id": "e1", "version": "1.0.0"}])),
        (410, {}, b"gone"),
        (200, {}, _body([{"id": "e2", "version": "2.0.0"}])),
    ]}
    puller, _, _ = _puller(tmp_path, routes)
    assert puller.pull(INTENT).status == fp.FRESH
    gone = puller.pull(INTENT)
    assert gone.status == fp.WITHDRAWN and gone.withdrawn
    assert not gone.confirmed and gone.events == []
    # 撤回期间缓存仍保留证据
    entry = puller.cache.read(fp.cache_key(BASE), now=1000.0)
    assert entry.withdrawn and entry.body
    # 页面恢复：撤回墓碑之后的成功拉取标记 RECOVERED
    back = puller.pull(INTENT)
    assert back.status == fp.RECOVERED and back.confirmed
    assert [e["eid"] for e in back.events] == ["e2"]
    assert not puller.cache.read(fp.cache_key(BASE)).withdrawn


# --------------------------------------------------------------------------
# 分页：部分成功 -> 断线续拉 -> 原子替换；下游写入只在整批确认后
# --------------------------------------------------------------------------
def test_paginated_batch_interrupt_then_resume(tmp_path):
    page1 = json.dumps({"entries": [{"id": "e1"}]}).encode()
    page2 = json.dumps({"entries": [{"id": "e2"}]}).encode()
    page3 = json.dumps({"entries": [{"id": "e3"}]}).encode()
    routes = {
        BASE: [
            (200, {"link": f'<{PAGE2}>; rel="next"'}, page1),
            # 续拉成功后不会再请求 base（见第二轮断言）
        ],
        PAGE2: [
            TransportError("read interrupted"),       # 第一轮断线
            (200, {"link": f'<{PAGE2}&c=3>; rel="next"'}, page2),
        ],
        PAGE2 + "&c=3": [(200, {}, page3)],
    }
    puller, handler, _ = _puller(tmp_path, routes, max_retries=1)
    o1 = puller.pull(INTENT)
    assert o1.status == fp.UNAVAILABLE and not o1.confirmed
    assert o1.pages == 1  # 首页已确认并暂存
    # 缓存未被替换：read 仍缺失
    assert puller.cache.read(fp.cache_key(BASE), now=1000.0) is None
    staged = puller.cache.load_stage(fp.cache_key(BASE))
    assert len(staged) == 1 and staged[0]["cursor"] == PAGE2

    # 第二轮：从游标续拉，不重复首页
    o2 = puller.pull(INTENT)
    assert o2.confirmed and o2.resumed and o2.pages == 3
    assert [e["eid"] for e in o2.events] == ["e1", "e2", "e3"]
    urls = handler.urls()
    assert urls.count(BASE) == 1  # base 全程只请求一次
    # 暂存区清理、原子缓存就位
    assert puller.cache.load_stage(fp.cache_key(BASE)) == []
    entry = puller.cache.read(fp.cache_key(BASE), now=1000.0)
    assert entry.digest == sha256_hex(page1 + b"\n" + page2 + b"\n" + page3)


def test_resume_failure_discards_stage_and_restarts_batch(tmp_path):
    page1 = json.dumps({"entries": [{"id": "e1"}]}).encode()
    routes = {
        BASE: [
            (200, {"link": f'<{PAGE2}>; rel="next"'}, page1),
            (200, {}, json.dumps({"entries": [{"id": "fresh"}]}).encode()),
        ],
        PAGE2: [
            TransportError("still offline"),
            TransportError("still offline"),
        ],
    }
    puller, _, _ = _puller(tmp_path, routes, max_retries=1)
    o1 = puller.pull(INTENT)
    assert o1.status == fp.UNAVAILABLE
    o2 = puller.pull(INTENT)  # 续拉再失败 => 弃用暂存，整批重来
    assert o2.confirmed and not o2.resumed
    assert [e["eid"] for e in o2.events] == ["fresh"]


def test_corrupt_page_mid_batch_keeps_old_cache(tmp_path):
    old = json.dumps({"entries": [{"id": "old"}]}).encode()
    p1 = json.dumps({"entries": [{"id": "new1"}]}).encode()
    routes = {
        BASE: [
            (200, {}, old),
            (200, {"link": f'<{PAGE2}>; rel="next"'}, p1),
        ],
        PAGE2: [(200, {"digest": "sha-256=" + sha256_hex(b"x")}, b"new2")],
    }
    puller, _, _ = _puller(tmp_path, routes)
    assert puller.pull(INTENT).status == fp.FRESH
    out = puller.pull(INTENT)
    assert out.status == fp.CORRUPT and not out.confirmed
    kept = puller.cache.read(fp.cache_key(BASE), now=1000.0)
    assert kept.body == old  # 原子替换未发生


# --------------------------------------------------------------------------
# 三类来源 + 坏意图 + 非 JSON 文档
# --------------------------------------------------------------------------
@pytest.mark.parametrize("kind", ["release", "changelog", "advisory"])
def test_all_feed_kinds_supported(tmp_path, kind):
    routes = {BASE: [(200, {}, _body([{"id": "a1"}]))]}
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull({"kind": kind, "url": BASE, "source": "s"})
    assert out.confirmed and out.events[0]["kind"] == kind


def test_bad_intent_never_hits_network(tmp_path):
    tr, handler = make_transport({})
    puller = fp.FeedPuller(tr, FeedCache(tmp_path / "c"))
    assert puller.pull({"kind": "bogus", "url": BASE}).status == fp.BAD_INTENT
    assert puller.pull({"kind": "release", "url": ""}).status == fp.BAD_INTENT
    assert handler.call_count == 0


def test_plain_text_changelog_wrapped_as_single_doc(tmp_path):
    routes = {BASE: [(200, {"content-type": "text/plain"}, b"# 1.0.0 - notes")]}
    puller, _, _ = _puller(tmp_path, routes)
    out = puller.pull(INTENT)
    assert out.confirmed and len(out.events) == 1
    assert out.events[0]["eid"] == "alpha:doc"
