"""FeedCache：完整性证据、原子替换、暂存续拉与损坏隔离。"""
import json

from changelogfeed.feedcache import FeedCache, cache_key, sha256_hex


def _make(tmp_path):
    return FeedCache(tmp_path / "cache")


def test_commit_and_read_roundtrip_with_digest(tmp_path):
    c = _make(tmp_path)
    body = b'{"entries":[]}'
    e = c.commit("k1", body, etag='"v1"', ttl=60, now=100.0)
    assert e.digest == sha256_hex(body)
    got = c.read("k1", now=101.0)
    assert got is not None and got.body == body
    assert got.etag == '"v1"' and got.is_fresh(101.0)
    assert not got.is_fresh(200.0)  # ttl 过后过期


def test_atomic_replace_keeps_old_entry_until_commit(tmp_path):
    c = _make(tmp_path)
    old = b'old'
    c.commit("k1", old, now=1.0)
    # 直接制造半成品临时文件：read 必须仍读到旧条目，绝不见半套数据
    d = c.root / "k1"
    (d / "body.tmp-0").write_bytes(b"partial")
    got = c.read("k1")
    assert got.body == old


def test_corrupted_body_is_quarantined_and_reported_missing(tmp_path):
    c = _make(tmp_path)
    c.commit("k1", b"good-body", now=1.0)
    (c.root / "k1" / "body").write_bytes(b"tampered")
    assert c.read("k1") is None  # 摘要复核失败 => 缓存缺失
    quar = list((c.root / "quarantine").glob("*"))
    assert len(quar) == 1
    assert (quar[0] / "reason.txt").read_text("utf-8").startswith(
        "digest mismatch")
    # 原损坏证据已被复制到隔离区
    assert (quar[0] / "body").read_bytes() == b"tampered"


def test_stage_pages_are_invisible_until_commit_and_resumable(tmp_path):
    c = _make(tmp_path)
    assert c.read("k1") is None
    c.stage_page("k1", 0, b"page0", {"link": '<?p=1>; rel="next"'}, "?p=1")
    c.stage_page("k1", 1, b"page1", {}, None)
    # 未提交前 read 看不到暂存内容
    assert c.read("k1") is None
    pages = c.load_stage("k1")
    assert [p["index"] for p in pages] == [0, 1]
    assert pages[0]["body"] == b"page0" and pages[1]["cursor"] is None
    c.commit("k1", b"page0\npage1", now=2.0)
    c.clear_stage("k1")
    assert c.load_stage("k1") == []
    assert c.read("k1").body == b"page0\npage1"


def test_tampered_stage_page_breaks_resume(tmp_path):
    c = _make(tmp_path)
    c.stage_page("k1", 0, b"page0", {}, "?p=1")
    sd = c.stage_dir("k1")
    (sd / "page-0000.body").write_bytes(b"tampered")
    assert c.load_stage("k1") == []  # 复核失败，上层整批重来


def test_withdrawn_tombstone_and_recovery(tmp_path):
    c = _make(tmp_path)
    c.commit("k1", b"original", now=1.0)
    c.mark_withdrawn("k1", now=2.0)
    e = c.read("k1", now=2.0)
    assert e.withdrawn is True and e.body == b"original"  # 证据保留
    # 没有旧缓存时也能立墓碑
    c.mark_withdrawn("k2", now=2.0)
    assert c.read("k2", now=2.0).withdrawn is True
    # 恢复：新的成功提交覆盖墓碑
    c.commit("k1", b"restored", now=3.0)
    e2 = c.read("k1", now=3.0)
    assert e2.withdrawn is False and e2.body == b"restored"


def test_cache_key_excludes_query_tokens(tmp_path):
    k1 = cache_key("https://example.com/feed?token=secret&a=1")
    k2 = cache_key("https://example.com/feed?token=other&a=2")
    assert k1 == k2 and "secret" not in k1
    assert cache_key("https://example.com/other") != k1


def test_touch_revalidated_preserves_digest(tmp_path):
    c = _make(tmp_path)
    e = c.commit("k1", b"body", etag='"e"', ttl=10, now=0.0)
    e2 = c.touch_revalidated("k1", e, now=50.0)
    assert e2.digest == e.digest and e2.body == b"body"
    assert e2.is_fresh(51.0)  # 新鲜度被刷新
