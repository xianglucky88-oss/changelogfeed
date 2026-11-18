"""日志纪律：只记录必要摘要（来源/键/状态/计数/短诊断），
绝不记录请求体、缓存正文或完整 URL 查询串。"""
import json
import logging

from src.changelogfeed import feed_pull
from src.changelogfeed.feedcache import FeedCache
from src.changelogfeed.transport import TransportError
from tests.offline_mock import make_transport

BASE = "https://feed.test/v1/secret-feed?token=abc123"
INTENT = {"kind": "release", "url": BASE, "source": "alpha", "ttl": 0}


def _puller(tmp_path, routes, **kw):
    tr, _ = make_transport(routes)
    return feed_pull.FeedPuller(
        tr, FeedCache(tmp_path / "c"), sleep=lambda s: None,
        clock=lambda: 1000.0, **kw)


def test_logs_are_summaries_without_body_or_token(tmp_path, caplog):
    body = json.dumps({"entries": [{"id": "e1", "secret": "do-not-log"}]}
                      ).encode()
    puller = _puller(tmp_path, {BASE: [
        (200, {}, body),
        TransportError("reset"),
    ]}, max_retries=1)
    with caplog.at_level(logging.INFO, logger="changelogfeed.feed_pull"):
        puller.pull(INTENT)
        puller.pull(INTENT)
    assert caplog.records  # 确有日志输出
    text = "\n".join(r.getMessage() for r in caplog.records)
    # 正文内容与令牌绝不进日志
    assert "do-not-log" not in text
    assert "abc123" not in text and "token" not in text
    # 只有结构化摘要字段
    assert any("status=unavailable" in r.getMessage() for r in caplog.records)
    for r in caplog.records:
        assert len(r.getMessage()) < 240  # 单行摘要有界
