"""单一职责：feed_pull——按来源做条件请求、分页、校验与缓存，失败必须可分类。

三条互斥的顶层分支（下游 semver 只处理 ``CONFIRMED``，绝不会把失败当更新）：

* 成功确认：FRESH / CACHE_HIT / STALE / RECOVERED / EMPTY / WITHDRAWN
* 下载损坏：CORRUPT（下载体校验失败、JSON 非法、装配后摘要不符）
* 暂时拿不到：UNAVAILABLE / RATE_LIMITED / BAD_INTENT / NOT_MODIFIED_MISSING

重试只发生在传输层单页请求内，且整批未确认前内容只进 stage 暂存区；
替换缓存走 FeedCache.commit 的原子改名并保留 sha256 完整性证据。
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Union

from . import config
from .feedcache import FeedCache, cache_key, sha256_hex
from .transport import RawResponse, Transport, TransportError

log = logging.getLogger("changelogfeed.feed_pull")

# ---- 结果分类（可诊断的稳定枚举值，日志/测试直接引用）------------------
FRESH = "fresh"                    # 200 新内容已确认并落缓存
CACHE_HIT = "cache_hit"            # 缓存仍新鲜，未发请求
STALE = "stale"                    # 缓存过期：304 续用 / 失败降级
RECOVERED = "recovered"            # 撤回墓碑之后重新拿到有效内容
EMPTY = "empty"                    # 200 空响应，缓存保持不变
WITHDRAWN = "withdrawn"            # 410 撤回页（立墓碑，内容不再下游可见）
CORRUPT = "corrupt"                # 下载损坏：摘要不符/截断/非法 JSON
UNAVAILABLE = "unavailable"        # 网络/5xx 等暂时错误，重试耗尽
RATE_LIMITED = "rate_limited"      # 429 超过重试预算
BAD_INTENT = "bad_intent"          # 记录的拉取意图不完整，未发请求
NOT_MODIFIED_MISSING = "not_modified_missing"  # 304 但缓存缺失/损坏

CONFIRMED_STATUSES = frozenset(
    {FRESH, CACHE_HIT, STALE, RECOVERED, EMPTY})
TRANSIENT_STATUSES = frozenset({UNAVAILABLE, RATE_LIMITED})
FAILURE_STATUSES = frozenset(
    {CORRUPT, UNAVAILABLE, RATE_LIMITED, BAD_INTENT, NOT_MODIFIED_MISSING})

_LINK_NEXT = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?', re.IGNORECASE)


@dataclass
class PullOutcome:
    """一次拉取的分类结论。failure 分支绝不携带 events。"""
    status: str
    key: str
    events: list[dict] = field(default_factory=list)
    # 失败降级时旧缓存的内容只放在这里，与确认事件严格区分，永不进下游
    stale_events: list[dict] = field(default_factory=list)
    etag: str | None = None
    last_modified: str | None = None
    digest: str | None = None
    detail: str = ""
    attempts: int = 0
    pages: int = 0
    resumed: bool = False
    withdrawn: bool = False
    confirmed: bool = False

    def as_meta(self) -> dict[str, str | int | bool]:
        """给记录用的可诊断摘要（不含正文，避免日志/落库膨胀）。"""
        m: dict[str, str | int | bool] = {"status": self.status,
                                          "detail": self.detail,
                                          "attempts": self.attempts,
                                          "pages": self.pages}
        if self.digest:
            m["digest"] = self.digest
        if self.etag:
            m["etag"] = self.etag
        if self.resumed:
            m["resumed"] = True
        if self.withdrawn:
            m["withdrawn"] = True
        if self.stale_events:
            m["stale_fallback"] = True
        return m


def parse_retry_after(value: str | None, now: float) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - now)
        except (TypeError, ValueError):
            return None


def expected_digest(headers: dict[str, str]) -> str | None:
    """支持 RFC 3230 Want- Digest 回传与常见 x-content-sha256。"""
    val = headers.get("digest") or headers.get("x-content-sha256")
    if not val:
        return None
    for part in val.split(","):
        algo, _, token = part.strip().partition("=")
        if algo.strip().lower() in {"sha-256", "sha256"}:
            return token.strip().strip('"').lower()
    return None


def next_cursor(headers: dict[str, str], payload: Any, base_url: str) -> str | None:
    m = _LINK_NEXT.search(headers.get("link", ""))
    if m:
        nxt = m.group(1)
        if nxt.startswith(("http://", "https://")):
            return nxt
        from urllib.parse import urljoin
        return urljoin(base_url, nxt)
    if isinstance(payload, dict):
        nxt = payload.get("next_cursor") or payload.get("next")
        if isinstance(nxt, str) and nxt:
            if nxt.startswith(("http://", "https://")):
                return nxt
            from urllib.parse import urljoin
            return urljoin(base_url, nxt)
    return None


def extract_events(payload: Any, kind: str, source: str) -> list[dict]:
    """从确认后的 JSON 体提取条目；非 JSON 体由调用方包成单条。"""
    if isinstance(payload, dict) and isinstance(payload.get("entries"), list):
        raw_entries = payload["entries"]
    elif isinstance(payload, list):
        raw_entries = payload
    else:
        return []
    events = []
    for i, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            continue
        eid = str(item.get("id") or item.get("version")
                  or item.get("eid") or f"{source}:{i}")
        events.append({
            "eid": eid, "kind": kind, "source": source,
            "version": item.get("version") or item.get("eid"),
            "title": item.get("title") or item.get("summary") or "",
            "severity": item.get("severity"),
            "ts": item.get("ts") or item.get("published_at") or "",
            "url": item.get("url") or "",
        })
    return events


class FeedPuller:
    """编排单来源的条件请求 + 分页 + 校验 + 缓存替换。"""

    def __init__(self, transport: Transport, cache: FeedCache | None = None,
                 *, max_retries: int | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.time):
        self.http = transport
        self.cache = cache or FeedCache()
        self.max_retries = (config.DEFAULT_MAX_RETRIES if max_retries is None
                            else max_retries)
        self._sleep = sleep
        self._clock = clock

    # ---- 暂时错误的有界重试 --------------------------------------------
    def _request_with_retry(self, url: str, headers: dict[str, str],
                            key: str
                            ) -> Union[tuple[RawResponse, int],
                                       tuple[None, "PullOutcome"]]:
        """成功返回 (RawResponse, attempts)；失败返回 (None, PullOutcome)。"""
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.http.get(url, headers=headers)
            except TransportError as exc:
                if attempt >= self.max_retries:
                    return None, PullOutcome(
                        UNAVAILABLE, key, detail=f"transport after "
                        f"{attempt} attempts: {exc}", attempts=attempt)
                self._sleep(config.BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
                continue
            if resp.status == 429:
                wait = parse_retry_after(resp.header("retry-after"),
                                         self._clock())
                wait = config.BACKOFF_BASE_SECONDS if wait is None else min(
                    wait, config.MAX_RETRY_AFTER_SECONDS)
                if attempt >= self.max_retries:
                    return None, PullOutcome(
                        RATE_LIMITED, key,
                        detail=f"429 after {attempt} attempts",
                        attempts=attempt)
                self._sleep(wait)
                continue
            if 500 <= resp.status < 600:
                if attempt >= self.max_retries:
                    return None, PullOutcome(
                        UNAVAILABLE, key,
                        detail=f"HTTP {resp.status} after {attempt} attempts",
                        attempts=attempt)
                self._sleep(config.BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
                continue
            return resp, attempt

    # ---- 单页校验 -------------------------------------------------------
    @staticmethod
    def _page_corrupt(resp, key: str) -> PullOutcome | None:
        want = expected_digest(resp.headers)
        if want and sha256_hex(resp.content) != want:
            return PullOutcome(CORRUPT, key,
                               detail="content digest mismatch",
                               attempts=1)
        if resp.status == 200 and len(resp.content) == 0:
            # 空字节流按传输截断处理，区别于合法的空 JSON 文档
            return PullOutcome(CORRUPT, key,
                               detail="truncated empty body", attempts=1)
        return None

    # ---- 主入口 ---------------------------------------------------------
    def pull(self, intent: dict[str, Any]) -> PullOutcome:
        kind = str(intent.get("kind", "")).strip().lower()
        url = str(intent.get("url", "")).strip()
        source = str(intent.get("source", "")).strip()
        raw_ttl = intent.get("ttl")
        ttl = float(raw_ttl) if raw_ttl is not None \
            else float(config.DEFAULT_TTL_SECONDS)
        if kind not in {"release", "changelog", "advisory"} or not url:
            return PullOutcome(BAD_INTENT, cache_key(url) if url else "",
                               detail=f"invalid intent kind={kind!r} url={url!r}")
        key = cache_key(url)
        now = self._clock()
        cached = self.cache.read(key, now=now)

        # 1) 缓存仍在当前意图 TTL 内：直接命中，零请求（策略以当前意图为准）
        if cached is not None and not cached.withdrawn \
                and (now - cached.fetched_at) < ttl:
            events = self._events_of(cached.body, kind, source)
            log.info("feed cache hit source=%s key=%s events=%d",
                     source, key, len(events))
            return PullOutcome(CACHE_HIT, key, events=events,
                               etag=cached.etag, last_modified=cached.last_modified,
                               digest=cached.digest, pages=1, confirmed=True)

        # 2) 条件请求头
        headers: dict[str, str] = {}
        if cached is not None:
            if cached.etag:
                headers["if-none-match"] = cached.etag
            if cached.last_modified:
                headers["if-modified-since"] = cached.last_modified

        # 2b) 断线续拉：暂存区有已确认分页时，从最后游标继续；
        #     续拉请求本身失败则丢弃暂存，整批从 base 重来
        staged = self.cache.load_stage(key)
        resume_pages: list[dict] = []
        if staged and staged[-1].get("cursor"):
            r_resp, r_info = self._request_with_retry(
                staged[-1]["cursor"], {}, key)
            if r_resp is not None and 200 <= r_resp.status < 300 \
                    and self._page_corrupt(r_resp, key) is None:
                resume_pages = staged
                resp = r_resp
            else:
                self.cache.clear_stage(key)
        elif staged:
            self.cache.clear_stage(key)

        if resume_pages:
            # 续拉响应在上面已校验为 200 且完整，直接进入批次装配
            return self._pull_batch(intent, key, resp, cached, ttl,
                                    resume_pages, first_attempts=r_info)
        resp, info = self._request_with_retry(url, headers, key)
        if resp is None:
            return self._degrade(info, cached, kind, source, key)

        # 3) 304：缓存必须存在且完整，否则是独立的可诊断分支
        if resp.status == 304:
            if cached is None:
                return PullOutcome(NOT_MODIFIED_MISSING, key,
                                   detail="304 without usable cache",
                                   attempts=info)
            entry = self.cache.touch_revalidated(key, cached, now=self._clock())
            events = self._events_of(entry.body, kind, source)
            log.info("feed revalidated source=%s key=%s events=%d",
                     source, key, len(events))
            return PullOutcome(STALE, key, events=events, etag=entry.etag,
                               last_modified=entry.last_modified,
                               digest=entry.digest, detail="304",
                               attempts=info, pages=1, confirmed=True)

        # 4) 410 撤回页：立墓碑；内容保留为证据但不再下游可见
        if resp.status == 410:
            self.cache.mark_withdrawn(key, now=self._clock())
            log.warning("feed withdrawn source=%s key=%s", source, key)
            return PullOutcome(WITHDRAWN, key, detail="HTTP 410",
                               attempts=info, withdrawn=True)

        # 5) 非 2xx 的其余状态：暂时拿不到，走降级
        if not (200 <= resp.status < 300):
            return self._degrade(
                PullOutcome(UNAVAILABLE, key,
                            detail=f"HTTP {resp.status}", attempts=info),
                cached, kind, source, key)

        # 6) 200 批量拉取（首页 + 分页），未确认内容只进 stage
        return self._pull_batch(intent, key, resp, cached, ttl, resume_pages,
                                first_attempts=info)

    # ---- 批量分页 + 续拉 ------------------------------------------------
    def _pull_batch(self, intent, key, first_resp, cached, ttl,
                    resume_pages=None, first_attempts=1) -> PullOutcome:
        kind, source = intent["kind"], intent["source"]
        now = self._clock()
        if not resume_pages:  # 续拉首页此前已逐页复核，无需重复校验
            corrupt = self._page_corrupt(first_resp, key)
            if corrupt is not None:
                return self._degrade(corrupt, cached, kind, source, key)

        resumed = bool(resume_pages)
        pages: list[dict] = list(resume_pages or [])
        resp = first_resp
        page_index = len(pages)
        attempts_total = first_attempts  # 首页（含条件请求重试）的尝试次数

        while True:
            try:
                payload = json.loads(resp.content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                # 非 JSON 且没有摘要头：文本类 changelog 允许，按单条文档处理
                if expected_digest(resp.headers) is None:
                    payload = None
                else:
                    return self._degrade(
                        PullOutcome(CORRUPT, key,
                                    detail=f"invalid json: {exc}"),
                        cached, kind, source, key)
            cursor = next_cursor(resp.headers, payload, intent["url"])
            self.cache.stage_page(key, page_index, resp.content,
                                  resp.headers, cursor)
            pages.append({"index": page_index, "body": resp.content,
                          "headers": resp.headers, "cursor": cursor})
            page_index += 1
            if not cursor:
                break
            resp, info = self._request_with_retry(cursor, {}, key)
            attempts_total += info.attempts if resp is None else info
            if resp is None:
                # 断线：已确认分页留在 stage，下次续拉；本次不替换缓存
                log.warning("feed batch interrupted source=%s key=%s pages=%d",
                            source, key, len(pages))
                return self._degrade(info, cached, kind, source, key,
                                     resumed=resumed, staged_pages=len(pages))
            if resp.status == 429:
                return self._degrade(
                    PullOutcome(RATE_LIMITED, key,
                                detail=f"HTTP 429 on page {page_index}",
                                attempts=info),
                    cached, kind, source, key, resumed=resumed,
                    staged_pages=len(pages))
            if not (200 <= resp.status < 300):
                return self._degrade(
                    PullOutcome(UNAVAILABLE, key,
                                detail=f"HTTP {resp.status} on page "
                                       f"{page_index}", attempts=info),
                    cached, kind, source, key, resumed=resumed,
                    staged_pages=len(pages))
            corrupt = self._page_corrupt(resp, key)
            if corrupt is not None:
                return self._degrade(corrupt, cached, kind, source, key,
                                     resumed=resumed, staged_pages=len(pages))

        # 7) 整批确认：装配 -> 全量摘要 -> 原子替换
        assembled = b"\n".join(p["body"] for p in sorted(
            pages, key=lambda r: r["index"]))
        first_headers = pages[0]["headers"]
        want = expected_digest(first_headers)
        if want and sha256_hex(assembled) != want:
            return self._degrade(
                PullOutcome(CORRUPT, key,
                            detail="assembled digest mismatch"),
                cached, kind, source, key)
        entry = self.cache.commit(
            key, assembled,
            etag=first_headers.get("etag"),
            last_modified=first_headers.get("last-modified"),
            ttl=ttl, now=now)
        self.cache.clear_stage(key)

        if len(assembled) == 0 or assembled in {b"[]", b"{}"} or \
                self._is_empty_json(assembled):
            log.info("feed empty source=%s key=%s", source, key)
            return PullOutcome(EMPTY, key, etag=entry.etag,
                               last_modified=entry.last_modified,
                               digest=entry.digest, attempts=attempts_total,
                               pages=len(pages), resumed=resumed,
                               confirmed=True)

        events = self._events_of(assembled, kind, source)
        was_withdrawn = cached is not None and cached.withdrawn
        status = RECOVERED if was_withdrawn else (
            STALE if cached is not None else FRESH)
        log.info("feed confirmed source=%s key=%s status=%s events=%d pages=%d",
                 source, key, status, len(events), len(pages))
        return PullOutcome(status, key, events=events, etag=entry.etag,
                           last_modified=entry.last_modified,
                           digest=entry.digest, attempts=attempts_total,
                           pages=len(pages), resumed=resumed, confirmed=True)

    @staticmethod
    def _parse_payloads(body: bytes) -> tuple[list, bool]:
        """解析单文档或分页装配出的 NDJSON；返回 (文档列表, 是否可解析)。"""
        text = body.decode("utf-8")
        try:
            return [json.loads(text)], True
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        docs, ok = [], True
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                docs.append(json.loads(line))
            except json.JSONDecodeError:
                ok = False
                break
        return docs, ok and bool(docs)

    @classmethod
    def _is_empty_json(cls, body: bytes) -> bool:
        docs, ok = cls._parse_payloads(body)
        if not ok:
            return False
        for payload in docs:
            if isinstance(payload, dict):
                entries = payload.get("entries")
                if not (isinstance(entries, list) and len(entries) == 0):
                    return False
            elif not (isinstance(payload, list) and len(payload) == 0):
                return False
        return bool(docs)

    @classmethod
    def _events_of(cls, body: bytes, kind: str, source: str) -> list[dict]:
        docs, ok = cls._parse_payloads(body)
        if not ok:
            # 非 JSON（文本 changelog）：包成单条文档事件
            return [{"eid": f"{source}:doc", "kind": kind, "source": source,
                     "version": None, "title": "", "severity": None,
                     "ts": "", "url": ""}]
        events = []
        for payload in docs:
            events.extend(extract_events(payload, kind, source))
        return events

    # ---- 失败降级：旧缓存保留为证据，失败绝不伪装成更新 -----------------
    def _degrade(self, failure: PullOutcome, cached, kind, source, key,
                 *, resumed: bool = False,
                 staged_pages: int = 0) -> PullOutcome:
        failure.resumed = resumed
        if staged_pages:
            failure.pages = staged_pages
        if cached is not None and not cached.withdrawn:
            # 有可验证旧缓存：降级内容单独存放，明确不是新确认、不进下游
            failure.stale_events = self._events_of(cached.body, kind, source)
            failure.digest = cached.digest
            failure.etag = cached.etag
            failure.last_modified = cached.last_modified
            failure.detail = (failure.detail + "；served stale cache").strip()
        log.warning("feed failure source=%s key=%s status=%s detail=%s",
                    source, key, failure.status, failure.detail)
        return failure


# --------------------------------------------------------------------------
# 流水线阶段：模块级会话，pipeline 无业务判断，测试可 reset 注入离线传输
# --------------------------------------------------------------------------
_SESSION: FeedPuller | None = None


def reset_session(puller: FeedPuller | None = None) -> None:
    """测试钩子：注入离线 Transport 后的 puller，或恢复默认会话。"""
    global _SESSION
    _SESSION = puller


def get_session() -> FeedPuller:
    global _SESSION
    if _SESSION is None:
        _SESSION = FeedPuller(Transport())
    return _SESSION


def process(record: dict) -> dict:
    """旧输入（无 _feed_intent）原样透传；新输入按来源拉取并打分类标记。"""
    rec = dict(record)
    rec.setdefault("_stage_feed_pull", "naive")
    intent = rec.get("_feed_intent")
    if not isinstance(intent, dict):
        return rec
    outcome = get_session().pull(intent)
    rec["_feed_status"] = outcome.status
    rec["_feed_meta"] = outcome.as_meta()
    rec["_feed_confirmed"] = outcome.confirmed
    rec["_feed_withdrawn"] = outcome.withdrawn
    rec["_stage_feed_pull"] = outcome.status
    # 只有确认分支携带事件；失败分支不写 events，semver 无法把失败当更新
    if outcome.confirmed:
        rec["_feed_events"] = outcome.events
        rec["_feed_digest"] = outcome.digest
    elif outcome.stale_events:
        # 降级旧内容单独标注，供诊断，不会被下游当作确认事件消费
        rec["_feed_stale_count"] = len(outcome.stale_events)
    return rec
