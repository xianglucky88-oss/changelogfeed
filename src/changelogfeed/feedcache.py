"""单一职责：feed 内容的磁盘缓存——完整性证据、原子替换、分页暂存、隔离。

目录布局（key 为来源标识的安全哈希）::

    <root>/<key>/meta.json        已提交条目（校验器/摘要/新鲜度/撤回墓碑）
    <root>/<key>/body             已提交的原始内容（临时内容绝不落到这里）
    <root>/<key>/stage/           批量分页的暂存区（未确认内容）
    <root>/quarantine/<ts>-<key>/ 损坏条目的证据留存

不变量：
* 替换缓存只通过 ``commit`` 的“写临时文件 + os.replace 原子改名”完成；
* 每次提交都落 sha256 摘要，读取时强制复核，损坏即隔离并视为缓存缺失；
* stage 里的分页在整批确认前对 ``read`` 不可见。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from . import config

_META = "meta.json"
_BODY = "body"
_STAGE = "stage"
_QUAR = "quarantine"

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def cache_key(url: str) -> str:
    """只取 origin+path，避免查询串里的令牌进入磁盘路径。"""
    from urllib.parse import urlsplit
    parts = urlsplit(url)
    basis = f"{parts.scheme}://{parts.netloc}{parts.path}"
    return _SAFE.sub("_", basis).strip("_") + "-" + hashlib.sha256(
        basis.encode("utf-8")).hexdigest()[:12]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class CacheEntry:
    body: bytes
    etag: str | None
    last_modified: str | None
    digest: str
    fetched_at: float
    ttl: float
    withdrawn: bool

    def is_fresh(self, now: float) -> bool:
        return (now - self.fetched_at) < self.ttl


class FeedCache:
    def __init__(self, root: Path | None = None):
        self.root = Path(root or config.CACHE_DIR)
        self.root.mkdir(parents=True, exist_ok=True)

    # ---- 内部路径 -------------------------------------------------------
    def _dir(self, key: str) -> Path:
        d = self.root / key
        d.mkdir(parents=True, exist_ok=True)
        return d

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        """同目录临时文件 + os.replace：读者要么看到旧文件要么看到新文件。"""
        tmp = path.with_name(path.name + ".tmp-" + str(os.getpid()))
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _quarantine(self, key: str, entry_dir: Path, reason: str) -> None:
        qdir = self.root / _QUAR / f"{int(time.time())}-{key}"
        qdir.mkdir(parents=True, exist_ok=True)
        try:
            for name in (_META, _BODY):
                p = entry_dir / name
                if p.exists():
                    shutil.copy2(p, qdir / name)
            (qdir / "reason.txt").write_text(reason, encoding="utf-8")
        except OSError:  # 隔离失败不能掩盖原始损坏
            pass

    # ---- 读取 -----------------------------------------------------------
    def read(self, key: str, now: float | None = None) -> CacheEntry | None:
        """返回通过完整性复核的已提交条目；损坏则隔离并返回 None。"""
        now = time.time() if now is None else now
        d = self.root / key
        meta_p, body_p = d / _META, d / _BODY
        if not (meta_p.exists() and body_p.exists()):
            return None
        try:
            meta = json.loads(meta_p.read_text(encoding="utf-8"))
            body = body_p.read_bytes()
        except (OSError, ValueError):
            self._quarantine(key, d, "unreadable meta/body")
            return None
        digest = meta.get("digest")
        if not digest or sha256_hex(body) != digest:
            self._quarantine(key, d, "digest mismatch on read")
            return None
        return CacheEntry(
            body=body,
            etag=meta.get("etag"),
            last_modified=meta.get("last_modified"),
            digest=digest,
            fetched_at=float(meta.get("fetched_at", 0.0)),
            ttl=float(meta.get("ttl", config.DEFAULT_TTL_SECONDS)),
            withdrawn=bool(meta.get("withdrawn", False)),
        )

    # ---- 提交（原子替换，保留完整性证据）--------------------------------
    def commit(self, key: str, body: bytes, *, etag=None, last_modified=None,
               ttl: float | None = None, now: float | None = None,
               withdrawn: bool = False) -> CacheEntry:
        """整批内容确认后一次性替换缓存；返回带摘要证据的新条目。"""
        now = time.time() if now is None else now
        d = self._dir(key)
        digest = sha256_hex(body)
        entry = CacheEntry(
            body=body, etag=etag, last_modified=last_modified, digest=digest,
            fetched_at=now, ttl=float(ttl if ttl is not None
                                      else config.DEFAULT_TTL_SECONDS),
            withdrawn=withdrawn,
        )
        # 先写 body 再原子换 meta，任何时刻 read() 都不会读到半套数据
        self._atomic_write(d / _BODY, body)
        meta = {
            "etag": etag, "last_modified": last_modified,
            "digest": digest, "fetched_at": now, "ttl": entry.ttl,
            "withdrawn": withdrawn,
        }
        self._atomic_write(d / _META,
                           json.dumps(meta, ensure_ascii=False).encode("utf-8"))
        return entry

    def touch_revalidated(self, key: str, entry: CacheEntry,
                          now: float | None = None) -> CacheEntry | None:
        """304 未变更：仅刷新新鲜度与校验器，内容与摘要原样保留。"""
        now = time.time() if now is None else now
        return self.commit(
            key, entry.body, etag=entry.etag, last_modified=entry.last_modified,
            ttl=entry.ttl, now=now, withdrawn=entry.withdrawn)

    def mark_withdrawn(self, key: str, now: float | None = None) -> None:
        """撤回墓碑：缓存仍保留可诊断证据，但被标记为撤回状态。"""
        now = time.time() if now is None else now
        entry = self.read(key, now=now)
        if entry is None:
            # 没缓存也立墓碑，防止撤回页被当成“暂时拿不到”反复重试
            self.commit(key, b"", now=now, withdrawn=True)
            return
        self.commit(key, entry.body, etag=entry.etag,
                    last_modified=entry.last_modified, ttl=entry.ttl,
                    now=now, withdrawn=True)

    # ---- 分页暂存区 -----------------------------------------------------
    def stage_dir(self, key: str) -> Path:
        sd = self._dir(key) / _STAGE
        sd.mkdir(parents=True, exist_ok=True)
        return sd

    def stage_page(self, key: str, index: int, body: bytes,
                   headers: dict[str, str], cursor: str | None) -> None:
        sd = self.stage_dir(key)
        self._atomic_write(sd / f"page-{index:04d}.body", body)
        rec = {"index": index, "digest": sha256_hex(body),
               "headers": dict(headers), "cursor": cursor}
        self._atomic_write(sd / f"page-{index:04d}.json",
                           json.dumps(rec, ensure_ascii=False).encode("utf-8"))

    def load_stage(self, key: str) -> list[dict]:
        """读取并逐页复核暂存；摘要不符的页直接丢弃（上层整批重来）。"""
        sd = self.root / key / _STAGE
        if not sd.is_dir():
            return []
        pages = []
        for jp in sorted(sd.glob("page-*.json")):
            try:
                rec = json.loads(jp.read_text(encoding="utf-8"))
                body = (sd / f"page-{int(rec['index']):04d}.body").read_bytes()
            except (OSError, ValueError, KeyError):
                return []
            if sha256_hex(body) != rec.get("digest"):
                return []
            rec["body"] = body
            pages.append(rec)
        pages.sort(key=lambda r: r["index"])
        return pages

    def clear_stage(self, key: str) -> None:
        sd = self.root / key / _STAGE
        if sd.exists():
            shutil.rmtree(sd, ignore_errors=True)
