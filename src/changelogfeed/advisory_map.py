"""单一职责：安全公告影响语义（advisory_map）。

本模块只负责"公告侧"语义，供 ``relate`` 消费，不另造一套变更条目关联流程：

1. 规范包名与生态（canonical package identity），处理公告别名；
2. 解析公告版本范围 / 受影响区间（含预发布语义、无版本约束）；
3. 维护公告证据状态：正文、撤回、先撤回后正文（待判定）、多来源冲突；
4. 按现有依赖记录契约（dict 记录 + 稳定 key + sqlite 幂等 upsert）
   输出可追溯的匹配依据（match basis）。

旧调用入口 ``process(record)`` 保留：对单条公告记录做无副作用的规范/解析；
需要持久化匹配、待判定记录与重启一致性时使用 ``ingest_*`` / ``evaluate_*``。
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable

from . import store as _store

# ---------------------------------------------------------------------------
# 证据 / 判定状态词表（输出契约，relate 与测试均依赖这些字面量）
# ---------------------------------------------------------------------------
AFFECTED = "affected"          # 命中受影响区间
NOT_AFFECTED = "not_affected"  # 同包同生态，但版本不在区间内
WITHDRAWN = "withdrawn"        # 公告已撤回，先前判断被有据更新
PENDING = "pending"            # 待判定：先收到撤回 / 区间无法解析

EVENT_BODY = "body"
EVENT_WITHDRAWN = "withdrawn"

# ---------------------------------------------------------------------------
# 生态规范：同一生态的不同写法归一到一个权威名
# ---------------------------------------------------------------------------
_ECOSYSTEM_ALIASES = {
    "pypi": "pypi", "pip": "pypi", "python": "pypi",
    "npm": "npm", "node": "npm", "nodejs": "npm",
    "maven": "maven", "jvm": "maven", "java": "maven",
    "go": "go", "golang": "go",
    "rubygems": "rubygems", "gem": "rubygems", "ruby": "rubygems",
    "crates.io": "crates.io", "crates": "crates.io", "cargo": "crates.io",
    "nuget": "nuget", ".net": "nuget", "dotnet": "nuget",
    "packagist": "packagist", "composer": "packagist",
}

# 各生态包名大小写规范：pypi/nuget 不区分大小写，其余保留大小写
_CASEFOLD_ECOSYSTEMS = {"pypi", "nuget"}

_VERSION_RE = re.compile(
    r"^[vV]?(\d+)(?:\.(\d+))?(?:\.(\d+))?"
    r"(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$"
)
_CONSTRAINT_RE = re.compile(r"\s*(>=|<=|==|!=|=|>|<)?\s*([0-9vV][0-9A-Za-z.+_-]*)\s*")


# ---------------------------------------------------------------------------
# 语义版本（含预发布 precedence），标准库实现、无外部依赖
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Version:
    release: tuple[int, ...]
    prerelease: tuple[Any, ...] | None
    build: tuple[str, ...] | None
    raw: str

    @staticmethod
    def _cmp_id(ident: str) -> tuple[int, Any]:
        # 数字标识按数值比较且优先级低于字符串标识（semver 11.4.1）
        if ident.isdigit():
            return (0, int(ident))
        return (1, ident)

    def _key(self) -> tuple:
        rel = self.release + (0, 0, 0)
        rel = rel[: max(3, len(self.release))]
        if self.prerelease is None:
            pre = (1,)          # 正式版 > 同版本预发布
        else:
            pre = (0,) + tuple(self._cmp_id(p) for p in self.prerelease)
        return rel, pre

    def __lt__(self, other: "Version") -> bool:
        return self._key() < other._key()

    def __le__(self, other: "Version") -> bool:
        return self._key() <= other._key()

    def __gt__(self, other: "Version") -> bool:
        return self._key() > other._key()


def parse_version(text: Any) -> Version:
    """解析版本号；无法解析时抛 ValueError，由上层转为待判定而非静默丢弃。"""
    if text is None:
        raise ValueError("empty version")
    raw = str(text).strip()
    if not raw or raw == "*":
        raise ValueError(f"unparsable version: {raw!r}")
    m = _VERSION_RE.match(raw)
    if not m:
        raise ValueError(f"unparsable version: {raw!r}")
    parts = tuple(int(g) for g in m.groups()[:3] if g is not None)
    prerelease = tuple(m.group(4).split(".")) if m.group(4) else None
    build = tuple(m.group(5).split(".")) if m.group(5) else None
    return Version(release=parts, prerelease=prerelease, build=build, raw=raw)


# ---------------------------------------------------------------------------
# 包身份规范
# ---------------------------------------------------------------------------
def canonical_ecosystem(ecosystem: Any) -> str:
    eco = str(ecosystem or "").strip().lower()
    return _ECOSYSTEM_ALIASES.get(eco, eco)


def canonical_name(name: Any, ecosystem: str = "") -> str:
    n = str(name or "").strip()
    if ecosystem in _CASEFOLD_ECOSYSTEMS:
        n = n.lower()
    return n


def _split_maven_coords(name: str) -> str:
    # Maven 公告可能给 groupId:artifactId，也可能只给 artifactId
    return name.split(":", 1)[1] if ":" in name else name


def canonical_identity(ecosystem: Any, name: Any) -> tuple[str, str]:
    eco = canonical_ecosystem(ecosystem)
    nm = canonical_name(name, eco)
    if eco == "maven":
        nm = _split_maven_coords(nm)
    return eco, nm


# ---------------------------------------------------------------------------
# 受影响区间解析
#
# 区间内部契约（JSON 可持久化）：
#   {"intervals": [{"inclusive_lower": str|None,
#                   "exclusive_upper": str|None,
#                   "inclusive_upper": str|None}],
#    "unbounded": bool, "raw": <原始片段>}
# ---------------------------------------------------------------------------
def _empty_interval() -> dict:
    return {"inclusive_lower": None, "exclusive_upper": None, "inclusive_upper": None}


def _unbounded_range(raw: Any) -> dict:
    return {"intervals": [_empty_interval()], "unbounded": True, "raw": raw}


def parse_constraints(text: Any) -> dict:
    """解析约束字符串，如 ">=1.0.0 <2.0.0"、"=1.2.3"、"*"。"""
    raw = str(text or "").strip()
    if not raw or raw == "*":
        return _unbounded_range(raw)
    iv = _empty_interval()
    for part in re.split(r"[,\s]+", raw):
        part = part.strip()
        if not part:
            continue
        m = _CONSTRAINT_RE.match(part)
        if not m:
            raise ValueError(f"unparsable constraint: {part!r}")
        op, ver = m.group(1) or "=", m.group(2)
        # 提前校验边界版本可解析
        parse_version(ver)
        if op in (">=",):
            iv["inclusive_lower"] = ver
        elif op == ">":
            iv["inclusive_lower"] = ver  # 下界严格性在比较时处理
            iv["lower_exclusive"] = True
        elif op == "<":
            iv["exclusive_upper"] = ver
        elif op == "<=":
            iv["inclusive_upper"] = ver
        elif op in ("=", "=="):
            iv["inclusive_lower"] = ver
            iv["inclusive_upper"] = ver
        elif op == "!=":
            raise ValueError(f"unsupported != constraint: {part!r}")
        else:  # pragma: no cover - 正则已限定操作符
            raise ValueError(f"unsupported operator: {op!r}")
    iv.setdefault("lower_exclusive", False)
    return {"intervals": [iv], "unbounded": False, "raw": raw}


def parse_events(events: Iterable[dict]) -> dict:
    """解析 OSV 风格事件序列：introduced / fixed / last_affected。"""
    intervals: list[dict] = []
    cur = _empty_interval()
    started = False
    for ev in events:
        if "introduced" in ev:
            if started:
                intervals.append(cur)
            cur = _empty_interval()
            started = True
            intro = str(ev["introduced"])
            if intro != "0":  # OSV 约定 0 表示区间起点
                cur["inclusive_lower"] = intro
            cur["lower_exclusive"] = False
        elif "fixed" in ev:
            cur["exclusive_upper"] = str(ev["fixed"])
            intervals.append(cur)
            cur = _empty_interval()
            started = False
        elif "last_affected" in ev:
            cur["inclusive_upper"] = str(ev["last_affected"])
            intervals.append(cur)
            cur = _empty_interval()
            started = False
    if started:
        intervals.append(cur)
    if not intervals:
        intervals = [_empty_interval()]
    for iv in intervals:
        iv.setdefault("lower_exclusive", False)
    return {"intervals": intervals, "unbounded": False, "raw": list(events)}


def parse_range(spec: Any) -> dict:
    """把单条范围规格（事件序列 / 约束串 / 区间 dict）规范为内部区间契约。"""
    if spec is None:
        return _unbounded_range(spec)
    if isinstance(spec, str):
        return parse_constraints(spec)
    if isinstance(spec, dict) and "events" in spec:
        return parse_events(spec["events"])
    if isinstance(spec, dict) and {"introduced", "fixed", "last_affected"} & set(spec):
        return parse_events([spec])
    if isinstance(spec, dict) and "constraints" in spec:
        return parse_constraints(spec["constraints"])
    if isinstance(spec, (list, tuple)) and spec and all(isinstance(e, dict) for e in spec):
        return parse_events(spec)
    raise ValueError(f"unparsable range spec: {spec!r}")


def range_contains(rng: dict, version: Version) -> bool:
    """版本是否落在受影响区间内；边界版本不可解析时抛 ValueError。"""
    if rng.get("unbounded"):
        return True
    for iv in rng["intervals"]:
        ok = True
        low = iv.get("inclusive_lower")
        if low is not None:
            low_v = parse_version(low)
            ok = version > low_v if iv.get("lower_exclusive") else version >= low_v
        if ok and iv.get("exclusive_upper") is not None:
            ok = version < parse_version(iv["exclusive_upper"])
        if ok and iv.get("inclusive_upper") is not None:
            ok = version <= parse_version(iv["inclusive_upper"])
        if ok:
            return True
    return False


# ---------------------------------------------------------------------------
# 公告记录解析（纯函数；即旧入口 process 的实现本体）
# ---------------------------------------------------------------------------
def _is_advisory(record: dict) -> bool:
    return bool(record.get("advisory_id") or record.get("withdrawn")
                or record.get("kind") in ("advisory", "withdrawn"))


def parse_advisory(record: dict) -> dict:
    """把一条公告原始记录规范为公告侧事实（无副作用、可独立测试）。"""
    rec = dict(record)
    if not _is_advisory(rec):
        # 非公告记录：保留旧入口的透传行为，只打阶段标记
        rec.setdefault("_stage_advisory_map", "ok")
        return rec

    advisory_key = str(rec["advisory_id"])
    source = str(rec.get("source", "")).strip().lower() or "unknown"
    ts = str(rec.get("ts", ""))
    withdrawn = bool(rec.get("withdrawn")) or str(rec.get("kind", "")).lower() == EVENT_WITHDRAWN
    event = EVENT_WITHDRAWN if withdrawn else EVENT_BODY
    eco, name = canonical_identity(rec.get("ecosystem"), rec.get("package"))

    identities: list[dict] = []
    if name:
        identities.append({"ecosystem": eco, "name": name})
    aliases: list[str] = []
    for alias in rec.get("aliases", []) or []:
        an = canonical_name(alias, eco)
        if an and an != name:
            aliases.append(an)
            identities.append({"ecosystem": eco, "name": an})

    ranges: list[dict] = []
    evidence = "parsed"
    if not withdrawn:
        if rec.get("no_constraints") or not any(
                k in rec for k in ("ranges", "range", "constraints", "last_affected")):
            # 无版本约束：任何版本都受影响，显式记录为无界区间
            ranges = [_unbounded_range(rec.get("constraints", "*"))]
        else:
            specs = rec.get("ranges")
            if specs is None:
                specs = [rec.get("range") if rec.get("range") is not None
                         else rec.get("constraints", rec.get("last_affected"))]
            if isinstance(specs, dict):
                specs = [specs]
            for spec in specs:
                try:
                    ranges.append(parse_range(spec))
                except ValueError:
                    evidence = "unparsed_range"
                    ranges.append({"intervals": [], "unbounded": False, "raw": spec})
    return {
        "kind": "advisory",
        "advisory_key": advisory_key,
        "source": source,
        "ts": ts,
        "event": event,
        "withdrawn": withdrawn,
        "ecosystem": eco,
        "name": name,
        "aliases": aliases,
        "identities": identities,
        "ranges": ranges,
        "has_constraints": bool(ranges) and not withdrawn,
        "evidence": evidence,
        "raw": record,
    }


def process(record):
    """旧调用入口：单条记录的公告侧规范/解析（无 IO、无副作用）。"""
    return parse_advisory(record)


# ---------------------------------------------------------------------------
# 持久化层：复用项目 sqlite 契约，独立表名，稳定 key 幂等 upsert
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS am_advisories(
    advisory_key TEXT NOT NULL,
    source       TEXT NOT NULL,
    ts           TEXT NOT NULL,
    ecosystem    TEXT NOT NULL,
    package_name TEXT NOT NULL,
    data         TEXT NOT NULL,
    PRIMARY KEY (advisory_key, source, ts)
);
CREATE TABLE IF NOT EXISTS am_aliases(
    ecosystem      TEXT NOT NULL,
    name           TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    source         TEXT NOT NULL,
    PRIMARY KEY (ecosystem, name, source)
);
CREATE TABLE IF NOT EXISTS am_pending(
    advisory_key TEXT NOT NULL,
    ecosystem    TEXT NOT NULL,
    package_name TEXT NOT NULL,
    reason       TEXT NOT NULL,
    source       TEXT NOT NULL,
    ts           TEXT NOT NULL,
    data         TEXT NOT NULL,
    PRIMARY KEY (advisory_key, reason, source, ts)
);
CREATE TABLE IF NOT EXISTS am_matches(
    match_key TEXT PRIMARY KEY,
    data      TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:  # pragma: no cover - 便捷封装
    con = _store.connect()
    ensure_schema(con)
    return con


def ensure_schema(con) -> None:
    con.executescript(_SCHEMA)


def _rows(con, sql: str, params=()):
    return con.execute(sql, params).fetchall()


def _load_advisories(con) -> list[dict]:
    return [json.loads(d) for d, in _rows(con, "SELECT data FROM am_advisories")]


def _load_pending(con) -> list[dict]:
    return [json.loads(d) for d, in _rows(con, "SELECT data FROM am_pending")]


def ingest_alias(con, ecosystem: str, name: str, canonical_name_: str,
                 source: str = "manual") -> dict:
    """登记一条包别名事实（公告外的别名来源也可独立入库）。"""
    ensure_schema(con)
    eco = canonical_ecosystem(ecosystem)
    row = {
        "ecosystem": eco,
        "name": canonical_name(name, eco),
        "canonical_name": canonical_name(canonical_name_, eco),
        "source": str(source),
    }
    con.execute(
        "INSERT INTO am_aliases(ecosystem,name,canonical_name,source) VALUES(?,?,?,?) "
        "ON CONFLICT(ecosystem,name,source) DO UPDATE SET canonical_name=excluded.canonical_name",
        (row["ecosystem"], row["name"], row["canonical_name"], row["source"]))
    return row


def _upsert_pending(con, parsed: dict, reason: str) -> None:
    payload = {
        "advisory_key": parsed["advisory_key"],
        "ecosystem": parsed["ecosystem"],
        "package_name": parsed["name"],
        "reason": reason,
        "source": parsed["source"],
        "ts": parsed["ts"],
        "data": parsed,
    }
    con.execute(
        "INSERT INTO am_pending(advisory_key,ecosystem,package_name,reason,source,ts,data) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(advisory_key,reason,source,ts) DO UPDATE SET "
        "ecosystem=excluded.ecosystem, package_name=excluded.package_name, data=excluded.data",
        (payload["advisory_key"], payload["ecosystem"], payload["package_name"], reason,
         parsed["source"], parsed["ts"], json.dumps(payload, ensure_ascii=False)))


def _refresh_withdrawn_matches(con, advisory_key: str, withdrawal: dict) -> None:
    """撤回到达后，把先前落库的受影响判断有据更新为 withdrawn。"""
    ev = {"event": EVENT_WITHDRAWN, "source": withdrawal["source"], "ts": withdrawal["ts"]}
    for match_key, data in _rows(
            con, "SELECT match_key,data FROM am_matches"):
        match = json.loads(data)
        if match.get("advisory", {}).get("advisory_key") != advisory_key:
            continue
        if match["verdict"] == WITHDRAWN and ev in match["history"]:
            continue
        match["verdict"] = WITHDRAWN
        match["status"] = WITHDRAWN
        if ev not in match["history"]:
            match["history"].append(ev)
        con.execute("UPDATE am_matches SET data=? WHERE match_key=?",
                    (json.dumps(match, ensure_ascii=False), match_key))


def ingest_advisory(con, record: dict) -> dict:
    """摄入一条公告事件（正文或撤回），维护证据状态与待判定记录。"""
    ensure_schema(con)
    parsed = parse_advisory(record)
    if not parsed.get("advisory_key"):
        return parsed

    con.execute(
        "INSERT INTO am_advisories(advisory_key,source,ts,ecosystem,package_name,data) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(advisory_key,source,ts) DO UPDATE SET "
        "ecosystem=excluded.ecosystem, package_name=excluded.package_name, data=excluded.data",
        (parsed["advisory_key"], parsed["source"], parsed["ts"],
         parsed["ecosystem"], parsed["name"],
         json.dumps(parsed, ensure_ascii=False)))

    for alias in parsed["aliases"]:
        ingest_alias(con, parsed["ecosystem"], alias, parsed["name"], parsed["source"])

    if parsed["event"] == EVENT_WITHDRAWN:
        body_already_seen = any(
            r["advisory_key"] == parsed["advisory_key"] and r["event"] == EVENT_BODY
            for r in _load_advisories(con))
        if body_already_seen:
            # 先正文后撤回：不是待判定，直接把既有判断有据更新
            _refresh_withdrawn_matches(con, parsed["advisory_key"], parsed)
        else:
            # 先收到撤回、正文尚未到达：登记待判定
            _upsert_pending(con, parsed, "withdrawn_before_body")
    else:
        prior_withdrawal = any(
            p["advisory_key"] == parsed["advisory_key"]
            and p["reason"] == "withdrawn_before_body" for p in _load_pending(con))
        if prior_withdrawal:
            # 先收到撤回再收到正文：保持待判定，直到显式重评（evaluate_all）
            _upsert_pending(con, parsed, "body_after_withdrawn")
        if parsed["evidence"] != "parsed":
            _upsert_pending(con, parsed, parsed["evidence"])
    return parsed


# ---------------------------------------------------------------------------
# 匹配：公告影响语义 → 依赖记录契约的可追溯匹配依据
# ---------------------------------------------------------------------------
def make_match_key(dep: dict, advisory_key: str) -> str:
    eco, name = canonical_identity(dep.get("ecosystem"), dep.get("package"))
    raw = "|".join([eco, name, str(dep.get("version", "")), advisory_key])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _identity_index(advisories: list[dict], alias_rows: list[tuple]) -> dict[tuple, set[str]]:
    index: dict[tuple, set[str]] = {}
    for adv in advisories:
        if adv["event"] != EVENT_BODY:
            continue
        for ident in adv["identities"]:
            index.setdefault((ident["ecosystem"], ident["name"]), set()).add(adv["advisory_key"])
    for eco, alias_name, canonical, _src in alias_rows:
        for adv in advisories:
            if adv["event"] != EVENT_BODY or adv["ecosystem"] != eco:
                continue
            if adv["name"] == canonical:
                index.setdefault((eco, alias_name), set()).add(adv["advisory_key"])
    return index


def _group_by_key(advisories: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for adv in advisories:
        grouped.setdefault(adv["advisory_key"], []).append(adv)
    return grouped


def _withdrawn_pending_keys(con) -> set[str]:
    return {p["advisory_key"] for p in _load_pending(con)
            if p["reason"] in ("withdrawn_before_body", "body_after_withdrawn")}


def evaluate_dependency(con, dep: dict) -> list[dict]:
    """对单条本地依赖评估全部相关公告，返回匹配依据并幂等落库。"""
    ensure_schema(con)
    advisories = _load_advisories(con)
    alias_rows = _rows(con, "SELECT ecosystem,name,canonical_name,source FROM am_aliases")
    index = _identity_index(advisories, alias_rows)
    grouped = _group_by_key(advisories)
    withdrawn_keys = _withdrawn_pending_keys(con)

    eco, name = canonical_identity(dep.get("ecosystem"), dep.get("package"))
    dep_version_raw = str(dep.get("version", ""))
    try:
        dep_version = parse_version(dep_version_raw)
        version_error = None
    except ValueError as exc:
        dep_version = None
        version_error = str(exc)

    out: list[dict] = []
    for advisory_key in sorted(index.get((eco, name), ())):
        rows = grouped[advisory_key]
        body_rows = [r for r in rows if r["event"] == EVENT_BODY]
        withdrawn_rows = [r for r in rows if r["event"] == EVENT_WITHDRAWN]
        matched_name = name
        canonical = body_rows[0]["name"] if body_rows else name
        # 命中名与规范名不同（公告内联别名或 am_aliases 别名表）即别名关联
        via_alias = matched_name != canonical or any(
            name in r["aliases"] for r in body_rows)

        history = [{"event": r["event"], "source": r["source"], "ts": r["ts"]}
                   for r in sorted(rows, key=lambda r: (r["ts"], r["source"]))]
        sources = sorted({r["source"] for r in rows})

        # 证据状态优先：撤回（含先撤回后正文）使先前/当前判断整体失效
        if withdrawn_rows or advisory_key in withdrawn_keys:
            verdict = WITHDRAWN
            conflict = ({"kind": "withdrawn_vs_active",
                         "detail": "active body remains while withdrawal was received"}
                        if body_rows else None)
            range_checks = [{"source": r["source"], "skipped": "withdrawn"}
                            for r in body_rows]
        elif version_error is not None:
            verdict = PENDING
            conflict = None
            range_checks = [{"source": r["source"], "error": version_error}
                            for r in body_rows]
        else:
            range_checks, per_source_verdict = [], {}
            any_unparsed = False
            for r in body_rows:
                if r["evidence"] != "parsed":
                    any_unparsed = True
                    per_source_verdict[r["source"]] = PENDING
                    range_checks.append({"source": r["source"],
                                         "error": "unparsed_range",
                                         "raw": [rg["raw"] for rg in r["ranges"]]})
                    continue
                hits, details = False, []
                for rng in r["ranges"]:
                    try:
                        contained = range_contains(rng, dep_version)
                    except ValueError as exc:
                        any_unparsed = True
                        details.append({"raw": rng["raw"], "error": str(exc)})
                        continue
                    hits = hits or contained
                    details.append({"raw": rng["raw"], "unbounded": rng["unbounded"],
                                    "contains": contained})
                per_source_verdict[r["source"]] = AFFECTED if hits else NOT_AFFECTED
                range_checks.append({"source": r["source"], "hits": hits, "ranges": details})
            if any_unparsed and all(v == PENDING for v in per_source_verdict.values()):
                verdict = PENDING
                conflict = None
            else:
                verdicts = set(per_source_verdict.values())
                verdict = AFFECTED if AFFECTED in verdicts else NOT_AFFECTED
                conflict = ({"kind": "source_conflict",
                             "detail": "sources disagree on affected status",
                             "verdicts": per_source_verdict}
                            if AFFECTED in verdicts and NOT_AFFECTED in verdicts else None)

        match = {
            "kind": "advisory_match",
            "match_key": make_match_key(dep, advisory_key),
            "dependency": {
                "ecosystem": eco,
                "package": name,
                "version": dep_version_raw,
                "dep_id": dep.get("id"),
            },
            "advisory": {
                "advisory_key": advisory_key,
                "sources": sources,
            },
            "identity": {
                "ecosystem": eco,
                "canonical_name": canonical,
                "matched_name": matched_name,
                "via_alias": via_alias,
            },
            "version_check": {
                "version": dep_version_raw,
                "prerelease": bool(dep_version and dep_version.prerelease is not None),
                "per_source": range_checks,
            },
            "verdict": verdict,
            "status": verdict,
            "conflict": conflict,
            "history": history,
        }
        con.execute(
            "INSERT INTO am_matches(match_key,data) VALUES(?,?) "
            "ON CONFLICT(match_key) DO UPDATE SET data=excluded.data",
            (match["match_key"], json.dumps(match, ensure_ascii=False)))
        out.append(match)
    return out


def evaluate_all(con, dependencies: Iterable[dict]) -> list[dict]:
    """评估一批本地依赖（同包多版本按多条依赖各自成行），返回全部匹配依据。"""
    ensure_schema(con)
    matches: list[dict] = []
    for dep in dependencies:
        matches.extend(evaluate_dependency(con, dep))
    return matches


def matches_for(con, advisory_key: str | None = None,
                dep_id: str | None = None) -> list[dict]:
    """读取已持久化的匹配依据（重启后结果一致）。"""
    ensure_schema(con)
    out = []
    for data, in _rows(con, "SELECT data FROM am_matches ORDER BY match_key"):
        match = json.loads(data)
        if advisory_key is not None and match["advisory"]["advisory_key"] != advisory_key:
            continue
        if dep_id is not None and match["dependency"].get("dep_id") != dep_id:
            continue
        out.append(match)
    return out


def pending_records(con) -> list[dict]:
    """待判定记录：先撤回后正文 / 正文不可解析 / 依赖版本不可解析的匹配。"""
    ensure_schema(con)
    items = _load_pending(con)
    for match in matches_for(con):
        if match["verdict"] == PENDING:
            items.append({
                "advisory_key": match["advisory"]["advisory_key"],
                "ecosystem": match["identity"]["ecosystem"],
                "package_name": match["identity"]["matched_name"],
                "reason": "pending_match",
                "source": ",".join(match["advisory"]["sources"]),
                "ts": "",
                "data": match,
            })
    return items
