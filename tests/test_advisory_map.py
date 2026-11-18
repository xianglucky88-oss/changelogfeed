"""advisory_map：公告影响语义与本地依赖关联的测试。

覆盖：规范包名/生态、版本范围与受影响区间（含预发布临界、无版本约束）、
别名、撤回（先撤回后正文 / 先正文后撤回）、多来源冲突、
同名不同生态、同包多版本、重复运行幂等、持久化与重启后一致、旧调用入口保留。
"""
import json
import sqlite3
from pathlib import Path

import pytest

from src.changelogfeed import advisory_map as am
from src.changelogfeed import store

FIX_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "advisory_map"


@pytest.fixture()
def con(tmp_path, monkeypatch):
    db = tmp_path / "am_test.db"
    monkeypatch.setattr(am._store, "DB_PATH", str(db))
    monkeypatch.setattr(store, "DB_PATH", str(db))
    connection = store.connect()
    am.ensure_schema(connection)
    yield connection
    connection.close()


@pytest.fixture()
def advisories():
    return json.loads((FIX_DIR / "advisories.json").read_text(encoding="utf-8"))


@pytest.fixture()
def dependencies():
    return json.loads((FIX_DIR / "dependencies.json").read_text(encoding="utf-8"))


def _by_dep(matches):
    return {m["dependency"]["dep_id"]: m for m in matches}


def _ingest_all(connection, advisories):
    for rec in advisories:
        am.ingest_advisory(connection, rec)


# --------------------------------------------------------------------------
# 旧调用入口保留：无副作用的单条规范/解析
# --------------------------------------------------------------------------
def test_legacy_process_entry_still_works_and_pure(advisories):
    body = am.process(advisories[0])
    assert body["advisory_key"] == "ADV-ALIAS"
    assert body["ecosystem"] == "pypi"
    assert body["name"] == "pyyaml"
    # 大小写写法归一后与规范名重合被去重，真正的异名别名保留
    assert body["aliases"] == ["yaml-parser"]
    # 非公告记录保持透传，不抛异常
    plain = am.process({"id": "r-001", "src": "alpha", "name": " Record One "})
    assert plain["id"] == "r-001" and plain["_stage_advisory_map"] == "ok"


def test_canonical_identity_ecosystem_and_name():
    assert am.canonical_identity("PyPI", "PyYAML") == ("pypi", "pyyaml")
    assert am.canonical_identity("node", "LeftPad") == ("npm", "LeftPad")
    assert am.canonical_identity("Maven", "com.example:nover") == ("maven", "nover")
    assert am.canonical_identity("golang", "x/y") == ("go", "x/y")


# --------------------------------------------------------------------------
# 版本与区间：预发布 precedence、临界版本、无版本约束
# --------------------------------------------------------------------------
def test_prerelease_ordering_and_boundary():
    rc1, rc2, rel = (am.parse_version(v) for v in ("1.0.0-rc.1", "1.0.0-rc.2", "1.0.0"))
    assert rc1 < rc2 < rel
    rng = am.parse_range({"events": [
        {"introduced": "1.0.0-rc.1"}, {"fixed": "1.0.0-rc.2"}]})
    assert am.range_contains(rng, rc1) is True       # introduced 含边界
    assert am.range_contains(rng, rc2) is False      # fixed 不含边界
    assert am.range_contains(rng, rel) is False


def test_constraint_string_and_last_affected():
    rng = am.parse_range(">=5.3 <5.4")
    assert am.range_contains(rng, am.parse_version("5.3.1"))
    assert not am.range_contains(rng, am.parse_version("5.4.0"))
    rng2 = am.parse_range({"events": [{"introduced": "0"}, {"last_affected": "2.0.0"}]})
    assert am.range_contains(rng2, am.parse_version("2.0.0"))
    assert not am.range_contains(rng2, am.parse_version("2.0.1"))


def test_unbounded_range_matches_every_version():
    rng = am.parse_range(None)
    assert rng["unbounded"] is True
    assert am.range_contains(rng, am.parse_version("99.99.99"))
    parsed = am.parse_advisory({"advisory_id": "X", "ecosystem": "pypi",
                                "package": "p", "no_constraints": True})
    assert all(r["unbounded"] for r in parsed["ranges"])


def test_unparsable_range_becomes_pending(con):
    am.ingest_advisory(con, {"advisory_id": "ADV-BAD", "source": "s", "ts": "t",
                             "ecosystem": "pypi", "package": "p",
                             "constraints": "not-a-range>>"})
    out = am.evaluate_dependency(con, {"id": "d", "ecosystem": "pypi",
                                       "package": "p", "version": "1.0.0"})
    assert out[0]["verdict"] == am.PENDING
    assert any(p["reason"] == "unparsed_range" for p in am.pending_records(con))


# --------------------------------------------------------------------------
# 别名：公告别名命中本地依赖，匹配依据可追溯
# --------------------------------------------------------------------------
def test_alias_match_basis(con, advisories, dependencies):
    _ingest_all(con, advisories)
    matches = am.evaluate_all(con, dependencies)
    by = _by_dep(matches)
    hit = by["dep-pyyaml-hit"]
    assert hit["verdict"] == am.AFFECTED
    assert hit["identity"]["canonical_name"] == "pyyaml"
    miss = by["dep-pyyaml-miss"]
    assert miss["verdict"] == am.NOT_AFFECTED
    alias_hit = by["dep-pyyaml-alias"]
    assert alias_hit["verdict"] == am.AFFECTED
    assert alias_hit["identity"]["via_alias"] is True
    assert alias_hit["identity"]["canonical_name"] == "pyyaml"
    assert alias_hit["identity"]["matched_name"] == "yaml-parser"
    assert isinstance(hit["version_check"]["per_source"], list)


# --------------------------------------------------------------------------
# 同名不同生态：不跨生态关联
# --------------------------------------------------------------------------
def test_same_name_different_ecosystem_no_cross_match(con, advisories, dependencies):
    _ingest_all(con, advisories)
    matches = am.evaluate_all(con, dependencies)
    by = _by_dep(matches)
    npm_hit = by["dep-yaml-npm-hit"]
    assert npm_hit["advisory"]["advisory_key"] == "ADV-CROSS-ECO"
    assert npm_hit["verdict"] == am.AFFECTED
    assert by["dep-yaml-npm-miss"]["verdict"] == am.NOT_AFFECTED
    # pypi 的 yaml 1.5.0 与 npm 公告同包名但生态不同 -> 无任何匹配行
    assert "dep-yaml-pypi-cross" not in by


def test_prerelease_critical_versions(con, advisories, dependencies):
    _ingest_all(con, advisories)
    by = _by_dep(am.evaluate_all(con, dependencies))
    assert by["dep-pre-rc1"]["verdict"] == am.AFFECTED
    assert by["dep-pre-rc2"]["verdict"] == am.NOT_AFFECTED
    assert by["dep-pre-rel"]["verdict"] == am.NOT_AFFECTED
    assert by["dep-pre-rc1"]["version_check"]["prerelease"] is True


def test_no_version_constraint_affects_all(con, advisories, dependencies):
    _ingest_all(con, advisories)
    by = _by_dep(am.evaluate_all(con, dependencies))
    for dep_id in ("dep-nover-a", "dep-nover-b"):
        assert by[dep_id]["verdict"] == am.AFFECTED
        assert by[dep_id]["version_check"]["per_source"][0]["ranges"][0]["unbounded"]


# --------------------------------------------------------------------------
# 多来源冲突：保留各方证据与冲突标记
# --------------------------------------------------------------------------
def test_multi_source_conflict_keeps_evidence(con, advisories, dependencies):
    _ingest_all(con, advisories)
    by = _by_dep(am.evaluate_all(con, dependencies))
    c15 = by["dep-conf-15"]
    assert c15["verdict"] == am.AFFECTED
    assert c15["conflict"] is None or c15["conflict"]["kind"] == "source_conflict"
    c35 = by["dep-conf-35"]
    assert c35["conflict"]["kind"] == "source_conflict"
    verdicts = c35["conflict"]["verdicts"]
    assert verdicts["ghsa"] == am.NOT_AFFECTED and verdicts["osv"] == am.AFFECTED
    # 任何来源命中即判受影响，且每来源区间证据都被保留
    assert c35["verdict"] == am.AFFECTED
    assert len(c35["version_check"]["per_source"]) == 2
    assert by["dep-conf-25"]["verdict"] == am.NOT_AFFECTED


# --------------------------------------------------------------------------
# 撤回：先收到撤回再收到正文（待判定）；先正文后撤回（有据更新）
# --------------------------------------------------------------------------
def test_withdrawal_before_body_stays_pending(con, advisories, dependencies):
    ordered = sorted(advisories, key=lambda r: r["ts"])
    _ingest_all(con, ordered)
    pending = am.pending_records(con)
    keys = {p["advisory_key"] for p in pending}
    assert "ADV-EARLY-W" in keys
    by = _by_dep(am.evaluate_all(con, dependencies))
    early = by["dep-early-11"]
    assert early["verdict"] == am.WITHDRAWN
    assert early["status"] == am.WITHDRAWN
    assert any(h["event"] == am.EVENT_WITHDRAWN for h in early["history"])


def test_withdrawal_after_body_updates_prior_judgement(con, advisories, dependencies):
    # 只先摄入 ADV-LATE 正文（排除撤回事件）
    for rec in advisories:
        if rec["advisory_id"] == "ADV-LATE" and not rec.get("withdrawn"):
            am.ingest_advisory(con, rec)
    dep = next(d for d in dependencies if d["id"] == "dep-late-21")
    before = am.evaluate_dependency(con, dep)[0]
    assert before["verdict"] == am.AFFECTED
    # 撤回到达：先前受影响判断必须有据更新
    for rec in advisories:
        if rec["advisory_id"] == "ADV-LATE" and rec.get("withdrawn"):
            am.ingest_advisory(con, rec)
    after = am.matches_for(con, advisory_key="ADV-LATE")[0]
    assert after["verdict"] == am.WITHDRAWN
    assert len(after["history"]) == 2
    # 重新评估保持撤回结论，且证据链完整
    again = am.evaluate_dependency(con, dep)[0]
    assert again["verdict"] == am.WITHDRAWN and again["match_key"] == before["match_key"]


def test_pending_cleared_on_full_evaluation_semantics(con, advisories, dependencies):
    ordered = sorted(advisories, key=lambda r: r["ts"])
    _ingest_all(con, ordered)
    am.evaluate_all(con, dependencies)
    pending_keys = {p["advisory_key"] for p in am.pending_records(con)}
    assert "ADV-LATE" not in pending_keys      # 正文+撤回成对，判定为 withdrawn 而非 pending
    assert "ADV-EARLY-W" in pending_keys       # 撤回早于正文，保持待判定


# --------------------------------------------------------------------------
# 同包多版本：每个 (依赖, 公告) 独立成行
# --------------------------------------------------------------------------
def test_same_package_multiple_versions_separate_rows(con):
    am.ingest_advisory(con, {"advisory_id": "ADV-MV", "source": "s", "ts": "t",
                             "ecosystem": "pypi", "package": "mv",
                             "constraints": ">=1.0 <2.0"})
    deps = [
        {"id": "mv-1", "ecosystem": "pypi", "package": "mv", "version": "1.0.0"},
        {"id": "mv-2", "ecosystem": "pypi", "package": "mv", "version": "2.0.0"},
    ]
    rows = am.evaluate_all(con, deps)
    assert len(rows) == 2
    assert {r["dependency"]["dep_id"] for r in rows} == {"mv-1", "mv-2"}
    verdicts = {r["dependency"]["dep_id"]: r["verdict"] for r in rows}
    assert verdicts == {"mv-1": am.AFFECTED, "mv-2": am.NOT_AFFECTED}
    # 稳定 key 不随运行变化
    assert am.make_match_key(deps[0], "ADV-MV") == rows[0]["match_key"]


# --------------------------------------------------------------------------
# 重复运行幂等 + 持久化与重启一致
# --------------------------------------------------------------------------
def test_repeated_runs_are_idempotent(con, advisories, dependencies):
    _ingest_all(con, advisories)
    first = am.evaluate_all(con, dependencies)
    keys_first = {m["match_key"] for m in first}
    # 再摄入一遍公告、再评估一遍：匹配行数与 key 完全不变
    _ingest_all(con, advisories)
    second = am.evaluate_all(con, dependencies)
    assert {m["match_key"] for m in second} == keys_first
    assert len(am.matches_for(con)) == len(keys_first)
    # 内容也应逐 key 一致
    assert {m["match_key"]: m for m in first} == {m["match_key"]: m for m in second}


def test_persistence_survives_restart(con, advisories, dependencies, tmp_path):
    _ingest_all(con, advisories)
    am.evaluate_all(con, dependencies)
    before = am.matches_for(con)
    pending_before = am.pending_records(con)
    con.close()
    # 模拟重启：重新连接同一 sqlite 文件，结果一致
    restarted = sqlite3.connect(str(tmp_path / "am_test.db"))
    am.ensure_schema(restarted)
    after = am.matches_for(restarted)
    assert after == before
    pending_after = am.pending_records(restarted)
    assert {(p["advisory_key"], p["reason"]) for p in pending_after} == \
           {(p["advisory_key"], p["reason"]) for p in pending_before}
    restarted.close()


def test_explicit_alias_registry_and_unparsable_dep_version(con):
    am.ingest_advisory(con, {"advisory_id": "A1", "source": "s", "ts": "t",
                             "ecosystem": "npm", "package": "left-pad",
                             "constraints": ">=1.0 <2.0"})
    am.ingest_alias(con, "node", "leftpad", "left-pad", source="alias-registry")
    rows = am.evaluate_dependency(con, {"id": "d1", "ecosystem": "node",
                                        "package": "leftpad", "version": "1.5.0"})
    assert rows[0]["verdict"] == am.AFFECTED
    assert rows[0]["identity"]["via_alias"] is True
    bad = am.evaluate_dependency(con, {"id": "d2", "ecosystem": "npm",
                                       "package": "left-pad", "version": "weird#"})
    assert bad[0]["verdict"] == am.PENDING
    assert any(p["reason"] == "pending_match" for p in am.pending_records(con))


# --------------------------------------------------------------------------
# relate 消费 advisory_map，不另造关联流程；无连接时保持旧行为
# --------------------------------------------------------------------------
def test_relate_consumes_advisory_mappings(con, advisories, dependencies):
    from src.changelogfeed import relate
    _ingest_all(con, advisories)
    dep = next(d for d in dependencies if d["id"] == "dep-pyyaml-hit")
    enriched = relate.process(dep, con=con)
    assert enriched["_stage_relate"] == "ok"
    assert enriched["_advisory_matches"][0]["verdict"] == am.AFFECTED
    # 旧的单参数入口仍可用
    plain = relate.process({"id": "x", "name": "n"})
    assert plain["_stage_relate"] == "ok" and "_advisory_matches" not in plain
