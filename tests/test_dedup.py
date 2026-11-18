"""dedup 多源归并测试：四类事件穿插重放、乱序、撤回、镜像缺字段、
页面改版、半写入恢复、规则升级回放与下游触发幂等。"""
import json
import sqlite3
from pathlib import Path

from src.changelogfeed import pipeline, semver, store
from src.changelogfeed.dedup import Deduper, canonical_url

REPLAY = Path(__file__).resolve().parents[1] / "fixtures" / "replay_events.json"


# ---------- 辅助 ----------
def fresh_con(tmp_path):
    con = sqlite3.connect(tmp_path / "t.db")
    store.init(con)
    return con


def run_pipeline(con, fixture=REPLAY):
    return pipeline.run(con=con, fixture=fixture)


def normalized_events():
    """经过 normalizer/feed_pull/semver 后的记录，模拟真实流水线输入。"""
    from src.changelogfeed import normalizer, feed_pull
    out = []
    for raw in json.loads(REPLAY.read_text(encoding="utf-8")):
        rec = semver.process(feed_pull.process(normalizer.process(raw)))
        out.append(rec)
    return out


def feed(deduper, events):
    """逐条喂给 deduper，返回 (下游输出列表, 非触发数, 触发数)。

    非触发包含两类：原样重传（retransmit，返回 None）与来源补登记
    （source_add，返回归并记录但 triggered=False），二者都不产生下游触发；
    通过 journal 可进一步区分。
    """
    emitted, suppressed, triggered = [], 0, 0
    for ev in events:
        result, did_trigger = deduper.handle(ev)
        if result is None:
            suppressed += 1
        else:
            emitted.append(result)
        if did_trigger:
            triggered += 1
    return emitted, suppressed, triggered


def group_by_component(deduper):
    return {state["component"]: state for _gid, state in deduper.list_groups()}


# ---------- 1. 四阶段穿插重放：首次发现时间 / 版本链 / 触发次数 ----------
def test_interleaved_replay_first_seen_chain_and_triggers(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    emitted, suppressed, _triggered = feed(deduper, normalized_events())
    con.commit()

    groups = dict(deduper.list_groups())
    assert len(groups) == 3  # pkg-alpha / pkg-beta / pkg-gamma 各一个发布身份

    alpha = group_by_component(deduper)["pkg-alpha"]
    # 首次发现时间保留最早一份证据（官方 release）
    assert alpha["first_seen"] == "2026-11-01T08:00:00Z"
    # 三类来源关系都保留，且镜像源缺字段被记录
    assert set(alpha["sources"]) == {"release", "changelog", "advisory"}
    assert "rid" in alpha["sources"]["advisory"]["missing_fields"]
    assert "url" in alpha["sources"]["advisory"]["missing_fields"]
    assert "body" in alpha["sources"]["advisory"]["missing_fields"]
    # 修订证据链：一次修订
    assert alpha["revision_seq"] == 1
    assert alpha["revision_of"][0]["event_id"] == "e-005"
    # 撤回留痕
    assert alpha["withdrawn"] is True
    assert alpha["withdrawn_event_id"] == "e-006"
    assert alpha["status"] == "withdrawn"
    # 下游触发：首次发现 1 + 修订 1 + 撤回 1 = 3（重传不触发）
    assert alpha["triggers"] == 3
    # 11 条事件：7 条触发下游（首见/修订/撤回，含乱序补登的首见），
    # 3 条来源补登记（返回归并记录但不触发），1 条原样重传 e-004 被抑制；
    # 全部事件都保留来源关系与流水。
    assert suppressed == 1
    assert _triggered == 7
    assert len(emitted) == 10  # 触发 7 + 来源补登记 3
    assert alpha["sources"]["release"]["events"].count("e-004") == 1

    beta = group_by_component(deduper)["pkg-beta"]
    # 乱序：撤回先到、发布后到，最终仍归并为同一发布
    assert beta["first_seen"] == "2026-11-04T08:00:00Z"  # 最早到达的是通告
    assert beta["withdrawn"] is True
    assert set(beta["sources"]) == {"advisory", "release", "changelog"}
    # 版本链按语义版本排序（即便事件乱序到达）
    versions = [node["version"] for node in beta["version_chain"]]
    assert versions == ["2.4.0"]
    # 下游触发：撤回 1 + 首份发布证据补登的“首次发现”1 = 2；
    # 变更日志拷贝为来源补登记，不触发；乱序下结果与顺序无关。
    assert beta["triggers"] == 2
    assert beta["revision_seq"] == 0

    gamma = group_by_component(deduper)["pkg-gamma"]
    versions = [node["version"] for node in gamma["version_chain"]]
    assert versions == ["0.9.0", "1.0.0"]  # 版本链有序
    assert gamma["revision_seq"] == 1
    assert gamma["triggers"] == 2  # 首次 + 版本修订

    # 下游触发台账：alpha 3（首见/修订/撤回）+ beta 2（撤回/乱序补登首见）
    # + gamma 2（首见/版本修订）= 7 行，与输出记录数一致
    assert len(deduper.emission_rows()) == 7
    assert deduper.trigger_count() == 7
    # 11 条事件全部留痕（含被抑制的重传）
    assert deduper.journal_len() == 11


# ---------- 2. 重复执行不得重复触发下游 ----------
def test_rerun_is_idempotent_no_duplicate_downstream(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    first_out, first_suppressed, _ = feed(deduper, normalized_events())
    con.commit()
    triggers_after_first = deduper.trigger_count()
    groups_after_first = len(deduper.list_groups())

    # 同一批事件原样重放（模拟重传/任务重跑）
    second_out, second_suppressed, _ = feed(deduper, normalized_events())
    con.commit()

    assert second_out == []                 # 全部识别为重传
    assert second_suppressed == 11
    assert deduper.trigger_count() == triggers_after_first
    assert len(deduper.list_groups()) == groups_after_first
    assert len(deduper.emission_rows()) == 7
    assert deduper.journal_len() == 11      # 流水不重复追加


# ---------- 3. 乱序到达结果与顺序无关 ----------
def test_out_of_order_converges_to_same_state(tmp_path):
    ordered = normalized_events()
    shuffled = [ordered[i] for i in (6, 0, 9, 1, 7, 3, 10, 4, 8, 2, 5)]

    con_a = sqlite3.connect(tmp_path / "a.db")
    store.init(con_a)
    da = Deduper(con_a)
    feed(da, ordered)
    con_a.commit()

    con_b = sqlite3.connect(tmp_path / "b.db")
    store.init(con_b)
    db = Deduper(con_b)
    feed(db, shuffled)
    con_b.commit()

    snap_a = {gid: state for gid, state in da.list_groups()}
    snap_b = {gid: state for gid, state in db.list_groups()}
    assert set(snap_a) == set(snap_b)
    for gid in snap_a:
        a, b = snap_a[gid], snap_b[gid]
        assert a["triggers"] == b["triggers"]
        assert a["withdrawn"] == b["withdrawn"]
        assert a["revision_seq"] == b["revision_seq"]
        assert sorted(a["sources"]) == sorted(b["sources"])
        assert [n["version"] for n in a["version_chain"]] == \
               [n["version"] for n in b["version_chain"]]
        assert a["first_seen"] == b["first_seen"]


# ---------- 4. 镜像源缺字段：靠版本范围/内容摘要补登记并回填身份 ----------
def test_mirror_missing_fields_merges_and_backfills(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    events = normalized_events()
    feed(deduper, [events[0], events[2]])  # 官方 release + 缺字段镜像公告
    con.commit()

    groups = dict(deduper.list_groups())
    assert len(groups) == 1
    gid, state = next(iter(groups.items()))
    assert "advisory" in state["sources"]
    # 镜像缺 rid/url，但其证据带来的身份仍归并到同一 group；
    # 后续带字段证据可回填规范 URL / 发布标识
    feed(deduper, [events[1]])
    con.commit()
    state = dict(deduper.list_groups())[gid]
    assert any(u.endswith("/pkg-x/1.2.0") for u in state["urls"])
    assert "rel-2026-001" in state["rids"]
    # 三次来源补登记均未修订内容，下游只触发首次一次
    assert state["triggers"] == 1


# ---------- 5. 页面改版：旧 URL（含跟踪参数）与新 URL 归并 ----------
def test_page_redesign_url_aliases_merge(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    events = normalized_events()
    emitted, _, _ = feed(deduper, [events[0], events[1]])
    con.commit()

    assert len(deduper.list_groups()) == 1
    state = next(iter(dict(deduper.list_groups()).values()))
    urls = state["urls"]
    # 跟踪参数被剥离，改版旧地址经静态映射归一到新地址
    assert "https://changelog.example.com/pkg-x/1.2.0" in urls
    assert all("utm_source" not in u for u in urls)
    assert all("v1/pkg-x/1-2-0" not in u for u in urls)
    # 第二份证据是同内容不同来源：来源补登记，不产生第二次下游触发，
    # 但返回刷新后的归并记录（store 仍 upsert 同一 group 主键）。
    assert state["triggers"] == 1
    assert len(emitted) == 2
    assert {r["id"] for r in emitted} == {state["group_id"]}


def test_canonical_url_rules():
    assert canonical_url("HTTPS://Example.COM/a//b/?utm_source=x#frag") == \
           "https://example.com/a/b"
    assert canonical_url("changelog.example.com/v1/pkg-x/1-2-0?ref=rss") == \
           "https://changelog.example.com/pkg-x/1.2.0"
    assert canonical_url("") == ""
    assert canonical_url(None) == ""


# ---------- 6. 半写入恢复：中断后继续处理与不中断结果一致 ----------
def test_recovery_after_torn_state(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    events = normalized_events()
    feed(deduper, events[:5])  # 先完整处理前 5 条
    con.commit()

    # 注入半写入：group 行存在但身份键登记缺失（模拟中断在两表写入之间）
    gid, good_state = deduper.list_groups()[0]
    con.execute("DELETE FROM dedup_aliases WHERE group_id=?", (gid,))
    con.commit()

    # 重新打开并 heal：后续事件必须仍归并到原 group，而不是新建发布
    con2 = sqlite3.connect(tmp_path / "t.db")
    store.init(con2)
    deduper2 = Deduper(con2)  # 构造时自动 heal
    emitted, suppressed, _triggered = feed(deduper2, events[5:])
    con2.commit()

    assert len(deduper2.list_groups()) == 3
    healed = dict(deduper2.list_groups())[gid]
    assert healed["first_seen"] == good_state["first_seen"]
    # 与一次不中断处理对照
    ref_con = sqlite3.connect(tmp_path / "ref.db")
    store.init(ref_con)
    ref = Deduper(ref_con)
    feed(ref, events)
    ref_con.commit()
    assert deduper2.trigger_count() == ref.trigger_count()
    assert {g for g, _ in deduper2.list_groups()} == {g for g, _ in ref.list_groups()}
    for g, state in ref.list_groups():
        healed_state = dict(deduper2.list_groups())[g]
        assert healed_state["triggers"] == state["triggers"]
        assert healed_state["withdrawn"] == state["withdrawn"]
        assert healed_state["revision_seq"] == state["revision_seq"]


def test_transaction_rollback_leaves_no_partial_state(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    events = normalized_events()
    feed(deduper, events[:3])
    con.rollback()  # 模拟异常回滚：半写入整体撤销

    assert deduper.journal_len() == 0
    assert len(deduper.list_groups()) == 0
    # 重放整批应与首次处理完全一致
    emitted, suppressed, triggered = feed(deduper, events)
    con.commit()
    assert suppressed == 1     # 仅 e-004 原样重传被抑制
    assert triggered == 7
    assert len(emitted) == 10
    assert len(deduper.list_groups()) == 3
    assert deduper.trigger_count() == 7


# ---------- 7. 规则升级：保留流水可回放，重建后触发台账一致 ----------
def test_rule_upgrade_replay_from_journal(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    feed(deduper, normalized_events())
    con.commit()
    before_groups = {gid: json.loads(json.dumps(state))
                     for gid, state in deduper.list_groups()}
    before_emissions = deduper.emission_rows()
    assert deduper.journal_len() == 11

    replayed = deduper.replay_journal()
    con.commit()
    assert replayed == 11

    after_groups = dict(deduper.list_groups())
    assert set(after_groups) == set(before_groups)
    for gid, state in before_groups.items():
        rebuilt = after_groups[gid]
        assert rebuilt["triggers"] == state["triggers"]
        assert rebuilt["first_seen"] == state["first_seen"]
        assert rebuilt["revision_seq"] == state["revision_seq"]
        assert rebuilt["withdrawn"] == state["withdrawn"]
    assert deduper.emission_rows() == before_emissions
    # 回放后再接收重传仍被抑制
    _, suppressed, _ = feed(deduper, normalized_events())
    assert suppressed == 11


# ---------- 8. pipeline 端到端：去重身份交给 relate / store，重跑不增行 ----------
def test_pipeline_replay_fixture_end_to_end(tmp_path):
    con = fresh_con(tmp_path)
    stats = run_pipeline(con)
    assert stats["fed"] == 11
    assert stats["emitted"] == 7      # 首见/修订/撤回触发
    assert stats["written"] == 10     # 触发 7 + 来源补登记刷新 3
    assert stats["suppressed"] == 1   # 原样重传
    assert stats["rows"] == 3  # 每个发布身份一行（group_id 即稳定主键）

    # 重跑：全部命中重传，下游零新增、零刷新
    again = run_pipeline(con)
    assert again["fed"] == 11
    assert again["emitted"] == 0
    assert again["written"] == 0
    assert again["suppressed"] == 11
    assert again["rows"] == 3

    # 落库记录携带 dedup 身份与修订信息，供 relate / store 消费；
    # 来源补登记的刷新保证三类来源关系都在最终记录里。
    rows = {k: data for k, data in store.all_records(con)}
    alpha_row = next(data for data in rows.values()
                     if data["component"] == "pkg-alpha")
    assert alpha_row["dedup"]["revision_seq"] == 1
    assert alpha_row["dedup"]["withdrawn"] is True
    assert set(alpha_row["dedup"]["sources"]) == {"release", "changelog", "advisory"}
    beta_row = next(data for data in rows.values()
                    if data["component"] == "pkg-beta")
    assert set(beta_row["dedup"]["sources"]) == {"advisory", "release", "changelog"}


# ---------- 9. 既有 happy-path 样例仍成立 ----------
def test_legacy_happy_path_unchanged(tmp_path):
    con = sqlite3.connect(tmp_path / "legacy.db")
    store.init(con)
    legacy = Path(__file__).resolve().parents[1] / "fixtures" / "input.json"
    a = pipeline.run(con=con, fixture=legacy)
    b = pipeline.run(con=con, fixture=legacy)
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3  # r-003 重复仍被去重
    # 通用记录不进入发布域状态机
    deduper = Deduper(con)
    assert deduper.journal_len() == 0


# ---------- 9b. 撤回幂等与同来源修订识别 ----------
def test_duplicate_withdraw_fires_once(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    base = {
        "src": "release", "component": "pkg-delta", "rid": "REL-D-1",
        "url": "https://example.test/delta/1.0.0", "version": "1.0.0",
        "title": "delta 1.0.0", "body": "首发", "event": "publish",
        "ts": "2026-11-10T00:00:00Z",
    }
    from src.changelogfeed import normalizer, feed_pull
    def prep(rec, eid):
        rec = dict(rec, id=eid)
        return semver.process(feed_pull.process(normalizer.process(rec)))

    d1 = prep(base, "d-1")
    w1 = prep(dict(base, event="withdraw", body="", title="delta 1.0.0",
                   ts="2026-11-11T00:00:00Z"), "d-2")
    w2 = prep(dict(base, event="withdraw", body="", title="delta 1.0.0",
                   ts="2026-11-12T00:00:00Z"), "d-3")
    out, suppressed, triggered = feed(deduper, [d1, w1, w2])
    con.commit()
    state = deduper.list_groups()[0][1]
    assert state["withdrawn"] is True
    assert state["withdrawn_event_id"] == "d-2"
    # 首次 + 撤回各一次，第二次撤回不重复触发
    assert state["triggers"] == 2
    assert triggered == 2
    assert suppressed == 1
    assert len(out) == 2


def test_same_source_revision_vs_cross_source_copy(tmp_path):
    con = fresh_con(tmp_path)
    deduper = Deduper(con)
    from src.changelogfeed import normalizer, feed_pull
    def prep(rec, eid):
        rec = dict(rec, id=eid)
        return semver.process(feed_pull.process(normalizer.process(rec)))

    first = prep({
        "src": "changelog", "component": "pkg-eps", "rid": "REL-E-1",
        "url": "https://example.test/eps/2.0.0", "version": "2.0.0",
        "title": "eps 2.0.0", "body": "初版说明", "event": "publish",
        "ts": "2026-11-10T00:00:00Z",
    }, "p-1")
    # 同一来源、同内容：原样重传
    again = prep(dict(first), "p-2")
    again["id"] = "p-2"
    # 同一来源、正文变化：真正的修订
    revised = prep({
        "src": "changelog", "component": "pkg-eps", "rid": "REL-E-1",
        "url": "https://example.test/eps/2.0.0", "version": "2.0.0",
        "title": "eps 2.0.0", "body": "修订后的说明", "event": "revise",
        "ts": "2026-11-11T00:00:00Z",
    }, "p-3")
    out, suppressed, triggered = feed(deduper, [first, again, revised])
    con.commit()
    state = deduper.list_groups()[0][1]
    assert state["revision_seq"] == 1
    assert state["revision_of"][0]["event_id"] == "p-3"
    assert state["body"] == "修订后的说明"
    assert triggered == 2  # 首见 + 修订
    assert suppressed == 1  # 原样重传
    assert len(out) == 2


# ---------- 10. semver 归一化作为比较输入 ----------
def test_semver_normalization_feeds_identity():
    assert semver.normalize_version("v1.2") == "1.2.0"
    assert semver.normalize_version("1.2.3-rc1") == "1.2.3"
    assert semver.normalize_version("nonsense") == ""
    assert semver.normalize_range(">= 1.0.0") == ">=1.0.0"
    assert semver.normalize_ranges(["< 2.4.1", ">=2.0.0"]) == \
           semver.normalize_ranges([">= 2.0.0", "<2.4.1"])
    rec = semver.process({"version": "v3.0.0", "ranges": [">= 1.0"]})
    assert rec["version_norm"] == "3.0.0"
    assert rec["ranges_norm"] == [">=1.0.0"]
    # 基线记录无版本字段时行为不变
    assert semver.process({"id": "r-001"}).get("_stage_semver") == "naive"
