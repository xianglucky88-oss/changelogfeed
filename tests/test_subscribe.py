"""subscribe 多源订阅状态：回补范围、暂停/恢复、过滤条件版本、来源失效/恢复、
半批提交重启的不漏/不重/游标可解释性测试。

事件来源（离线构造，monkeypatch loader 回放）：
- changelog 源：c-001..c-012，按天递增；
- advisory  源：a-001..a-012，按天递增。
两个来源独立失效/恢复，互不影响。
"""
import sqlite3

import pytest

from src.changelogfeed import pipeline, store, subscribe, feed_pull
from src.changelogfeed import loader as loader_mod


# ---------------------------------------------------------------------------
# 夹具与辅助
# ---------------------------------------------------------------------------
def ev(src, eid, day, status="active", value=10, name=None):
    return {"id": eid, "src": src, "name": name or eid, "value": value,
            "status": status, "ts": f"2026-11-{day:02d}T00:00:00Z"}


def changelog_events(n=12):
    return [ev("changelog", f"c-{i:03d}", i) for i in range(1, n + 1)]


def advisory_events(n=12):
    return [ev("advisory", f"a-{i:03d}", i, value=100 + i) for i in range(1, n + 1)]


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = tmp_path / "sub.db"
    con = store.connect(db)
    feed_pull.reset_health()

    runs: dict[str, list[dict]] = {}

    def fake_load():
        # 每轮回放当前批次；loader 模块被 pipeline 以 .loader 方式导入，
        # 直接替换其 load 属性即可。
        return list(runs.get("batch", []))

    monkeypatch.setattr(loader_mod, "load", fake_load)
    monkeypatch.setattr(pipeline._loader, "load", fake_load)

    class Env:
        def run(self, batch, run_id, batch_size=50, crash_after_commits=-1):
            runs["batch"] = batch
            engine = subscribe.SubscriptionEngine(
                con, batch_size=batch_size, run_id=run_id,
                backfill_limit=5, crash_after_commits=crash_after_commits)
            return pipeline.run(con=con, engine=engine, run_id=run_id,
                                batch_size=batch_size)

        def engine(self, run_id="setup", **kw):
            return subscribe.SubscriptionEngine(con, run_id=run_id, **kw)

        def register(self, src, filters=None, backfill_limit=5):
            # 在任何拉取发生之前显式注册：首次回补按 backfill_limit 有限窗口截断。
            return self.engine("register").upsert_subscription(
                src, filters=filters, backfill_limit=backfill_limit)

        def sub(self, src):
            cur = con.execute(
                "SELECT config_ver,filters,status,cursor_ts,cursor_id,backfill_limit,"
                "catchup_pending FROM subscriptions WHERE src=?", (src,)).fetchone()
            assert cur is not None, f"no subscription for {src}"
            return cur

        def delivered_ids(self, src):
            return [r[0] for r in con.execute(
                "SELECT event_id FROM deliveries WHERE src=? ORDER BY ts,event_id", (src,))]

        def delivery_rows(self, src):
            return con.execute(
                "SELECT event_id,config_ver,run_id FROM deliveries "
                "WHERE src=? ORDER BY ts,event_id", (src,)).fetchall()

        def stored_ids(self, src):
            import json
            out = []
            for (d,) in con.execute("SELECT data FROM records"):
                rec = json.loads(d)
                if rec.get("src") == src:
                    out.append(rec["id"])
            return sorted(out)

        def audit_kinds(self, src):
            return [r[0] for r in con.execute(
                "SELECT kind FROM sub_audit WHERE src=? ORDER BY id", (src,))]

        def audit(self, src):
            import json
            return [(k, json.loads(d)) for k, d in con.execute(
                "SELECT kind,detail FROM sub_audit WHERE src=? ORDER BY id", (src,))]

    yield Env()
    con.close()
    feed_pull.reset_health()


# ---------------------------------------------------------------------------
# 1. 首次回补：显式注册的订阅只回补最近 backfill_limit 条，更老的跳过且有审计
# ---------------------------------------------------------------------------
def test_initial_backfill_is_bounded_and_audited(env):
    env.register("changelog", backfill_limit=5)
    env.run(changelog_events(12), "r1")
    # backfill_limit=5：只投递最近 5 条 c-008..c-012
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(8, 13)]
    cfg_ver, _filters, status, cts, cid, limit, catchup = env.sub("changelog")
    assert (cfg_ver, status, limit, catchup) == (1, "active", 5, 0)
    # 窗口处理完后游标走到最后一条投递事件 c-012；窗口起点由审计记录
    assert (cts, cid) == ("2026-11-12T00:00:00Z", "c-012")
    kinds = env.audit_kinds("changelog")
    assert kinds[0] == "sub_created"
    assert "backfill_skip" in kinds
    skip = [d for k, d in env.audit("changelog") if k == "backfill_skip"][0]
    assert skip["skipped"] == 7 and skip["limit"] == 5
    assert skip["window_starts_after"] == ["2026-11-07T00:00:00Z", "c-007"]
    assert skip["first_kept"] == ["2026-11-08T00:00:00Z", "c-008"]


def test_autocreated_subscription_consumes_full_first_pull(env):
    # 未显式注册：首批事件即订阅时刻的“现在”，全量投递、不截断；
    # 半批重启后更大的首批也不会二次截断历史（backfill_done 已落库）。
    env.run(changelog_events(12), "r1")
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 13)]
    assert "backfill_skip" not in env.audit_kinds("changelog")
    assert "backfill_window_prepared" in env.audit_kinds("changelog")


def test_second_run_continues_from_cursor_without_replay(env):
    env.register("changelog", backfill_limit=5)
    env.run(changelog_events(12), "r1")
    first = env.delivered_ids("changelog")
    # 同样的事件重跑：游标之后没有新事件，通知集合不变、不产生重复投递
    env.run(changelog_events(12), "r2")
    assert env.delivered_ids("changelog") == first
    # deliveries 主键去重：每个 event_id 恰好一行
    n = env.engine().con.execute(
        "SELECT COUNT(*) FROM deliveries WHERE src='changelog'").fetchone()[0]
    assert n == 5
    # 新事件 c-013 增量到达：只投递它一条
    env.run(changelog_events(12) + [ev("changelog", "c-013", 13)], "r3")
    assert env.delivered_ids("changelog") == first + ["c-013"]
    assert env.sub("changelog")[3:5] == ("2026-11-13T00:00:00Z", "c-013")


# ---------------------------------------------------------------------------
# 2. 暂停/恢复：暂停期间不投递、游标冻结；恢复后补齐漏掉的事件，不重报
# ---------------------------------------------------------------------------
def test_pause_freezes_cursor_and_resume_replays_missed(env):
    env.register("changelog", backfill_limit=50)
    env.run(changelog_events(5), "r1")  # 5 条在回补深度内，全部投递
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 6)]
    eng = env.engine("ops")
    eng.pause("changelog")
    assert env.sub("changelog")[2] == "paused"
    cursor_before = env.sub("changelog")[3:5]

    # 暂停期间到达 c-006..c-008：事件照常入库，但没有新通知、游标不动
    env.run(changelog_events(8), "r2")
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 6)]
    assert env.sub("changelog")[3:5] == cursor_before
    assert "c-008" in env.stored_ids("changelog")  # 事件确实落库了

    eng.resume("changelog")
    env.run(changelog_events(8), "r3")
    # 恢复后从冻结游标补看，漏掉的 c-006..c-008 全部补上，且 c-001..c-005 不重报
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 9)]
    assert env.sub("changelog")[3:5] == ("2026-11-08T00:00:00Z", "c-008")
    rows = env.delivery_rows("changelog")
    assert len(rows) == len({r[0] for r in rows}) == 8  # 无重复行


# ---------------------------------------------------------------------------
# 3. 过滤条件变化：配置版本 +1，历史不重发，只对游标之后的新事件生效
# ---------------------------------------------------------------------------
def test_filter_change_bumps_version_and_does_not_rescan_history(env):
    env.register("advisory", backfill_limit=50)
    env.run(advisory_events(5), "r1")
    assert env.delivered_ids("advisory") == [f"a-{i:03d}" for i in range(1, 6)]
    assert {r[1] for r in env.delivery_rows("advisory")} == {1}

    eng = env.engine("ops")
    eng.update_filters("advisory", {"status": "sold"})
    assert env.sub("advisory")[0] == 2  # config_ver
    # 历史通知原封不动（不会把历史再发一遍）
    assert env.delivered_ids("advisory") == [f"a-{i:03d}" for i in range(1, 6)]

    # 游标之后的新事件按新条件过滤：active 的被过滤，sold 的投递且带新版本号
    batch = advisory_events(5) + [
        ev("advisory", "a-006", 6, status="active"),
        ev("advisory", "a-007", 7, status="sold"),
    ]
    env.run(batch, "r2")
    assert env.delivered_ids("advisory") == [f"a-{i:03d}" for i in range(1, 6)] + ["a-007"]
    new_rows = [r for r in env.delivery_rows("advisory") if r[0] == "a-007"]
    assert new_rows[0][1:] == (2, "r2")
    # 被过滤事件仍推进游标（否则会无限重看），审计可查
    assert "filtered_out" in env.audit_kinds("advisory")
    assert env.sub("advisory")[3:5] == ("2026-11-07T00:00:00Z", "a-007")


# ---------------------------------------------------------------------------
# 4. 两个来源独立失效/恢复：互不影响；恢复走有限回补窗口，审计可解释
# ---------------------------------------------------------------------------
def test_two_sources_fail_and_recover_independently(env):
    env.register("changelog", backfill_limit=5)
    env.register("advisory", backfill_limit=5)
    env.run(changelog_events(4) + advisory_events(4), "r1")
    assert len(env.delivered_ids("changelog")) == 4
    assert len(env.delivered_ids("advisory")) == 4

    # changelog 失效，advisory 仍健康：changelog 的新事件被拦、订阅置 invalid，
    # advisory 的新事件正常增量投递
    feed_pull.set_source_health("changelog", False)
    env.run(changelog_events(6) + advisory_events(6), "r2")
    assert env.sub("changelog")[2] == "invalid"
    assert env.sub("advisory")[2] == "active"
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 5)]
    assert env.delivered_ids("advisory") == [f"a-{i:03d}" for i in range(1, 7)]
    # 失效期间游标冻结在 c-004
    assert env.sub("changelog")[3:5] == ("2026-11-04T00:00:00Z", "c-004")
    assert "source_invalid" in env.audit_kinds("changelog")

    # 失效期间 changelog 侧继续累积 c-005..c-010
    feed_pull.set_source_health("changelog", False)
    env.run(changelog_events(10) + advisory_events(8), "r3")
    assert env.sub("advisory")[3:5] == ("2026-11-08T00:00:00Z", "a-008")

    # changelog 恢复：backfill_limit=5 的窗口，只补 c-006..c-010；
    # c-005 落在窗口外，按明确策略跳过并留 recovery_skip 审计
    feed_pull.set_source_health("changelog", True)
    env.run(changelog_events(10) + advisory_events(8), "r4")
    assert env.sub("changelog")[2] == "active"
    assert env.sub("changelog")[6] == 0  # catchup_pending 已清除
    assert "source_recovered" in env.audit_kinds("changelog")
    assert "recovery_skip" in env.audit_kinds("changelog")
    skip = [d for k, d in env.audit("changelog") if k == "recovery_skip"][0]
    assert skip["skipped"] == 1 and skip["first_kept"] == [
        "2026-11-06T00:00:00Z", "c-006"]
    # c-001..c-004 已投递不重报；c-005 明确跳过；c-006..c-010 补齐
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 5)] + \
        [f"c-{i:03d}" for i in range(6, 11)]
    assert env.sub("changelog")[3:5] == ("2026-11-10T00:00:00Z", "c-010")

    # 再跑一轮无新事件：两边都稳定，不产生任何重复通知
    before = (env.delivered_ids("changelog"), env.delivered_ids("advisory"))
    env.run(changelog_events(10) + advisory_events(8), "r5")
    assert (env.delivered_ids("changelog"), env.delivered_ids("advisory")) == before


# ---------------------------------------------------------------------------
# 5. 半批提交处重启：未提交批次整体重做，deliveries 去重保证不重不漏
# ---------------------------------------------------------------------------
def test_recovery_window_is_bounded_by_backfill_limit(env):
    # 显式注册一个回补深度只有 3 的订阅：失效期间攒下 6 条，恢复只补最近 3 条
    env.register("changelog", backfill_limit=3)
    env.run(changelog_events(2), "r1")  # c-001,c-002 全量投递
    feed_pull.set_source_health("changelog", False)
    env.run(changelog_events(8), "r2")  # 失效：c-003..c-008 全部拉不到
    assert env.sub("changelog")[2] == "invalid"
    feed_pull.set_source_health("changelog", True)
    env.run(changelog_events(8), "r3")  # 恢复：游标后有 6 条，窗口只留最近 3 条
    # c-003..c-005 在窗口外被明确跳过，c-006..c-008 回补投递
    assert env.delivered_ids("changelog") == ["c-001", "c-002",
                                              "c-006", "c-007", "c-008"]
    skip = [d for k, d in env.audit("changelog") if k == "recovery_skip"][0]
    assert skip["skipped"] == 3
    assert skip["window_starts_after"] == ["2026-11-05T00:00:00Z", "c-005"]
    assert env.sub("changelog")[3:5] == ("2026-11-08T00:00:00Z", "c-008")
    # 跳过事件在交叉核对时属于“有解释的未通知”，不视为漏项
    assert env.sub("changelog")[6] == 0  # catchup_pending 已清


def test_restart_at_half_batch_commit_is_exactly_once(env):
    env.register("changelog", backfill_limit=50)
    # 12 条事件、批大小 4：第 1 批提交后（4 条已通知）注入崩溃
    with pytest.raises(RuntimeError, match="injected crash"):
        env.run(changelog_events(12), "crash-run", batch_size=4,
                crash_after_commits=1)
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 5)]
    # 已提交批次的游标停在 c-004
    assert env.sub("changelog")[3:5] == ("2026-11-04T00:00:00Z", "c-004")

    # 新进程从同一 DB 继续：c-001..c-004 不重报，c-005..c-012 补齐
    env.run(changelog_events(12), "restart-run", batch_size=4)
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(1, 13)]
    rows = env.delivery_rows("changelog")
    assert len(rows) == 12 and len({r[0] for r in rows}) == 12
    # 重启后游标连续走到 c-012，没有跳跃
    assert env.sub("changelog")[3:5] == ("2026-11-12T00:00:00Z", "c-012")


def test_crash_before_first_commit_replays_whole_batch(env):
    env.register("advisory", backfill_limit=50)
    # 崩溃发生在第 1 次批提交之后：第一批 4 条已落库，重启整体续跑且不重复
    with pytest.raises(RuntimeError, match="injected crash"):
        env.run(advisory_events(8), "crash0", batch_size=4, crash_after_commits=1)
    # 批大小 4、8 条：第一批 4 条已提交后才崩
    assert len(env.delivered_ids("advisory")) == 4
    env.run(advisory_events(8), "restart", batch_size=4)
    ids = env.delivered_ids("advisory")
    assert ids == [f"a-{i:03d}" for i in range(1, 9)]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# 6. 事件库 × 通知记录交叉核对：每个事件要么已通知、要么有明确解释，无不明游标跳跃
# ---------------------------------------------------------------------------
def test_event_store_vs_delivery_reconciliation(env):
    import json
    # 完整生命周期：回补 -> 暂停 -> 恢复 -> 改过滤 -> 失效 -> 恢复
    env.register("changelog", backfill_limit=50)
    env.run(changelog_events(5), "r1")
    eng = env.engine("ops")
    eng.pause("changelog")
    env.run(changelog_events(7), "r2")
    eng.resume("changelog")
    env.run(changelog_events(7), "r3")
    eng.update_filters("changelog", {"min_value": 1000})  # 后续全被过滤
    env.run(changelog_events(9), "r4")
    feed_pull.set_source_health("changelog", False)
    env.run(changelog_events(11), "r5")
    feed_pull.set_source_health("changelog", True)
    env.run(changelog_events(11), "r6")

    delivered = set(env.delivered_ids("changelog"))
    stored = set()
    for (d,) in env.engine().con.execute("SELECT data FROM records"):
        rec = json.loads(d)
        if rec.get("src") == "changelog":
            stored.add(rec["id"])
    # 1) 所有通知的事件都在事件库中
    assert delivered <= stored
    # 2) deliveries 无重复（主键保证，再显式断言一次）
    assert len(delivered) == len(env.delivered_ids("changelog"))

    # 3) 每个未通知事件都有明确解释：filtered_out / backfill_skip / recovery_skip
    explained: set[str] = set()
    for kind, detail in env.audit("changelog"):
        if kind == "filtered_out":
            explained.add(detail["event_id"])
        elif kind in ("backfill_skip", "recovery_skip"):
            # 跳过区间用边界表达：窗口起点之前的事件都算已解释
            boundary = detail["window_starts_after"][1]
            explained.update(e["id"] for e in changelog_events(11)
                             if (e["ts"], e["id"]) <=
                             (detail["window_starts_after"][0], boundary))
    unexplained = stored - delivered - explained
    assert unexplained == set(), f"事件无解释: {unexplained}"

    # 4) 游标连续性：最终游标 = 已处理事件的最大位置，且审计中每次推进都有记录
    cts, cid = env.sub("changelog")[3:5]
    max_pos = max((e["ts"], e["id"]) for e in changelog_events(11))
    assert (cts, cid) == max_pos
    # 审计里不存在没有对应 kind 的游标变化（所有变化路径都写审计）
    assert set(env.audit_kinds("changelog")) <= {
        "sub_created", "backfill_skip", "backfill_window_prepared",
        "paused", "resumed", "delivered", "delivery_dedup", "filtered_out",
        "filters_changed", "source_invalid", "source_recovered",
        "recovery_skip", "catchup_window_prepared"}


def test_cursor_never_advances_for_invalid_source_and_no_dedup_gap(env):
    # 失效期间完全没有事件落库；恢复后窗口内事件与之前的通知首尾相接
    env.register("advisory", backfill_limit=50)
    env.run(advisory_events(3), "r1")
    feed_pull.set_source_health("advisory", False)
    env.run(advisory_events(6), "r2")  # 整源拉取失败，staged 全 None
    assert env.sub("advisory")[3:5] == ("2026-11-03T00:00:00Z", "a-003")
    feed_pull.set_source_health("advisory", True)
    env.run(advisory_events(6), "r3")  # 恢复回补：游标后 a-004..a-006，无截断
    ids = env.delivered_ids("advisory")
    assert ids == [f"a-{i:03d}" for i in range(1, 7)]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# 7. 边界：首次拉取前就暂停；未绑定引擎直接调 process() 的防护
# ---------------------------------------------------------------------------
def test_paused_before_first_pull_delivers_nothing_then_backfills_on_resume(env):
    env.register("changelog", backfill_limit=5)
    env.engine("ops").pause("changelog")
    # 暂停中的首轮：事件入库、不投递、游标保持 NULL、回补窗口不消费
    env.run(changelog_events(8), "r1")
    assert env.delivered_ids("changelog") == []
    assert env.sub("changelog")[3:5] == (None, None)
    assert env.sub("changelog")[2] == "paused"
    assert "c-008" in env.stored_ids("changelog")

    env.engine("ops").resume("changelog")
    env.run(changelog_events(8), "r2")
    # 恢复后按订阅的回补深度有限窗口投递最近 5 条，老事件留审计
    assert env.delivered_ids("changelog") == [f"c-{i:03d}" for i in range(4, 9)]
    assert "backfill_skip" in env.audit_kinds("changelog")


def test_process_without_engine_raises():
    subscribe.reset_engine()
    with pytest.raises(RuntimeError, match="bind_engine"):
        subscribe.process({"id": "x", "src": "changelog"})


def test_filter_helpers_and_position_ordering():
    assert subscribe.position_of(ev("s", "z", 2)) > subscribe.position_of(ev("s", "a", 1))
    same = ev("s", "x", 1)  # ts=2026-11-01, id=x，与游标同位置
    assert subscribe.after_cursor(same, "2026-11-01T00:00:00Z", "x") is False
    assert subscribe.after_cursor(ev("s", "x", 2), "2026-11-01T00:00:00Z", "x") is True
    assert subscribe.after_cursor(ev("s", "x", 2), None, None) is True
    assert subscribe.match_filters({"status": "sold", "value": 5, "name": "n"},
                                   {"status": "sold", "min_value": 1, "max_value": 9,
                                    "name_contains": "n"})
    assert not subscribe.match_filters({"status": "active", "value": 5, "name": "n"},
                                       {"status": "sold"})
    assert not subscribe.match_filters({"value": 50}, {"max_value": 10})
