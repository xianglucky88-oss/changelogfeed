"""单一职责：多源 changelog/advisory 订阅状态管理与通知投递（subscribe 阶段）。

状态如何一起保存（SQLite，随事件库同一个连接）
------------------------------------------------
1. ``subscriptions`` —— 每个来源一行，保存：
   - 订阅配置版本 ``config_ver``：过滤条件每变更一次 +1，随通知一起落库，
     可区分一条通知是哪一版过滤条件的产物；
   - 过滤条件 ``filters``（JSON）；
   - 订阅状态 ``status``：``active`` / ``paused`` / ``invalid``；
   - 来源游标 ``cursor_ts`` + ``cursor_id``：已处理边界（位置由 (ts,id) 字典序
     确定），重启后从该边界之后继续；
   - 首次回补范围 ``backfill_limit``。
2. ``deliveries`` —— 已投递事件（通知记录），主键 ``(src,event_id)``。
   投递走 ``INSERT OR IGNORE``：重复事件/半批重启不会产生第二条通知，
   即“重复投递语义”= 每事件对每来源最多通知一次（exactly-once 落库）。
3. ``sub_audit`` —— 只增审计轨迹：状态变化、配置变化、回补跳过、窗口边界，
   保证任何游标推进都有可解释的记录，杜绝“无法解释的游标跳跃”。

明确的范围选择（题目要求的边界语义）
------------------------------------
- 首次回补多深：
  * 显式注册（``upsert_subscription`` 先于拉取）的订阅：只回补该来源最近
    ``backfill_limit`` 条（默认 50），更老的事件不投递，游标先推到回补窗口
    起点并写 ``backfill_skip`` 审计；窗口边界在处理任何事件之前算出并落库，
    半批重启不会二次截断。
  * 流水线中自动建立的订阅：首次拉取到的事件就是订阅时刻的“现在”，首批
    全量投递（``backfill_done`` 落库后同样不会二次截断）。
- 条件变化后重看哪些区间：**不回看历史**。过滤条件变更只让配置版本 +1，
  对当前游标之后的新事件生效；已投递/已处理的历史区间保持结论不变。
- 暂停/恢复：暂停期间事件照常入库但不投递、游标冻结；恢复后从冻结的游标
  位置无界补看（没有 backfill_limit 截断），漏掉的匹配事件逐条补通知，
  已投递事件由 deliveries 主键去重，不会重报。
- 来源失效/恢复：来源失效时订阅置 ``invalid``、游标不动；恢复后置
  ``catchup_pending``（落库），下一轮只在“游标之后”按 ``backfill_limit``
  取有限窗口回补，窗口之外的历史跳过并写 ``recovery_skip`` 审计，之后增量。
- 半批提交：每个批次把“事件入库 + 通知 + 游标推进”放进同一事务提交，
  重启后未提交的批次整体重做，deliveries 去重保证不重不漏。
"""
from __future__ import annotations

import contextvars
import json
from typing import Any, Optional

# ---- 状态常量 ---------------------------------------------------------------
ACTIVE = "active"
PAUSED = "paused"
INVALID = "invalid"

DEFAULT_BACKFILL_LIMIT = 50

# 处理单条记录时的上下文：编排层注入同一个引擎（含连接、批次设置）。
_engine_ctx: contextvars.ContextVar[Optional["SubscriptionEngine"]] = (
    contextvars.ContextVar("subscribe_engine", default=None)
)


# ---- 位置与过滤（纯函数） ----------------------------------------------------
def position_of(rec: dict[str, Any]) -> tuple[str, str]:
    """事件在来源内的有序位置：(事件时间, 事件 id)，均按字符串字典序比较。"""
    return (str(rec.get("ts", "")), str(rec.get("id", "")))


def after_cursor(rec: dict[str, Any], cur_ts: Optional[str], cur_id: Optional[str]) -> bool:
    if cur_ts is None:
        return True
    return position_of(rec) > (str(cur_ts), str(cur_id or ""))


def match_filters(rec: dict[str, Any], filters: dict[str, Any]) -> bool:
    """过滤条件（显式约定，支持的最小集合）：

    - ``status``：记录 status 必须等于给定值；
    - ``min_value`` / ``max_value``：记录 value 的闭区间；
    - ``name_contains``：记录 name 的子串匹配（大小写敏感）。
    无过滤条件或空条件视为全部匹配。
    """
    if not filters:
        return True
    if "status" in filters and rec.get("status") != filters["status"]:
        return False
    value = rec.get("value")
    if "min_value" in filters and (value is None or value < filters["min_value"]):
        return False
    if "max_value" in filters and (value is None or value > filters["max_value"]):
        return False
    if "name_contains" in filters and filters["name_contains"] not in str(rec.get("name", "")):
        return False
    return True


# ---- 引擎 -------------------------------------------------------------------
class SubscriptionEngine:
    """持有连接与批次策略，负责建表、订阅变更、逐事件处理与事务提交。"""

    def __init__(self, con, batch_size: int = 50, run_id: str = "run",
                 backfill_limit: int = DEFAULT_BACKFILL_LIMIT,
                 crash_after_commits: int = -1):
        self.con = con
        self.batch_size = max(1, int(batch_size))
        self.run_id = str(run_id)
        self.backfill_limit = max(1, int(backfill_limit))
        # 测试注入点：完成第 N 次批提交后抛错模拟进程崩溃（-1 表示不崩溃）。
        self.crash_after_commits = crash_after_commits
        self._commits = 0
        self._pending = 0
        # 恢复待回补标记在落库（catchup_pending），重启不丢。
        self.stats = {"notified": 0, "skipped_filter": 0,
                      "paused_held": 0, "backfill_skipped": 0}
        self._create_tables()

    # -- 建表 ------------------------------------------------------------------
    def _create_tables(self) -> None:
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriptions(
                src             TEXT PRIMARY KEY,
                config_ver      INTEGER NOT NULL DEFAULT 1,
                filters         TEXT NOT NULL DEFAULT '{}',
                status          TEXT NOT NULL DEFAULT 'active',
                cursor_ts       TEXT,
                cursor_id       TEXT,
                backfill_limit  INTEGER NOT NULL DEFAULT 50,
                catchup_pending INTEGER NOT NULL DEFAULT 0,
                backfill_done   INTEGER NOT NULL DEFAULT 0,
                created_run     TEXT NOT NULL,
                updated_run     TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries(
                src          TEXT NOT NULL,
                event_id     TEXT NOT NULL,
                config_ver   INTEGER NOT NULL,
                run_id       TEXT NOT NULL,
                ts           TEXT NOT NULL,
                notified_at  TEXT NOT NULL,
                PRIMARY KEY(src, event_id)
            );
            CREATE TABLE IF NOT EXISTS sub_audit(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                src         TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                kind        TEXT NOT NULL,
                detail      TEXT NOT NULL,
                cursor_ts   TEXT,
                cursor_id   TEXT
            );
            """
        )
        self.con.commit()

    # -- 读取 ------------------------------------------------------------------
    def get_subscription(self, src: str) -> Optional[dict[str, Any]]:
        row = self.con.execute(
            "SELECT src,config_ver,filters,status,cursor_ts,cursor_id,backfill_limit,"
            "catchup_pending,backfill_done FROM subscriptions WHERE src=?", (src,)).fetchone()
        if row is None:
            return None
        return {"src": row[0], "config_ver": row[1], "filters": json.loads(row[2]),
                "status": row[3], "cursor_ts": row[4], "cursor_id": row[5],
                "backfill_limit": row[6], "catchup_pending": bool(row[7]),
                "backfill_done": bool(row[8])}

    def deliveries_for(self, src: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT event_id,config_ver,run_id,ts FROM deliveries WHERE src=? "
            "ORDER BY ts,event_id", (src,)).fetchall()
        return [{"event_id": r[0], "config_ver": r[1], "run_id": r[2], "ts": r[3]}
                for r in rows]

    def was_delivered(self, src: str, event_id: str) -> bool:
        return self.con.execute(
            "SELECT 1 FROM deliveries WHERE src=? AND event_id=?",
            (src, str(event_id))).fetchone() is not None

    def audit_trail(self, src: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT run_id,kind,detail,cursor_ts,cursor_id FROM sub_audit "
            "WHERE src=? ORDER BY id", (src,)).fetchall()
        return [{"run_id": r[0], "kind": r[1], "detail": r[2],
                 "cursor_ts": r[3], "cursor_id": r[4]} for r in rows]

    # -- 变更（显式订阅操作） ---------------------------------------------------
    def upsert_subscription(self, src: str, filters: Optional[dict[str, Any]] = None,
                            backfill_limit: Optional[int] = None) -> dict[str, Any]:
        """注册来源；已存在则只回补元数据，不改状态/游标/配置版本。"""
        src = str(src).strip().lower()
        sub = self.get_subscription(src)
        if sub is not None:
            if backfill_limit is not None:
                self.con.execute(
                    "UPDATE subscriptions SET backfill_limit=?,updated_run=? WHERE src=?",
                    (int(backfill_limit), self.run_id, src))
                self.con.commit()
            return self.get_subscription(src)
        limit = self.backfill_limit if backfill_limit is None else int(backfill_limit)
        self.con.execute(
            "INSERT INTO subscriptions(src,config_ver,filters,status,cursor_ts,cursor_id,"
            "backfill_limit,catchup_pending,backfill_done,created_run,updated_run) "
            "VALUES(?,1,?,'active',NULL,NULL,?,0,0,?,?)",
            (src, json.dumps(filters or {}, ensure_ascii=False), limit,
             self.run_id, self.run_id))
        self._audit(src, "sub_created",
                    {"backfill_limit": limit, "filters": filters or {}}, None, None)
        self.con.commit()
        return self.get_subscription(src)

    def update_filters(self, src: str, filters: dict[str, Any]) -> dict[str, Any]:
        """过滤条件变更：配置版本 +1，游标不动，历史不重看（仅对游标后事件生效）。"""
        sub = self._require(src)
        self.con.execute(
            "UPDATE subscriptions SET filters=?,config_ver=config_ver+1,updated_run=? "
            "WHERE src=?", (json.dumps(filters, ensure_ascii=False), self.run_id, src))
        new_ver = sub["config_ver"] + 1
        self._audit(src, "filters_changed",
                    {"from_ver": sub["config_ver"], "to_ver": new_ver,
                     "filters": filters}, sub["cursor_ts"], sub["cursor_id"])
        self.con.commit()
        return self.get_subscription(src)

    def pause(self, src: str) -> dict[str, Any]:
        sub = self._require(src)
        if sub["status"] != PAUSED:
            self._set_status(src, PAUSED, "paused", sub)
        return self.get_subscription(src)

    def resume(self, src: str) -> dict[str, Any]:
        sub = self._require(src)
        if sub["status"] == PAUSED:
            # 游标保持冻结位置不变；恢复后 process() 从该位置补看，无 backfill 截断。
            self._set_status(src, ACTIVE, "resumed", sub)
        return self.get_subscription(src)

    def mark_invalid(self, src: str, reason: str = "source_unavailable") -> None:
        """来源失效：置 invalid，游标冻结不动。"""
        sub = self.get_subscription(src)
        if sub is None:
            return
        if sub["status"] != INVALID:
            self._set_status(src, INVALID, "source_invalid", sub, {"reason": reason})

    def mark_recovered(self, src: str) -> None:
        """来源恢复：invalid -> active，并置 catchup_pending。

        下一轮 prepare_sources 看到该标记后按 backfill_limit 有限窗口回补；
        标记落库，即使恢复后立刻重启也不会丢失回补范围。
        """
        sub = self.get_subscription(src)
        if sub is not None and sub["status"] == INVALID:
            self.con.execute(
                "UPDATE subscriptions SET status=?,catchup_pending=1,updated_run=? "
                "WHERE src=?", (ACTIVE, self.run_id, src))
            self._audit(src, "source_recovered",
                        {"from": INVALID, "to": ACTIVE},
                        sub["cursor_ts"], sub["cursor_id"])
            self.con.commit()

    # -- 逐事件处理（流水线阶段入口） -------------------------------------------
    def handle(self, rec: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if rec is None:
            return None
        src = str(rec.get("src", "")).strip().lower()
        if not src:
            return rec
        sub = self.get_subscription(src)
        if sub is None:
            # 未显式注册的来源：以默认过滤/回补深度自动建订阅（happy-path 兼容）。
            sub = self.upsert_subscription(src)

        if sub["status"] == INVALID:
            # 失效来源的事件理论上已在 feed_pull 被拦；防御性跳过，游标不动。
            return rec
        if sub["status"] == PAUSED:
            # 暂停：事件继续入库，不投递、游标冻结，恢复后从冻结点补看。
            self.stats["paused_held"] += 1
            return rec

        if not after_cursor(rec, sub["cursor_ts"], sub["cursor_id"]):
            # 游标之前的历史：不重复处理（过滤变化也不回看，见模块文档）。
            return rec

        return self._handle_one(src, sub, rec)

    # -- 每轮窗口准备 -----------------------------------------------------------
    def prepare_sources(self, available: dict[str, list[dict[str, Any]]]) -> None:
        """每轮拉取结束、逐事件处理前调用一次。

        ``available``：src -> 本轮从该来源拉到的、按 (ts,id) 升序的事件列表。
        对两类订阅计算有限回补窗口，窗口外的老事件跳过并写审计、游标先推到
        窗口起点：
        1. 新订阅（cursor 为空）——首次回补，深度 backfill_limit；
        2. 刚从 invalid 恢复的订阅（catchup_pending=1）——恢复回补，同深度，
           窗口处理完清除标记（标记落库，重启不丢）。
        暂停/恢复不走有限窗口：游标冻结，恢复后从冻结点无界补看。
        """
        for src, events in available.items():
            src = str(src).strip().lower()
            sub = self.get_subscription(src)
            existed = sub is not None
            if not existed:
                sub = self.upsert_subscription(src)
            # 首次回补：backfill_done=0 且游标仍空；恢复回补：catchup_pending=1。
            is_new = (not sub["backfill_done"]) and sub["cursor_ts"] is None
            is_catchup = sub["catchup_pending"]
            if sub["status"] != ACTIVE or not (is_new or is_catchup):
                continue
            ordered = sorted(events, key=position_of)
            limit = int(sub["backfill_limit"])
            # 恢复回补只在“游标之后”的待处理事件上算窗口；游标及之前的事件
            # 已处理完毕，不参与截断。
            window_input = ordered
            if is_catchup and sub["cursor_ts"] is not None:
                window_input = [e for e in ordered
                                if after_cursor(e, sub["cursor_ts"], sub["cursor_id"])]
            # 有限窗口的明确选择：
            # - 恢复回补（is_catchup）：游标之后补看，只保留最近 limit 条；
            # - 首次回补（is_new）：只有订阅在本轮拉取之前就已显式注册
            #   （existed=True）才按 limit 截断——边界在处理任何事件之前算出
            #   并随事务落库，半批重启不会二次截断。本轮自动建的订阅，其首批
            #   事件就是订阅时刻的“现在”，全量投递、不截断。
            truncate = is_catchup or (is_new and existed)
            if truncate and len(window_input) > limit:
                skipped = window_input[: len(window_input) - limit]
                first_kept = window_input[len(window_input) - limit]
                boundary = position_of(skipped[-1])
                self._advance_cursor(src, boundary[0], boundary[1])
                kind = "backfill_skip" if is_new else "recovery_skip"
                self._audit(src, kind,
                            {"skipped": len(skipped),
                             "reason": ("bounded_backfill" if is_new
                                        else "bounded_recovery"),
                             "limit": limit,
                             "window_starts_after": list(boundary),
                             "first_kept": list(position_of(first_kept))},
                            boundary[0], boundary[1])
                self.stats["backfill_skipped"] += len(skipped)
            if is_new:
                # 标记首次回补窗口已消费：后续批次即使更大也不会再次截断历史。
                self.con.execute(
                    "UPDATE subscriptions SET backfill_done=1,updated_run=? WHERE src=?",
                    (self.run_id, src))
                self._audit(src, "backfill_window_prepared",
                            {"events_in_window": len(ordered), "limit": limit},
                            sub["cursor_ts"], sub["cursor_id"])
            if is_catchup:
                self.con.execute(
                    "UPDATE subscriptions SET catchup_pending=0,updated_run=? WHERE src=?",
                    (self.run_id, src))
                self._audit(src, "catchup_window_prepared",
                            {"events_in_window": min(len(window_input), limit)},
                            sub["cursor_ts"], sub["cursor_id"])
            self.con.commit()

    def note_source_down(self, src: str) -> None:
        self.mark_invalid(src)

    def note_source_up(self, src: str) -> None:
        self.mark_recovered(src)

    def _handle_one(self, src: str, sub: dict[str, Any],
                    rec: dict[str, Any]) -> dict[str, Any]:
        out = dict(rec)
        if not match_filters(rec, sub["filters"]):
            # 不匹配：不投递，但游标照常推进，否则同一事件会被无限重看。
            self._advance_to(src, rec)
            self._audit(src, "filtered_out",
                        {"event_id": str(rec.get("id", "")),
                         "config_ver": sub["config_ver"]},
                        *position_of(rec))
            self.stats["skipped_filter"] += 1
            return out
        event_id = str(rec.get("id", ""))
        cur = self.con.execute(
            "INSERT OR IGNORE INTO deliveries(src,event_id,config_ver,run_id,ts,notified_at) "
            "VALUES(?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            (src, event_id, sub["config_ver"], self.run_id, str(rec.get("ts", ""))))
        delivered = cur.rowcount == 1
        if delivered:
            self.stats["notified"] += 1
            self._audit(src, "delivered",
                        {"event_id": event_id, "config_ver": sub["config_ver"]},
                        *position_of(rec))
        else:
            # 重放/半批重启命中已投递记录：不重复通知，仍保证游标继续向前。
            self._audit(src, "delivery_dedup",
                        {"event_id": event_id, "config_ver": sub["config_ver"]},
                        *position_of(rec))
        self._advance_to(src, rec)
        out["_subscribed"] = {"src": src, "event_id": event_id,
                              "config_ver": sub["config_ver"], "delivered": delivered}
        self._pending += 1
        if self._pending >= self.batch_size:
            self.commit_batch()
        return out

    def commit_batch(self) -> None:
        """把本批“事件入库 + 通知 + 游标推进”作为一个事务提交。"""
        if self._pending == 0:
            self.con.commit()
            return
        self.con.commit()
        self._pending = 0
        self._commits += 1
        if 0 <= self.crash_after_commits <= self._commits:
            raise RuntimeError(
                f"injected crash after {self._commits} batch commit(s)")

    def finalize(self) -> dict[str, Any]:
        self.commit_batch()
        return dict(self.stats)

    # -- 基础工具 ---------------------------------------------------------------
    def _require(self, src: str) -> dict[str, Any]:
        sub = self.get_subscription(src)
        if sub is None:
            raise KeyError(f"subscription not found for source: {src}")
        return sub

    def _set_status(self, src: str, status: str, kind: str,
                    sub: dict[str, Any], extra: Optional[dict] = None) -> None:
        self.con.execute(
            "UPDATE subscriptions SET status=?,updated_run=? WHERE src=?",
            (status, self.run_id, src))
        detail = {"from": sub["status"], "to": status}
        if extra:
            detail.update(extra)
        self._audit(src, kind, detail, sub["cursor_ts"], sub["cursor_id"])
        self.con.commit()

    def _advance_cursor(self, src: str, ts: str, event_id: str) -> None:
        self.con.execute(
            "UPDATE subscriptions SET cursor_ts=?,cursor_id=?,updated_run=? WHERE src=?",
            (ts, event_id, self.run_id, src))

    def _advance_to(self, src: str, rec: dict[str, Any]) -> None:
        ts, event_id = position_of(rec)
        self._advance_cursor(src, ts, event_id)

    def _audit(self, src: str, kind: str, detail: Any,
               cursor_ts: Optional[str], cursor_id: Optional[str]) -> None:
        self.con.execute(
            "INSERT INTO sub_audit(src,run_id,kind,detail,cursor_ts,cursor_id) "
            "VALUES(?,?,?,?,?,?)",
            (src, self.run_id, kind,
             json.dumps(detail, ensure_ascii=False, default=str), cursor_ts, cursor_id))


# ---- 流水线阶段接口 ----------------------------------------------------------
def bind_engine(engine: SubscriptionEngine):
    """编排层在本轮运行前注入引擎，返回 token 以便用完复位。"""
    return _engine_ctx.set(engine)


def reset_engine(token=None) -> None:
    if token is not None:
        _engine_ctx.reset(token)
    else:
        _engine_ctx.set(None)


def process(record):
    """subscribe 阶段：必须由编排层通过 bind_engine 注入引擎后使用。"""
    engine = _engine_ctx.get()
    if engine is None:
        raise RuntimeError("subscribe.process() requires an engine bound via bind_engine()")
    return engine.handle(record)
