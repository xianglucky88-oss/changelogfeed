"""单一职责：多源 release / changelog / advisory 去重（流水线阶段）。

以四类身份键识别同一发布的重传与更新：
  1. 规范 URL（canonical_url）
  2. 发布标识（release_id / rid）
  3. 版本范围（component + 归一化版本/版本范围，来自 semver 阶段）
  4. 内容摘要（component + 标题 + 正文指纹）

持久化状态（与 store 共用同一 sqlite 连接，随同一事务提交/回滚）：
  dedup_groups   —— 每个发布一份归并状态（首次发现时间、版本链、来源关系）
  dedup_aliases  —— 身份键 → group_id 映射，支撑页面改版/镜像补登记
  dedup_journal  —— 仅追加事件流水，规则升级后可完整回放重建状态
  dedup_emitted  —— 下游触发台账（首次发现/每次修订/撤回各一次，重传不触发）

处理结果（返回 (规范记录, 是否触发下游)）：
  首次发现或产生修订/撤回时返回归并记录且 triggers=True（relate / store 消费）；
  来源补登记返回更新后的归并记录但 triggers=False（store 刷新，不产生下游触发）；
  纯重传返回 (None, False)，pipeline 据此跳过下游，保证重复执行不重复触发。
"""
import hashlib
import json

RULE_VERSION = "dedup-2026.11.17"

# 静态 URL 改版映射：页面迁移后旧地址仍应归并到同一发布。
# 命中映射的 URL 先替换为目标地址，再做规范化。
URL_ALIASES = {
    "https://changelog.example.com/v1/pkg-x/1-2-0":
        "https://changelog.example.com/pkg-x/1.2.0",
}

# 受 dedup 管理的记录需要具备发布域字段；缺省的通用记录走基线直通逻辑。
_DOMAIN_FIELDS = ("url", "rid", "version", "ranges", "title", "body", "component", "event")


def _now():
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_url(url):
    """小写 host、去锚点、去冗余斜杠与常见跟踪参数，得到规范 URL。"""
    if not url:
        return ""
    s = str(url).strip()
    if "://" not in s:
        s = "https://" + s
    # 页面改版别名按“去查询串/锚点”的地址匹配，再替换为目标地址。
    bare = s.split("#", 1)[0].split("?", 1)[0]
    if bare in URL_ALIASES:
        s = URL_ALIASES[bare]
    elif s in URL_ALIASES:
        s = URL_ALIASES[s]
    head, _, rest = s.partition("://")
    head = head.lower()
    if "#" in rest:
        rest = rest.split("#", 1)[0]
    path, _, query = rest.partition("?")
    host, _, pathpart = path.partition("/")
    host = host.lower()
    pathpart = "/" + "/".join(seg for seg in pathpart.split("/") if seg)
    kept = []
    if query:
        for pair in query.split("&"):
            key = pair.split("=", 1)[0].lower()
            if key in {"utm_source", "utm_medium", "utm_campaign", "ref", "fbclid", "gclid"}:
                continue
            kept.append(pair)
    out = f"{head}://{host}{pathpart}"
    if kept:
        out += "?" + "&".join(kept)
    return out


def content_fingerprint(rec):
    """标题+正文的完整内容摘要（含正文为空的镜像）。"""
    title = str(rec.get("title_norm") or rec.get("title") or "").strip()
    body = str(rec.get("body_norm") or rec.get("body") or "").strip()
    digest = hashlib.sha256(f"{title}\n{body}".encode("utf-8")).hexdigest()[:16]
    component = str(rec.get("component") or "").strip().lower()
    return f"fp:{component}:{digest}"


def title_fingerprint(rec):
    """仅标题摘要：镜像缺正文时仍可与官方发布归并，不因空字段产生新身份。"""
    title = str(rec.get("title_norm") or rec.get("title") or "").strip()
    digest = hashlib.sha256(title.encode("utf-8")).hexdigest()[:16]
    component = str(rec.get("component") or "").strip().lower()
    return f"tp:{component}:{digest}"


def evidence_fingerprint(rec):
    """单条来源证据的指纹，用于识别同一来源的原样重传。"""
    payload = {
        "url": canonical_url(rec.get("url")),
        "rid": str(rec.get("rid") or "").strip().lower(),
        "version": rec.get("version_norm") or str(rec.get("version") or "").strip().lower(),
        "title": str(rec.get("title_norm") or rec.get("title") or "").strip(),
        "body": str(rec.get("body_norm") or rec.get("body") or "").strip(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]


def range_key(rec):
    """版本范围身份键：组件 + 归一化版本范围集合（semver 结果为比较输入）。

    通告类来源通常只给受影响范围、没有具体版本，因此范围键与版本键分开构造，
    使范围重叠的通告与发布仍能归并到同一发布。
    """
    component = str(rec.get("component") or "").strip().lower()
    if not component:
        return ""
    ranges = rec.get("ranges_norm")
    if ranges is None and rec.get("ranges"):
        ranges = sorted(str(r).strip().lower() for r in rec["ranges"] if str(r).strip())
    if not ranges:
        return ""
    return f"rng:{component}:" + "|".join("r=" + r for r in ranges)


def version_key(rec):
    """具体版本身份键：组件 + semver 归一化版本。"""
    component = str(rec.get("component") or "").strip().lower()
    if not component:
        return ""
    ver = rec.get("version_norm")
    if not ver and rec.get("version") is not None:
        ver = str(rec["version"]).strip().lower()
    return f"ver:{component}:{ver}" if ver else ""


def identity_keys(rec):
    """按规范 URL → 发布标识 → 版本范围 → 版本 → 内容摘要的顺序构造候选身份键。"""
    keys = []
    cu = canonical_url(rec.get("url"))
    if cu:
        keys.append("url:" + cu)
    rid = str(rec.get("rid") or "").strip().lower()
    if rid:
        keys.append("rid:" + rid)
    rk = range_key(rec)
    if rk:
        keys.append(rk)
    vk = version_key(rec)
    if vk:
        keys.append(vk)
    keys.append(title_fingerprint(rec))
    keys.append(content_fingerprint(rec))
    # 去重但保持优先级顺序
    seen, ordered = set(), []
    for k in keys:
        if k not in seen:
            seen.add(k)
            ordered.append(k)
    return ordered


def is_managed(rec):
    """带有发布域字段的记录由本阶段管理；通用记录沿用基线直通。"""
    return any(rec.get(f) not in (None, "", []) for f in _DOMAIN_FIELDS)


def _group_id(rec, first_key):
    component = str(rec.get("component") or "").strip().lower() or "pub"
    digest = hashlib.sha256(first_key.encode("utf-8")).hexdigest()[:12]
    return f"grp-{component}-{digest}"


class Deduper:
    """有状态去重器；状态落在传入的 sqlite 连接上，可跨重放/崩溃恢复。"""

    def __init__(self, con):
        self.con = con
        self._init_tables()
        self.heal()

    # ---------- 持久化结构 ----------
    def _init_tables(self):
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS dedup_groups(
                group_id        TEXT PRIMARY KEY,
                state           TEXT NOT NULL,
                first_seen      TEXT NOT NULL,
                status          TEXT NOT NULL,
                triggers        INTEGER NOT NULL,
                rule_version    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dedup_aliases(
                id_key          TEXT PRIMARY KEY,
                group_id        TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dedup_journal(
                event_id        TEXT PRIMARY KEY,
                group_id        TEXT NOT NULL,
                payload         TEXT NOT NULL,
                rule_version    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dedup_emitted(
                group_id        TEXT NOT NULL,
                emission_seq    INTEGER NOT NULL,
                event_id        TEXT NOT NULL,
                kind            TEXT NOT NULL,
                PRIMARY KEY(group_id, emission_seq, kind)
            );
            """
        )

    def heal(self):
        """修复半写入：补齐缺失的身份键登记，使中断后的状态与完整处理一致。"""
        groups = self.con.execute("SELECT group_id, state FROM dedup_groups").fetchall()
        for gid, state_text in groups:
            try:
                state = json.loads(state_text)
            except (ValueError, TypeError):
                continue
            keys = set(state.get("identity_keys", ()))
            keys.update(state.get("aliases", ()))
            known = {
                row[0]
                for row in self.con.execute(
                    "SELECT id_key FROM dedup_aliases WHERE group_id=?", (gid,)
                )
            }
            for key in keys - known:
                self._register_alias(key, gid)

    def replay_journal(self):
        """规则升级后的可回放依据：清空派生状态，按事件流水重建。

        原始 payload 完整保留，升级后的判定规则可在回放时重新应用；
        回放结束后下游触发次数与按新规则完整处理一致。
        """
        rows = self.con.execute(
            "SELECT payload FROM dedup_journal ORDER BY rowid"
        ).fetchall()
        self.con.executescript(
            "DELETE FROM dedup_emitted; DELETE FROM dedup_aliases; "
            "DELETE FROM dedup_groups; DELETE FROM dedup_journal;"
        )
        replayed = 0
        for (payload_text,) in rows:
            payload = json.loads(payload_text)
            payload["_replayed"] = True
            self.handle(payload)
            replayed += 1
        return replayed

    # ---------- 主入口 ----------
    def handle(self, rec):
        """处理一条已归一化记录；返回 (规范记录或 None, 是否触发下游)。"""
        event_id = str(rec.get("id") or evidence_fingerprint(rec))
        row = self.con.execute(
            "SELECT group_id FROM dedup_journal WHERE event_id=?", (event_id,)
        ).fetchone()
        if row is not None:
            # 同一事件编号再次到达即原样重传：不产生新状态、不触发下游。
            return None, False
        return self._handle_new(rec, event_id, replayed=bool(rec.get("_replayed")))

    def _handle_new(self, rec, event_id, replayed):
        keys = identity_keys(rec)
        gid = self._resolve_group(keys)
        is_new_group = gid is None
        if is_new_group:
            gid = _group_id(rec, keys[0])
            state = self._new_state(rec, gid)
            # 乱序到达：新 group 的首条事件也可能是撤回（通告先于发布到达）。
            event_type = str(rec.get("event") or "publish").strip().lower()
            kind = "withdraw" if event_type == "withdraw" else "first_seen"
        else:
            state = json.loads(
                self.con.execute(
                    "SELECT state FROM dedup_groups WHERE group_id=?", (gid,)
                ).fetchone()[0]
            )
            kind = self._classify(rec, state)

        if kind == "retransmit":
            # 同一来源的原样重传：走完来源/证据登记，但不改发布内容、
            # 不触发下游；事件流水仅追加一次（event_id 主键幂等）。
            self._apply_event(rec, state, kind, event_id)
            self._persist_group(gid, state)
            self._append_journal(event_id, gid, rec, replayed)
            return None, False

        # _apply_event 可能把“来源补登记”升级为乱序补登的“首次发现”。
        kind = self._apply_event(rec, state, kind, event_id)
        for key in keys:
            self._register_alias(key, gid)
        state.setdefault("identity_keys", [])
        for key in keys:
            if key not in state["identity_keys"]:
                state["identity_keys"].append(key)
        self._persist_group(gid, state)
        self._append_journal(event_id, gid, rec, replayed)
        canonical = self._canonical_record(gid, state)
        if kind in ("first_seen", "revise", "withdraw"):
            # 首次发现 / 修订 / 撤回才写下游触发台账；
            # 来源补登记只刷新归并记录（store upsert），不产生触发。
            self._record_emission(gid, state, event_id, kind)
            # 下游触发次数以台账为唯一事实来源，避免状态机重复计数。
            state["triggers"] = self.con.execute(
                "SELECT COUNT(*) FROM dedup_emitted WHERE group_id=?", (gid,)
            ).fetchone()[0]
            self._persist_group(gid, state)
            canonical = self._canonical_record(gid, state)
            return canonical, True
        return canonical, False

    # ---------- 身份解析 ----------
    def _resolve_group(self, keys):
        for key in keys:
            row = self.con.execute(
                "SELECT group_id FROM dedup_aliases WHERE id_key=?", (key,)
            ).fetchone()
            if row is not None:
                return row[0]
        return None

    def _register_alias(self, key, gid):
        # 身份键一旦归属某发布不再改投；INSERT OR IGNORE 保证并发/重入幂等。
        self.con.execute(
            "INSERT OR IGNORE INTO dedup_aliases(id_key, group_id) VALUES(?,?)",
            (key, gid),
        )

    # ---------- 状态机 ----------
    def _new_state(self, rec, gid):
        now = _now()
        return {
            "group_id": gid,
            "component": str(rec.get("component") or "").strip().lower(),
            "status": "active",
            "first_event_id": str(rec.get("id") or evidence_fingerprint(rec)),
            "first_seen": rec.get("ts") or now,
            "first_seen_at": now,
            "last_seen": rec.get("ts") or now,
            "revision_seq": 0,
            "emission_seq": 0,
            "triggers": 0,
            "revision_of": [],
            "withdrawn": False,
            "withdrawn_event_id": None,
            "version_chain": [],
            "sources": {},
            "identity_keys": [],
            "publish_seen": False,
            "rule_version": RULE_VERSION,
        }

    def _classify(self, rec, state):
        """区分撤回 / 修订 / 重传 / 来源补登记；判定只依赖内容，与到达顺序无关。"""
        event_type = str(rec.get("event") or "publish").strip().lower()
        if event_type == "withdraw" and not state.get("withdrawn"):
            return "withdraw"

        source = str(rec.get("src") or "unknown").strip().lower()
        fp = evidence_fingerprint(rec)
        known = state["sources"].get(source)
        same_evidence = bool(known and known.get("evidence_fp") == fp)

        changed = self._content_changed(rec, state)
        if event_type == "revise":
            # 显式修订事件：与已存证据完全一致时按重传处理，否则计修订。
            return "retransmit" if same_evidence and not changed else "revise"
        if changed:
            return "revise"
        if same_evidence:
            return "retransmit"
        # 新来源（如镜像公告）带来同一发布的另一份拷贝，或仅回填缺失字段：
        # 属于来源补登记，不改变发布内容，不触发下游。
        return "source_add"

    def _content_changed(self, rec, state):
        """判断同一来源的内容是否真的发生修订。

        不同来源携带的是同一发布的不同文档（通告正文 vs release notes），
        跨来源文本差异属于来源补登记而非修订；只有同一来源前后证据不一致，
        或版本号升级时才计修订。
        """
        source = str(rec.get("src") or "unknown").strip().lower()
        known = state["sources"].get(source)
        if known:
            title = str(rec.get("title_norm") or rec.get("title") or "").strip()
            body = str(rec.get("body_norm") or rec.get("body") or "").strip()
            if title and title != known.get("title_norm"):
                return True
            if body and body != known.get("body_norm"):
                return True
        ver = rec.get("version_norm") or (
            str(rec["version"]).strip().lower() if rec.get("version") is not None else ""
        )
        # 版本号升级才算修订；若 group 此前没有版本（通告先到、发布后到的
        # 乱序场景），首次回填版本属于来源补登记，不产生修订。
        if ver and state.get("current_version") and ver != state["current_version"]:
            return True
        return False

    def _apply_event(self, rec, state, kind, event_id):
        source = str(rec.get("src") or "unknown").strip().lower()
        ts = rec.get("ts") or _now()
        state["last_seen"] = ts

        # 来源关系始终保留（含镜像缺字段的补登记与重传来源）。
        evidence = state["sources"].setdefault(
            source,
            {
                "src": source,
                "first_seen": ts,
                "evidence_fp": evidence_fingerprint(rec),
                "events": [],
                "missing_fields": [],
            },
        )
        evidence["last_seen"] = ts
        evidence["evidence_fp"] = evidence_fingerprint(rec)
        # 保留每个来源各自的内容快照，供同来源修订判定使用。
        if rec.get("title_norm") or rec.get("title"):
            evidence["title_norm"] = str(
                rec.get("title_norm") or rec.get("title")
            ).strip()
        if rec.get("body_norm") or rec.get("body"):
            evidence["body_norm"] = str(
                rec.get("body_norm") or rec.get("body")
            ).strip()
        if event_id not in evidence["events"]:
            evidence["events"].append(event_id)
        missing = [
            f
            for f in ("url", "rid", "version", "ranges", "title", "body")
            if rec.get(f) in (None, "", [])
        ]
        for f in missing:
            if f not in evidence["missing_fields"]:
                evidence["missing_fields"].append(f)

        # 规范 URL 与发布标识随新证据补全（镜像源缺字段的典型场景）。
        cu = canonical_url(rec.get("url"))
        if cu and cu not in state.get("urls", []):
            state.setdefault("urls", []).append(cu)
        rid = str(rec.get("rid") or "").strip().lower()
        if rid and rid not in state.get("rids", []):
            state.setdefault("rids", []).append(rid)

        # 任何事件都先做字段回填：乱序到达（撤回先于发布）时，后续发布证据
        # 仍能补全标题/正文/版本；镜像缺字段不会覆盖已有内容。
        self._capture_content(rec, state)
        event_type = str(rec.get("event") or "publish").strip().lower()
        backfill_first_seen = (
            kind == "source_add"
            and not state.get("publish_seen")
            and event_type == "publish"
        )

        if kind == "first_seen":
            state["publish_seen"] = True
            self._bump_version_chain(state, rec)

        elif backfill_first_seen:
            # 乱序恢复：group 由撤回通告先行建立，首份发布证据到达时
            # 补发“首次发现”下游触发，保证最终触发数与顺序无关。
            state["publish_seen"] = True
            self._bump_version_chain(state, rec)
            kind = "first_seen"

        elif kind == "revise":
            self._bump_version_chain(state, rec)
            state["revision_seq"] += 1
            state["revision_of"].append(
                {
                    "seq": state["revision_seq"],
                    "event_id": event_id,
                    "src": source,
                    "ts": ts,
                    "fingerprint": content_fingerprint(rec),
                }
            )

        elif kind == "withdraw":
            if not state.get("withdrawn"):
                state["withdrawn"] = True
                state["withdrawn_event_id"] = event_id
                state["status"] = "withdrawn"
            state.setdefault("withdraw_sources", [])
            if source not in state["withdraw_sources"]:
                state["withdraw_sources"].append(source)

        # source_add / retransmit：仅补来源关系与字段，版本链与触发数不变。
        return kind

    def _capture_content(self, rec, state):
        """更新归并内容快照；镜像缺字段（空串）不覆盖已有证据，只做回填。

        通告类来源的正文是漏洞描述而非发布说明，不覆盖发布正文，
        仅登记在来源关系中；标题取信息更完整的一份。
        """
        source = str(rec.get("src") or "unknown").strip().lower()
        is_advisory = source == "advisory"
        title = str(rec.get("title") or "").strip()
        if title and (not is_advisory or not state.get("title")):
            state["title"] = title
        body = str(rec.get("body") or "").strip()
        if body and not is_advisory:
            state["body"] = body
        title_norm = str(rec.get("title_norm") or title).strip()
        if title_norm and (not is_advisory or not state.get("title_norm")):
            state["title_norm"] = title_norm
        body_norm = str(rec.get("body_norm") or body).strip()
        if body_norm and not is_advisory:
            state["body_norm"] = body_norm
        ver = rec.get("version_norm") or (
            str(rec["version"]).strip().lower() if rec.get("version") is not None else ""
        )
        # 版本链追加放在各 kind 分支内：来源补登记只回填当前版本字段，
        # 不把“新来源带来的版本”记成一次修订。
        if ver:
            state["current_version"] = ver
        if rec.get("ranges_norm"):
            state["current_ranges"] = rec["ranges_norm"]
        state.setdefault("current_ranges", [])

    def _bump_version_chain(self, state, rec):
        ver = rec.get("version_norm") or (
            str(rec["version"]).strip().lower() if rec.get("version") is not None else ""
        )
        if not ver:
            return
        chain = state.setdefault("version_chain", [])
        if not any(node.get("version") == ver for node in chain):
            from .semver import version_sort_key
            chain.append(
                {
                    "version": ver,
                    "ts": rec.get("ts") or _now(),
                    "src": str(rec.get("src") or "unknown").strip().lower(),
                }
            )
            chain.sort(key=lambda n: (version_sort_key(n["version"]), n["ts"]))

    # ---------- 落库 ----------
    def _persist_group(self, gid, state):
        self.con.execute(
            "INSERT INTO dedup_groups(group_id,state,first_seen,status,triggers,rule_version) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(group_id) DO UPDATE SET state=excluded.state, "
            "first_seen=excluded.first_seen, status=excluded.status, "
            "triggers=excluded.triggers, rule_version=excluded.rule_version",
            (
                gid,
                json.dumps(state, ensure_ascii=False, sort_keys=True),
                state["first_seen"],
                state["status"],
                state["triggers"],
                RULE_VERSION,
            ),
        )

    def _append_journal(self, event_id, gid, rec, replayed):
        payload = {k: v for k, v in rec.items() if k != "_replayed"}
        self.con.execute(
            "INSERT OR IGNORE INTO dedup_journal(event_id,group_id,payload,rule_version) "
            "VALUES(?,?,?,?)",
            (event_id, gid, json.dumps(payload, ensure_ascii=False, sort_keys=True),
             RULE_VERSION),
        )

    def _record_emission(self, gid, state, event_id, kind):
        # 每次下游触发占用 group 内单调递增的独立序号；
        # 撤回不占用修订序号，revision_seq 仅反映内容修订次数。
        state["emission_seq"] = state.get("emission_seq", 0) + 1
        self.con.execute(
            "INSERT OR IGNORE INTO dedup_emitted(group_id,emission_seq,event_id,kind) "
            "VALUES(?,?,?,?)",
            (gid, state["emission_seq"], event_id, kind),
        )

    # ---------- 输出 ----------
    def _canonical_record(self, gid, state):
        return {
            "id": gid,
            "dedup": {
                "group_id": gid,
                "rule_version": RULE_VERSION,
                "first_seen": state["first_seen"],
                "first_seen_at": state["first_seen_at"],
                "last_seen": state["last_seen"],
                "revision_seq": state["revision_seq"],
                "revisions": list(state.get("revision_of", [])),
                "withdrawn": state.get("withdrawn", False),
                "withdrawn_event_id": state.get("withdrawn_event_id"),
                "triggers": state["triggers"],
                "version_chain": list(state.get("version_chain", [])),
                "sources": {k: dict(v) for k, v in state.get("sources", {}).items()},
                "identity_keys": list(state.get("identity_keys", [])),
                "canonical_urls": list(state.get("urls", [])),
                "release_ids": list(state.get("rids", [])),
            },
            "component": state.get("component", ""),
            "title": state.get("title", ""),
            "body": state.get("body", ""),
            "version": state.get("current_version", ""),
            "ranges": list(state.get("current_ranges", [])),
            "status": state.get("status", "active"),
            "ts": state.get("first_seen"),
        }

    # ---------- 测试/检视辅助 ----------
    def list_groups(self):
        rows = self.con.execute(
            "SELECT group_id, state FROM dedup_groups ORDER BY group_id"
        ).fetchall()
        return [(gid, json.loads(text)) for gid, text in rows]

    def trigger_count(self, gid=None):
        if gid is None:
            return self.con.execute(
                "SELECT COALESCE(SUM(triggers),0) FROM dedup_groups"
            ).fetchone()[0]
        row = self.con.execute(
            "SELECT triggers FROM dedup_groups WHERE group_id=?", (gid,)
        ).fetchone()
        return row[0] if row else 0

    def emission_rows(self):
        return self.con.execute(
            "SELECT group_id, emission_seq, event_id, kind "
            "FROM dedup_emitted ORDER BY group_id, emission_seq"
        ).fetchall()

    def journal_len(self):
        return self.con.execute("SELECT COUNT(*) FROM dedup_journal").fetchone()[0]


# ---------- 流水线对接：pipeline.run 绑定同一 sqlite 连接 ----------
_DEDUPER = None


def bind(con):
    """由 pipeline 在打开连接后调用，使 process() 使用持久化去重状态。"""
    global _DEDUPER
    _DEDUPER = Deduper(con)
    return _DEDUPER


def close():
    global _DEDUPER
    _DEDUPER = None


def process(record):
    """流水线阶段入口，统一返回 (记录或 None, 是否触发下游)。

    已绑定连接时执行有状态多源去重；未绑定连接或通用基线记录走朴素直通，
    不进入发布域状态机、不产生触发标记，保证既有样例的落库行为不变。
    """
    rec = dict(record)
    if _DEDUPER is None or not is_managed(rec):
        rec.setdefault("_stage_dedup", "ok")
        return rec, False
    return _DEDUPER.handle(rec)
