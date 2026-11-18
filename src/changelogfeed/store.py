"""单一职责：本地 sqlite3 持久化，提供幂等 upsert / 计数 / 全量读取。

另提供 feed 事件账本（events 表）：dedup 阶段用它保证重试/重跑
不会制造重复事件，账本写入同样是幂等的。
"""
import json, sqlite3
from .config import DB_PATH

def connect():
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    con.execute(
        "CREATE TABLE IF NOT EXISTS events("
        "event_key TEXT PRIMARY KEY, first_seen REAL NOT NULL, "
        "source TEXT NOT NULL, kind TEXT NOT NULL)")
    return con

def upsert(con, record):
    con.execute("INSERT INTO records(key,data) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (record.key, json.dumps(record.data, ensure_ascii=False)))

def commit(con): con.commit()

def count(con): return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]

# ---- feed 事件账本：跨运行幂等 ------------------------------------------
def event_exists(con, event_key):
    row = con.execute(
        "SELECT 1 FROM events WHERE event_key=?", (event_key,)).fetchone()
    return row is not None

def record_event(con, event_key, source, kind, seen_at):
    """已存在则不改写，返回 False；新事件插入并返回 True。"""
    cur = con.execute(
        "INSERT OR IGNORE INTO events(event_key,first_seen,source,kind) "
        "VALUES(?,?,?,?)", (event_key, seen_at, source, kind))
    return cur.rowcount > 0

def count_events(con):
    return con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
