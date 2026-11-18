"""单一职责：集中管理路径与默认策略（可被环境变量/测试覆盖）。"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"
DB_PATH = Path(os.environ.get("CHANGELOGFEED_DB", ROOT / "changelogfeed.db"))
# feed_pull 的磁盘缓存根目录（提交体、暂存分页、隔离区都在其下）
CACHE_DIR = Path(os.environ.get("CHANGELOGFEED_CACHE", ROOT / ".cache" / "feeds"))

DEFAULT_TTL_SECONDS = int(os.environ.get("CHANGELOGFEED_TTL", "300"))
DEFAULT_MAX_RETRIES = int(os.environ.get("CHANGELOGFEED_MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = 0.5
# Retry-After 给出的等待上限，避免离线/异常服务把进程无限挂起
MAX_RETRY_AFTER_SECONDS = 60.0
