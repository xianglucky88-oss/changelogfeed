"""单一职责：语义版本/版本范围归一化（流水线阶段）。

归一化结果（``version_norm`` / ``ranges_norm``）作为下游 dedup 的比较输入；
不含归一化字段的记录保持基线行为，仅追加阶段标记。
"""
import re

_VER_RE = re.compile(r"^v?\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+][0-9A-Za-z.\-]*)?$")
_OP_RE = re.compile(r"^\s*(>=|<=|==|!=|=|>|<)?\s*(.*?)\s*$")


def normalize_version(value):
    """把 ``v1.2`` / ``1.2.3-rc1`` 等形式归一为 ``major.minor.patch``；无法识别返回 ''。"""
    if value is None:
        return ""
    m = _VER_RE.match(str(value).strip())
    if not m:
        return ""
    major, minor, patch = (int(g or 0) for g in m.groups())
    return f"{major}.{minor}.{patch}"


def normalize_range(value):
    """归一单条版本范围，如 ``>= 1.0.0`` → ``>=1.0.0``；操作符缺省视为 ``=``。"""
    if value is None:
        return ""
    s = re.sub(r"\s+", "", str(value))
    if not s:
        return ""
    m = _OP_RE.match(s)
    op, ver = (m.group(1) or "="), m.group(2)
    if op == "==":
        op = "="
    nv = normalize_version(ver)
    return f"{op}{nv}" if nv else s.lower()


def normalize_ranges(values):
    """归一并去重、排序版本范围列表，保证乱序/空格差异得到同一身份键。"""
    if not values:
        return []
    if isinstance(values, str):
        values = [values]
    return sorted({normalize_range(v) for v in values if str(v).strip()})


def version_sort_key(version):
    """供版本链排序使用；非数值版本排到末尾，保证稳定可比较。"""
    nums = re.findall(r"\d+", str(version or ""))
    if not nums:
        return (1, 0, 0, 0)
    return (0,) + tuple(int(n) for n in nums[:3]) + (0,) * (3 - min(3, len(nums)))


def process(record):
    rec = dict(record)
    rec.setdefault("_stage_semver", "naive")
    if rec.get("version") is not None:
        rec["version_norm"] = normalize_version(rec.get("version"))
    if rec.get("ranges"):
        rec["ranges_norm"] = normalize_ranges(rec.get("ranges"))
    return rec
