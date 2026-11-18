"""单一职责：把单次 HTTP 交互收敛为离线可替换的传输层（不含重试/缓存判断）。

生产用 httpx 真实传输；测试通过 ``MockTransport``（handler 脚本化响应）
即可完全离线复现 304/410/429/断线/截断等所有分支，无需起服务。
"""
from __future__ import annotations

from dataclasses import dataclass

import httpx

USER_AGENT = "changelogfeed/1.0 (+offline)"


class TransportError(Exception):
    """传输层失败（连接中断、读取中断、超时等），供上层归类为暂时错误。"""


@dataclass(frozen=True)
class RawResponse:
    """一次请求的原始结果，不做任何业务判断。"""
    status: int
    headers: dict[str, str]
    content: bytes
    url: str

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())


def _norm_headers(headers) -> dict[str, str]:
    return {k.lower(): v for k, v in dict(headers).items()}


class Transport:
    """真实/模拟传输的统一薄封装。"""

    def __init__(self, client: httpx.Client | None = None,
                 mock_handler=None, timeout: float = 10.0):
        if client is not None:
            self._client = client
            self._owns = False
        elif mock_handler is not None:
            self._client = httpx.Client(
                transport=httpx.MockTransport(mock_handler), timeout=timeout)
            self._owns = True
        else:  # pragma: no cover - 真实联网路径，离线测试不进入
            self._client = httpx.Client(
                headers={"user-agent": USER_AGENT}, timeout=timeout)
            self._owns = True

    def get(self, url: str, headers: dict[str, str] | None = None) -> RawResponse:
        try:
            r = self._client.get(url, headers=headers or {})
        except httpx.HTTPError as exc:
            # 连接被拒、读中断、超时等统一成可诊断的传输异常
            raise TransportError(f"{type(exc).__name__}: {exc}") from exc
        except Exception as exc:
            # MockTransport 脚本化的普通异常（模拟断线）同样归类为传输失败
            raise TransportError(f"{type(exc).__name__}: {exc}") from exc
        return RawResponse(
            status=r.status_code,
            headers=_norm_headers(r.headers),
            content=r.content,
            url=str(r.url),
        )

    def close(self) -> None:
        if self._owns:
            self._client.close()

    def __enter__(self) -> "Transport":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
