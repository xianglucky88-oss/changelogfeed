"""离线测试夹具：脚本化的 httpx MockTransport，无需起服务即可复现
304/410/429/5xx、断线（TransportError）、截断、分页与续拉等全部场景。

handler 以请求 URL 为键给出响应脚本（按调用次序消费），支持：
* ``(status, headers_dict, body_bytes)`` 元组
* ``Exception`` 实例——模拟连接/读取中断
"""
import httpx

from src.changelogfeed.transport import Transport


class ScriptedHandler:
    def __init__(self, routes: dict):
        # routes: {url 或 url 前缀: [响应脚本...]}
        self._routes = routes
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = str(request.url)
        # 精确匹配优先，其次最长前缀，避免分页 URL 被短前缀吞掉
        matches = [p for p in self._routes if url == p or url.startswith(p)]
        if not matches:
            return httpx.Response(404, content=b"not scripted")
        pattern = max(matches, key=len)
        script = self._routes[pattern]
        if not script:
            raise AssertionError(f"unexpected extra request: {url}")
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        status, headers, body = item
        return httpx.Response(status, headers=headers, content=body)

    @property
    def call_count(self) -> int:
        return len(self.requests)

    def urls(self) -> list[str]:
        return [str(r.url) for r in self.requests]

    def header_sets(self) -> list[dict]:
        return [{k.lower(): v for k, v in r.headers.items()}
                for r in self.requests]


def make_transport(routes: dict) -> tuple[Transport, ScriptedHandler]:
    handler = ScriptedHandler(routes)
    return Transport(mock_handler=handler), handler
