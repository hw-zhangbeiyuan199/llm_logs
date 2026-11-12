"""httpx 转发：阻塞 + SSE 流式。

- 阻塞：整体转发，返回完整响应
- 流式：逐 chunk 透传给客户端，同时后台累积 delta 文本 + usage，结束后汇总
- 上游 4xx/5xx 原样透传状态码与 body
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

from app.config import Route

logger = logging.getLogger("llm_gateway.forwarder")


@dataclass
class ForwardResult:
    status_code: int = 0
    response_body: str = ""
    error: str = ""
    # 解析得到的统计字段
    model: Optional[str] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    # 流式专用
    accumulated_content: str = ""
    accumulated_reasoning: str = ""
    client_disconnected: bool = False
    # 推理模型：思维链（阻塞模式从 message.reasoning_content 取）
    reasoning: str = ""


def _upstream_url(route: Route, path: str) -> str:
    # base_url 含 /v1，path 形如 /chat/completions
    base = route.upstream.base_url.rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return base + path


def _passthrough_headers(incoming: dict, *, strip_accept: bool = False) -> dict:
    """透传 agent 请求头（含 Authorization），仅剔除 hop-by-hop 与由 httpx 重算的字段。

    网关只改指向，不替换 api_key。
    """
    skip = {"host", "content-length", "connection"}
    if strip_accept:
        skip.add("accept")
    return {k: v for k, v in incoming.items() if k.lower() not in skip}


def _upstream_headers(route: Route, incoming: dict) -> dict:
    return _passthrough_headers(incoming)


def _maybe_map_model(body: dict, route: Route) -> None:
    mm = route.upstream.model_mapping
    if mm and "model" in body:
        body["model"] = mm.get(body["model"], body["model"])


async def forward_blocking(
    route: Route,
    path: str,
    body: dict,
    incoming_headers: dict,
    timeout: float,
) -> tuple[httpx.Response, ForwardResult]:
    """阻塞转发。返回 (原始 Response, 解析结果)。"""
    _maybe_map_model(body, route)
    url = _upstream_url(route, path)
    headers = _upstream_headers(route, incoming_headers)
    result = ForwardResult()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=body, headers=headers)
            result.status_code = resp.status_code
            result.response_body = resp.text
            try:
                j = resp.json()
                result.model = j.get("model")
                u = j.get("usage") or {}
                result.prompt_tokens = u.get("prompt_tokens")
                result.completion_tokens = u.get("completion_tokens")
                result.total_tokens = u.get("total_tokens")
                # 推理模型：捕获思维链
                choices = j.get("choices") or []
                if choices:
                    msg = choices[0].get("message") or {}
                    rc = msg.get("reasoning_content")
                    if rc:
                        result.reasoning = rc
            except Exception:
                pass
            return resp, result
    except httpx.TimeoutException:
        result.error = "upstream timeout"
        result.status_code = 504
        raise
    except httpx.HTTPError as e:
        result.error = f"upstream error: {e}"
        result.status_code = 502
        raise


def _parse_sse_delta(line: str) -> dict:
    """解析单行 SSE data: {...}，返回 JSON dict 或空 dict。"""
    if not line:
        return {}
    if line.startswith(":") or line.startswith("event:") or line.startswith("id:"):
        return {}
    if line.startswith("data:"):
        payload = line[5:].strip()
        if payload == "[DONE]":
            return {"__done__": True}
        try:
            return json.loads(payload)
        except Exception:
            return {}
    return {}


async def forward_stream(
    route: Route,
    path: str,
    body: dict,
    incoming_headers: dict,
    timeout: float,
) -> tuple[httpx.Response, AsyncIterator[bytes], ForwardResult]:
    """流式转发。返回 (原始 Response, 字节迭代器, 结果对象)。

    调用方负责消费迭代器；迭代结束后 result.accumulated_content / usage 已填充。
    """
    _maybe_map_model(body, route)
    url = _upstream_url(route, path)
    # 透传 agent 头（含 Authorization），保留 agent 的 Accept（流式时为 text/event-stream）
    headers = _passthrough_headers(incoming_headers)

    result = ForwardResult()

    client = httpx.AsyncClient(timeout=timeout)
    req = client.build_request("POST", url, json=body, headers=headers)
    resp = await client.send(req, stream=True)
    result.status_code = resp.status_code

    async def gen() -> AsyncIterator[bytes]:
        buf = ""
        try:
            async for chunk in resp.aiter_bytes():
                # 透传
                yield chunk
                # 累积解析（按行）
                try:
                    text = chunk.decode("utf-8", errors="replace")
                except Exception:
                    continue
                buf += text
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.rstrip("\r")
                    if not line:
                        continue
                    obj = _parse_sse_delta(line)
                    if obj.get("__done__"):
                        continue
                    if "model" in obj and result.model is None:
                        result.model = obj.get("model")
                    u = obj.get("usage")
                    if u:
                        result.prompt_tokens = u.get("prompt_tokens", result.prompt_tokens)
                        result.completion_tokens = u.get("completion_tokens", result.completion_tokens)
                        result.total_tokens = u.get("total_tokens", result.total_tokens)
                    choices = obj.get("choices") or []
                    if choices:
                        delta = choices[0].get("delta") or {}
                        c = delta.get("content")
                        if c:
                            result.accumulated_content += c
                        rc = delta.get("reasoning_content")
                        if rc:
                            result.accumulated_reasoning += rc
        except Exception as e:
            result.error = f"stream error: {e}"
            logger.warning("流式传输异常: %s", e)
        finally:
            result.response_body = result.accumulated_content
            result.reasoning = result.accumulated_reasoning
            await resp.aclose()
            await client.aclose()

    return resp, gen(), result
