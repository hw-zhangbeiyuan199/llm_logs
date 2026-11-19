"""httpx 转发：阻塞 + SSE 流式。

- 阻塞：整体转发，返回完整响应
- 流式：逐 chunk 透传给客户端，同时后台累积 delta 文本 + usage，结束后汇总
- 上游 4xx/5xx 原样透传状态码与 body
"""

from __future__ import annotations

import codecs
import json
import logging
import time
from dataclasses import dataclass, field
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
    cached_tokens: Optional[int] = None
    reasoning_tokens: Optional[int] = None
    # 首 Token 时延（毫秒）：流式首个 chunk 收到时刻相对请求开始的时延；阻塞为 None
    ttft_ms: Optional[int] = None
    # 流式专用
    accumulated_content: str = ""
    accumulated_reasoning: str = ""
    client_disconnected: bool = False
    # 推理模型：思维链（阻塞模式从 message.reasoning_content 取）
    reasoning: str = ""
    # 流式聚合的响应元信息与工具调用
    response_id: Optional[str] = None
    created: Optional[int] = None
    role: Optional[str] = None
    finish_reason: Optional[str] = None
    tool_calls: list = field(default_factory=list)
    # 上游原始 usage 对象（原样回填进重构响应，保留缓存/推理等全部字段）
    raw_usage: Optional[dict] = None


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


def _parse_usage(u: dict | None) -> dict:
    """从 usage 提取 tokens，兼容 OpenAI/DeepSeek 的 cache 与 reasoning 字段结构。

    - cached_tokens: 优先 usage.prompt_tokens_details.cached_tokens（OpenAI 嵌套），
      回退 usage.prompt_cache_hit_tokens（DeepSeek 扁平）
    - reasoning_tokens: usage.completion_tokens_details.reasoning_tokens
    """
    if not u:
        return {}
    pd = u.get("prompt_tokens_details") or {}
    if "cached_tokens" in pd:
        cached = pd.get("cached_tokens") or 0
    elif "prompt_cache_hit_tokens" in u:
        cached = u.get("prompt_cache_hit_tokens") or 0
    else:
        cached = 0
    cd = u.get("completion_tokens_details") or {}
    reasoning = cd.get("reasoning_tokens") or 0
    return {
        "prompt_tokens": u.get("prompt_tokens", 0),
        "completion_tokens": u.get("completion_tokens", 0),
        "total_tokens": u.get("total_tokens", 0),
        "cached_tokens": cached,
        "reasoning_tokens": reasoning,
    }


async def forward_blocking(
    route: Route,
    path: str,
    body: dict,
    incoming_headers: dict,
    timeout: float,
) -> tuple[httpx.Response, ForwardResult]:
    """阻塞转发。返回 (原始 Response, 解析结果)。"""
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
                u = _parse_usage(j.get("usage"))
                result.prompt_tokens = u.get("prompt_tokens")
                result.completion_tokens = u.get("completion_tokens")
                result.total_tokens = u.get("total_tokens")
                result.cached_tokens = u.get("cached_tokens")
                result.reasoning_tokens = u.get("reasoning_tokens")
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


def _merge_tool_call(acc: dict, tc: dict) -> None:
    """按 index 聚合一条 tool_call 分片：id/type/name 记录一次，arguments 顺序拼接。"""
    idx = tc.get("index")
    if idx is None:
        idx = len(acc)
    entry = acc.get(idx)
    if entry is None:
        entry = {}
        acc[idx] = entry
    if tc.get("id"):
        entry["id"] = tc["id"]
    if tc.get("type"):
        entry["type"] = tc["type"]
    fn = tc.get("function") or {}
    f = entry.setdefault("function", {})
    if fn.get("name"):
        f["name"] = fn["name"]
    if fn.get("arguments"):
        f["arguments"] = f.get("arguments", "") + fn["arguments"]


def _build_stream_response(result: "ForwardResult") -> str:
    """流结束时将聚合到的全部返回信息重构为与阻塞模式同构的完整响应 JSON。

    只包含实际出现的字段；流式聚合任何信息都没拿到时返回空串。
    """
    if not (
        result.accumulated_content
        or result.accumulated_reasoning
        or result.tool_calls
        or result.model
        or result.finish_reason
        or result.raw_usage
        or result.prompt_tokens is not None
    ):
        return ""
    message: dict = {"role": result.role or "assistant"}
    if result.accumulated_content:
        message["content"] = result.accumulated_content
    if result.accumulated_reasoning:
        message["reasoning_content"] = result.accumulated_reasoning
    if result.tool_calls:
        message["tool_calls"] = result.tool_calls
    out: dict = {
        "choices": [
            {
                "index": 0,
                "finish_reason": result.finish_reason,
                "message": message,
            }
        ],
    }
    if result.response_id:
        out["id"] = result.response_id
    if result.created is not None:
        out["created"] = result.created
    if result.model:
        out["model"] = result.model
    if result.raw_usage:
        out["usage"] = result.raw_usage
    elif (
        result.prompt_tokens is not None
        or result.completion_tokens is not None
        or result.total_tokens is not None
    ):
        out["usage"] = {
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
            "total_tokens": result.total_tokens,
        }
    return json.dumps(out, ensure_ascii=False)


async def forward_stream(
    route: Route,
    path: str,
    body: dict,
    incoming_headers: dict,
    timeout: float,
    start: float = 0.0,
) -> tuple[httpx.Response, AsyncIterator[bytes], ForwardResult]:
    """流式转发。返回 (原始 Response, 字节迭代器, 结果对象)。

    调用方负责消费迭代器；迭代结束后 result.accumulated_content / usage 已填充。
    """
    url = _upstream_url(route, path)
    # 透传 agent 头（含 Authorization），保留 agent 的 Accept（流式时为 text/event-stream）
    headers = _passthrough_headers(incoming_headers)

    result = ForwardResult()

    client = httpx.AsyncClient(timeout=timeout)
    req = client.build_request("POST", url, json=body, headers=headers)
    resp = await client.send(req, stream=True)
    result.status_code = resp.status_code

    async def gen() -> AsyncIterator[bytes]:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        buf = ""
        tool_call_acc: dict = {}

        def process_line(line: str) -> None:
            line = line.rstrip("\r")
            if not line:
                return
            obj = _parse_sse_delta(line)
            if obj.get("__done__"):
                return
            if "model" in obj and result.model is None:
                result.model = obj.get("model")
            if result.response_id is None and obj.get("id"):
                result.response_id = obj.get("id")
            if result.created is None and obj.get("created") is not None:
                result.created = obj.get("created")
            u = obj.get("usage")
            if u:
                result.raw_usage = u
                pu = _parse_usage(u)
                result.prompt_tokens = pu["prompt_tokens"]
                result.completion_tokens = pu["completion_tokens"]
                result.total_tokens = pu["total_tokens"]
                result.cached_tokens = pu["cached_tokens"]
                result.reasoning_tokens = pu["reasoning_tokens"]
            choices = obj.get("choices") or []
            if choices:
                ch = choices[0] or {}
                fr = ch.get("finish_reason")
                if fr:
                    result.finish_reason = fr
                delta = ch.get("delta") or {}
                if delta.get("role"):
                    result.role = delta.get("role")
                c = delta.get("content")
                if c:
                    result.accumulated_content += c
                rc = delta.get("reasoning_content")
                if rc:
                    result.accumulated_reasoning += rc
                tcs = delta.get("tool_calls")
                if tcs:
                    for tc in tcs:
                        if isinstance(tc, dict):
                            _merge_tool_call(tool_call_acc, tc)

        try:
            async for chunk in resp.aiter_bytes():
                # 首 Token 时延：首个 chunk 收到时刻
                if result.ttft_ms is None and start:
                    result.ttft_ms = int((time.time() - start) * 1000)
                # 透传
                yield chunk
                # 累积解析（按行，增量解码避免多字节字符被 chunk 边界截断）
                buf += decoder.decode(chunk)
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    process_line(line)
            # 处理末尾不带换行的残留行
            if buf:
                process_line(buf)
                buf = ""
        except Exception as e:
            result.error = f"stream error: {e}"
            logger.warning("流式传输异常: %s", e)
        finally:
            if tool_call_acc:
                result.tool_calls = [tool_call_acc[k] for k in sorted(tool_call_acc)]
            result.response_body = _build_stream_response(result)
            result.reasoning = result.accumulated_reasoning
            await resp.aclose()
            await client.aclose()

    return resp, gen(), result
