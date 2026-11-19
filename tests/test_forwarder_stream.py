"""流式聚合单元测试：mock 上游，不依赖外部服务。

覆盖：
- tool_calls 流式响应聚合（修复：纯工具调用响应 response_body 为空）
- content / reasoning_content 同时存在
- UTF-8 多字节字符跨 chunk 边界
- 末尾事件不带换行符
- 透传字节与上游原始字节一致
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

import app.core.forwarder as forwarder
from app.config import Route, Upstream


def _route() -> Route:
    return Route(
        name="mock",
        source_ips=["127.0.0.1"],
        upstream=Upstream(base_url="http://mock-upstream/v1"),
    )


class _FakeUpstreamClient:
    resp: httpx.Response | None = None

    def __init__(self, *args, **kwargs):
        pass

    def build_request(self, *args, **kwargs):
        return httpx.Request("POST", "http://mock-upstream/v1/chat/completions")

    async def send(self, req, stream=False):
        assert _FakeUpstreamClient.resp is not None
        return _FakeUpstreamClient.resp

    async def aclose(self):
        pass


def _patch_upstream(monkeypatch: pytest.MonkeyPatch, chunks: list[bytes]) -> None:
    async def byte_iter():
        for c in chunks:
            yield c

    _FakeUpstreamClient.resp = httpx.Response(200, content=byte_iter())
    monkeypatch.setattr(forwarder.httpx, "AsyncClient", _FakeUpstreamClient)


def _consume(body: dict) -> tuple[str, forwarder.ForwardResult]:
    async def run():
        resp, gen, result = await forwarder.forward_stream(
            _route(), "/chat/completions", body, {}, 5.0, time.time()
        )
        passthrough = []
        async for c in gen:
            passthrough.append(c)
        return b"".join(passthrough).decode(), result

    return asyncio.run(run())


def _sse(obj: dict) -> bytes:
    return ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8")


def test_stream_tool_calls_captured(monkeypatch):
    """纯 tool_calls 响应：修复前 response_body 为空，修复后为完整 JSON。"""
    chunks = [
        _sse(
            {
                "id": "resp-1",
                "created": 123,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_abc",
                                    "type": "function",
                                    "function": {"name": "read_file", "arguments": ""},
                                }
                            ],
                        },
                    }
                ],
            }
        ),
        _sse(
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {"index": 0, "function": {"arguments": '{"path"'}}
                            ]
                        },
                    }
                ]
            }
        ),
        _sse(
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {"index": 0, "function": {"arguments": ':"a.py"}'}}
                            ]
                        },
                    }
                ]
            }
        ),
        _sse({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
        _sse(
            {
                "choices": [],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                },
            }
        ),
        b"data: [DONE]\n\n",
    ]
    _patch_upstream(monkeypatch, chunks)

    passthrough, result = _consume({"model": "test-model", "messages": []})

    assert passthrough == b"".join(chunks).decode()
    assert result.error == ""
    body = json.loads(result.response_body)
    msg = body["choices"][0]["message"]
    tc = msg["tool_calls"][0]
    assert tc["id"] == "call_abc"
    assert tc["function"]["name"] == "read_file"
    assert tc["function"]["arguments"] == '{"path":"a.py"}'
    assert body["choices"][0]["finish_reason"] == "tool_calls"
    assert body["id"] == "resp-1"
    assert body["created"] == 123
    assert body["model"] == "test-model"
    assert body["usage"] == {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
    }
    assert result.prompt_tokens == 10 and result.total_tokens == 15


def test_stream_content_and_reasoning(monkeypatch):
    """content 与 reasoning_content 同时出现时都进入重构响应。"""
    chunks = [
        _sse(
            {
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "reasoning_content": "先思考"},
                    }
                ],
            }
        ),
        _sse({"choices": [{"index": 0, "delta": {"reasoning_content": "，再作答"}}]}),
        _sse({"choices": [{"index": 0, "delta": {"content": "答案是"}}]}),
        _sse({"choices": [{"index": 0, "delta": {"content": "42"}}]}),
        _sse({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
        b"data: [DONE]\n\n",
    ]
    _patch_upstream(monkeypatch, chunks)

    _, result = _consume({"model": "test-model", "messages": []})

    body = json.loads(result.response_body)
    msg = body["choices"][0]["message"]
    assert msg["content"] == "答案是42"
    assert msg["reasoning_content"] == "先思考，再作答"
    assert "tool_calls" not in msg
    assert result.reasoning == "先思考，再作答"


def test_stream_utf8_split_across_chunks(monkeypatch):
    """中文多字节字符被 chunk 边界切开时不产生乱码。"""
    payload = {"choices": [{"index": 0, "delta": {"content": "你好世界"}}]}
    raw = _sse(payload)
    chunks = [raw[i : i + 3] for i in range(0, len(raw), 3)]
    _patch_upstream(monkeypatch, chunks)

    _, result = _consume({"model": "m", "messages": []})

    body = json.loads(result.response_body)
    assert body["choices"][0]["message"]["content"] == "你好世界"


def test_stream_trailing_line_without_newline(monkeypatch):
    """末尾事件不以换行结束时仍被解析。"""
    chunks = [
        _sse({"model": "m", "choices": [{"index": 0, "delta": {"content": "A"}}]}),
        _sse({"choices": [{"index": 0, "delta": {"content": "B"}}]}),
        b'data: {"choices":[{"index":0,"delta":{"content":"C"}}]}',
    ]
    _patch_upstream(monkeypatch, chunks)

    _, result = _consume({"model": "m", "messages": []})

    body = json.loads(result.response_body)
    assert body["choices"][0]["message"]["content"] == "ABC"


def test_stream_usage_deepseek_cache_passthrough(monkeypatch):
    """DeepSeek 风格 usage（prompt_cache_hit_tokens）原样回填进重构响应。"""
    chunks = [
        _sse({"model": "m", "choices": [{"index": 0, "delta": {"content": "答"}}]}),
        _sse(
            {
                "choices": [],
                "usage": {
                    "prompt_tokens": 12313,
                    "completion_tokens": 5691,
                    "total_tokens": 18004,
                    "prompt_cache_hit_tokens": 4096,
                    "prompt_cache_miss_tokens": 8217,
                },
            }
        ),
        b"data: [DONE]\n\n",
    ]
    _patch_upstream(monkeypatch, chunks)

    _, result = _consume({"model": "m", "messages": []})

    body = json.loads(result.response_body)
    assert body["usage"] == {
        "prompt_tokens": 12313,
        "completion_tokens": 5691,
        "total_tokens": 18004,
        "prompt_cache_hit_tokens": 4096,
        "prompt_cache_miss_tokens": 8217,
    }
    assert result.cached_tokens == 4096


def test_stream_usage_openai_details_passthrough(monkeypatch):
    """OpenAI 风格嵌套 usage（prompt_tokens_details 等）原样回填进重构响应。"""
    usage = {
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_tokens": 150,
        "prompt_tokens_details": {"cached_tokens": 80},
        "completion_tokens_details": {"reasoning_tokens": 30},
    }
    chunks = [
        _sse({"model": "m", "choices": [{"index": 0, "delta": {"content": "答"}}]}),
        _sse({"choices": [], "usage": usage}),
        b"data: [DONE]\n\n",
    ]
    _patch_upstream(monkeypatch, chunks)

    _, result = _consume({"model": "m", "messages": []})

    body = json.loads(result.response_body)
    assert body["usage"] == usage
    assert result.cached_tokens == 80
    assert result.reasoning_tokens == 30
