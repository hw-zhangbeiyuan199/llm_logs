"""转发端点：/v1/chat/completions, /v1/completions。

支持阻塞与流式（stream: true）。
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.config import get_config_manager
from app.core.forwarder import forward_blocking, forward_stream
from app.core.pipeline import RequestContext, get_pipeline
from app.core.router import resolve_route
from app.db import insert_log

logger = logging.getLogger("llm_gateway.proxy")
router = APIRouter()


def _client_ip(request: Request) -> str:
    cfg = get_config_manager().current
    if cfg.trust_forwarded_for:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _truncate(text: str, max_kb: int) -> str:
    if not text:
        return text
    limit = max_kb * 1024
    if len(text.encode("utf-8")) > limit:
        return text[:limit] + "...[truncated]"
    return text


async def _handle(request: Request, path: str) -> Any:
    cfg = get_config_manager().current
    start = time.time()
    source_ip = _client_ip(request)
    route = resolve_route(source_ip, cfg)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    is_stream = bool(body.get("stream", False))
    user_agent = request.headers.get("user-agent", "")

    ctx = RequestContext(
        source_ip=source_ip,
        route_name=route.name,
        method=request.method,
        path=path,
        model=body.get("model"),
        is_stream=is_stream,
        request_body=body,
    )
    pipeline = get_pipeline()
    await pipeline.on_request(ctx, cfg)
    if ctx.aborted:
        # 限流等中止也记录一条日志
        latency = int((time.time() - start) * 1000)
        insert_log({
            "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
            "route_name": route.name, "upstream_url": route.upstream.base_url,
            "method": request.method, "path": path, "model": body.get("model"),
            "status_code": ctx.abort_status, "latency_ms": latency, "is_stream": is_stream,
            "request_body": _truncate(json.dumps(body, ensure_ascii=False), cfg.logging.max_body_kb),
            "response_body": ctx.abort_message, "error": ctx.abort_message,
            "user_agent": user_agent,
        })
        return JSONResponse({"error": ctx.abort_message}, status_code=ctx.abort_status)

    incoming_headers = dict(request.headers)

    if is_stream:
        return await _do_stream(request, ctx, cfg, route, path, body, incoming_headers, source_ip, start, user_agent)
    else:
        return await _do_blocking(ctx, cfg, route, path, body, incoming_headers, source_ip, start, user_agent)


async def _do_blocking(ctx, cfg, route, path, body, incoming_headers, source_ip, start, user_agent) -> Any:
    pipeline = get_pipeline()
    try:
        resp, result = await forward_blocking(route, path, body, incoming_headers, float(cfg.upstream_timeout))
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        await pipeline.on_error(ctx, cfg, e)
        insert_log({
            "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
            "route_name": route.name, "upstream_url": route.upstream.base_url,
            "method": "POST", "path": path, "model": body.get("model"),
            "status_code": 502, "latency_ms": latency, "is_stream": False,
            "request_body": _truncate(json.dumps(body, ensure_ascii=False), cfg.logging.max_body_kb),
            "error": str(e), "user_agent": user_agent,
        })
        return JSONResponse({"error": "upstream error", "detail": str(e)}, status_code=502)

    await pipeline.on_response(ctx, cfg)
    latency = int((time.time() - start) * 1000)
    insert_log({
        "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
        "route_name": route.name, "upstream_url": route.upstream.base_url,
        "method": "POST", "path": path, "model": result.model or body.get("model"),
        "status_code": result.status_code, "latency_ms": latency, "is_stream": False,
        "prompt_tokens": result.prompt_tokens, "completion_tokens": result.completion_tokens,
        "total_tokens": result.total_tokens,
        "request_body": _truncate(json.dumps(ctx.request_body, ensure_ascii=False), cfg.logging.max_body_kb),
        "response_body": _truncate(result.response_body, cfg.logging.max_body_kb),
        "reasoning": _truncate(result.reasoning, cfg.logging.max_body_kb),
        "error": result.error or (None if result.status_code < 400 else result.response_body[:500]),
        "user_agent": user_agent,
    })

    # 原样透传响应
    headers = {"content-type": resp.headers.get("content-type", "application/json")}
    return JSONResponse(content=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text,
                        status_code=resp.status_code, headers=headers)


async def _do_stream(request: Request, ctx, cfg, route, path, body, incoming_headers, source_ip, start, user_agent) -> Any:
    pipeline = get_pipeline()
    try:
        resp, gen, result = await forward_stream(route, path, body, incoming_headers, float(cfg.upstream_timeout))
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        await pipeline.on_error(ctx, cfg, e)
        insert_log({
            "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
            "route_name": route.name, "upstream_url": route.upstream.base_url,
            "method": "POST", "path": path, "model": body.get("model"),
            "status_code": 502, "latency_ms": latency, "is_stream": True,
            "request_body": _truncate(json.dumps(body, ensure_ascii=False), cfg.logging.max_body_kb),
            "error": str(e), "user_agent": user_agent,
        })
        return JSONResponse({"error": "upstream error", "detail": str(e)}, status_code=502)

    if resp.status_code >= 400:
        # 上游错误：读取完整 body 后透传
        err_body = await resp.aread()
        await resp.aclose()
        latency = int((time.time() - start) * 1000)
        insert_log({
            "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
            "route_name": route.name, "upstream_url": route.upstream.base_url,
            "method": "POST", "path": path, "model": body.get("model"),
            "status_code": resp.status_code, "latency_ms": latency, "is_stream": True,
            "request_body": _truncate(json.dumps(body, ensure_ascii=False), cfg.logging.max_body_kb),
            "response_body": _truncate(err_body.decode("utf-8", "replace"), cfg.logging.max_body_kb),
            "error": err_body.decode("utf-8", "replace")[:500],
            "user_agent": user_agent,
        })
        return JSONResponse({"error": "upstream error", "detail": err_body.decode("utf-8", "replace")},
                            status_code=resp.status_code)

    async def stream_with_log():
        try:
            async for chunk in gen:
                yield chunk
        finally:
            await pipeline.on_response(ctx, cfg)
            latency = int((time.time() - start) * 1000)
            insert_log({
                "ts": datetime.utcnow().isoformat(), "source_ip": source_ip,
                "route_name": route.name, "upstream_url": route.upstream.base_url,
                "method": "POST", "path": path, "model": result.model or body.get("model"),
                "status_code": result.status_code or 200, "latency_ms": latency, "is_stream": True,
                "prompt_tokens": result.prompt_tokens, "completion_tokens": result.completion_tokens,
                "total_tokens": result.total_tokens,
                "request_body": _truncate(json.dumps(ctx.request_body, ensure_ascii=False), cfg.logging.max_body_kb),
                "response_body": _truncate(result.accumulated_content or result.response_body, cfg.logging.max_body_kb),
                "reasoning": _truncate(result.reasoning, cfg.logging.max_body_kb),
                "error": result.error or None,
                "client_disconnected": result.client_disconnected,
                "user_agent": user_agent,
            })

    return StreamingResponse(
        stream_with_log(),
        status_code=resp.status_code,
        media_type="text/event-stream",
        headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
    )


@router.post("/chat/completions")
@router.post("/v1/chat/completions")
async def chat_completions(request: Request) -> Any:
    return await _handle(request, "/chat/completions")


@router.post("/completions")
@router.post("/v1/completions")
async def completions(request: Request) -> Any:
    return await _handle(request, "/completions")


@router.get("/models")
@router.get("/v1/models")
async def list_models(request: Request) -> Any:
    """简单透传：转发到上游 /models，原样透传 agent 的 Authorization。"""
    cfg = get_config_manager().current
    source_ip = _client_ip(request)
    route = resolve_route(source_ip, cfg)
    url = route.upstream.base_url.rstrip("/") + "/models"
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length", "connection")}
    import httpx
    async with httpx.AsyncClient(timeout=float(cfg.upstream_timeout)) as client:
        try:
            resp = await client.get(url, headers=headers)
            return JSONResponse(content=resp.json(), status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": "upstream error", "detail": str(e)}, status_code=502)
