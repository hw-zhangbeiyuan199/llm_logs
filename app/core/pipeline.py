"""Hook 管道：可扩展点。

内置 Hook（配置驱动）：
- RateLimitHook：按 IP 限流（默认关闭）
- MaskHook：日志/响应中 api_key 脱敏（始终启用，依赖 logging.mask_keys）
- RetentionHook：定时清理旧日志（默认关闭）

扩展新功能：实现 Hook 接口，注册到 PipelineManager 即可。
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from app.config import Config, get_config_manager
from app.db import cleanup_old

logger = logging.getLogger("llm_gateway.pipeline")


@dataclass
class RequestContext:
    """贯穿一次转发的上下文。Hook 可读写。"""
    source_ip: str = ""
    route_name: str = ""
    method: str = "POST"
    path: str = ""
    model: Optional[str] = None
    is_stream: bool = False
    request_body: dict = field(default_factory=dict)
    # Hook 可设置以中止请求
    abort_status: int = 0
    abort_message: str = ""
    aborted: bool = False
    # 自由元数据
    meta: dict = field(default_factory=dict)


class Hook:
    """Hook 基类。可选实现任意方法。"""
    async def on_request(self, ctx: RequestContext, cfg: Config) -> None:
        ...

    async def on_response(self, ctx: RequestContext, cfg: Config) -> None:
        ...

    async def on_error(self, ctx: RequestContext, cfg: Config, exc: BaseException) -> None:
        ...


class RateLimitHook(Hook):
    """按 IP 滑动窗口限流。"""
    def __init__(self) -> None:
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock_guard = 0  # asyncio 单线程，无需锁

    async def on_request(self, ctx: RequestContext, cfg: Config) -> None:
        if not cfg.rate_limit.enabled:
            return
        now = time.time()
        window = 60.0
        limit = cfg.rate_limit.requests_per_minute
        dq = self._buckets[ctx.source_ip]
        while dq and now - dq[0] > window:
            dq.popleft()
        if len(dq) >= limit:
            ctx.abort_status = 429
            ctx.abort_message = "rate limit exceeded"
            ctx.aborted = True
            return
        dq.append(now)


class MaskHook(Hook):
    """对 request_body 中疑似 api_key 字段脱敏（写入日志前）。

    简单实现：遍历常见字段名。
    """
    _SENSITIVE = {"api_key", "apikey", "key", "authorization", "secret"}

    async def on_response(self, ctx: RequestContext, cfg: Config) -> None:
        if not cfg.logging.mask_keys:
            return
        self._mask(ctx.request_body)

    def _mask(self, obj) -> None:
        if isinstance(obj, dict):
            for k in list(obj.keys()):
                if k.lower() in self._SENSITIVE and isinstance(obj[k], str) and obj[k]:
                    obj[k] = obj[k][:4] + "***" + obj[k][-4:] if len(obj[k]) > 8 else "***"
                else:
                    self._mask(obj[k])
        elif isinstance(obj, list):
            for item in obj:
                self._mask(item)


class RetentionHook(Hook):
    """定期清理（由后台任务调用）。"""
    def __init__(self) -> None:
        self._last_run = 0.0

    async def on_response(self, ctx: RequestContext, cfg: Config) -> None:
        if not cfg.retention.enabled:
            return
        now = time.time()
        if now - self._last_run > 3600:  # 每小时最多跑一次
            self._last_run = now
            try:
                n = cleanup_old(cfg.retention.days)
                if n:
                    logger.info("日志保留清理: 删除 %d 条超过 %d 天的记录", n, cfg.retention.days)
            except Exception as e:
                logger.warning("日志保留清理失败: %s", e)


class PipelineManager:
    """管理 Hook 注册与执行。"""
    def __init__(self) -> None:
        self.hooks: list[Hook] = []

    def register(self, hook: Hook) -> None:
        self.hooks.append(hook)

    async def on_request(self, ctx: RequestContext, cfg: Config) -> None:
        for h in self.hooks:
            try:
                await h.on_request(ctx, cfg)
                if ctx.aborted:
                    return
            except Exception as e:
                logger.warning("Hook on_request 异常 (%s): %s", type(h).__name__, e)

    async def on_response(self, ctx: RequestContext, cfg: Config) -> None:
        for h in self.hooks:
            try:
                await h.on_response(ctx, cfg)
            except Exception as e:
                logger.warning("Hook on_response 异常 (%s): %s", type(h).__name__, e)

    async def on_error(self, ctx: RequestContext, cfg: Config, exc: BaseException) -> None:
        for h in self.hooks:
            try:
                await h.on_error(ctx, cfg, exc)
            except Exception as e:
                logger.warning("Hook on_error 异常 (%s): %s", type(h).__name__, e)


_pipeline: Optional[PipelineManager] = None


def get_pipeline() -> PipelineManager:
    global _pipeline
    if _pipeline is None:
        pm = PipelineManager()
        pm.register(RateLimitHook())
        pm.register(MaskHook())
        pm.register(RetentionHook())
        _pipeline = pm
    return _pipeline
