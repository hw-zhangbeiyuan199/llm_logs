"""路由解析：根据源 IP 匹配路由规则。"""
from __future__ import annotations

from app.config import Config, Route


def resolve_route(source_ip: str, cfg: Config) -> Route:
    """首条命中返回；未命中返回最后一条（约定最后一条为 default 兜底）。"""
    for r in cfg.routes:
        if r.matches(source_ip):
            return r
    if cfg.routes:
        return cfg.routes[-1]
    raise RuntimeError("无可用的路由配置")
