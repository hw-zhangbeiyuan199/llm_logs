"""配置加载 + 热重载。

- 配置文件缺失时自动生成模板 config.yaml
- 后台轮询 mtime，变更后校验并原子替换内存配置
- 校验失败保留旧配置并记录告警
- 可热改：路由、超时、限流、保留、日志参数、XFF 信任等
- 需重启：server.host/port、logging.db_path（启动时读取一次）
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger("llm_gateway.config")

CONFIG_PATH = Path("config.yaml")
DEFAULT_CONFIG_PATH = CONFIG_PATH  # 同一文件

_TEMPLATE = """\
server:
  host: "0.0.0.0"
  port: 8000

# 是否信任 X-Forwarded-For 取真实客户端 IP（部署在反代后时设为 true）
trust_forwarded_for: false

# 上游请求超时（秒），流式建议设大
upstream_timeout: 300

# 日志
logging:
  db_path: "data/llm_gateway.db"
  max_body_kb: 512        # 单条请求/响应体存储上限，超出截断
  mask_keys: true         # 日志中对疑似 api_key 字段脱敏

# 限流（默认关闭）
rate_limit:
  enabled: false
  requests_per_minute: 60

# 日志保留（默认关闭）
retention:
  enabled: false
  days: 30

# 路由规则：按顺序匹配，首条命中生效；未命中任何 source_ips 走 default
# base_url 约定：网关在其后拼接 /chat/completions
#   - OpenAI: "https://api.openai.com/v1"
#   - DeepSeek: "https://api.deepseek.com"
#   - Ollama: "http://localhost:11434/v1"
# 网关只改指向，不替换 api_key：agent 自带 Authorization 原样透传给上游
routes:
  - name: office
    source_ips:
      - "192.168.1.0/24"
      - "10.0.0.5"
    upstream:
      base_url: "https://api.openai.com/v1"
  - name: default
    source_ips:
      - "0.0.0.0/0"
    upstream:
      base_url: "https://api.deepseek.com"
"""


@dataclass
class Upstream:
    base_url: str


@dataclass
class Route:
    name: str
    source_ips: list[str]
    # 预编译：精确 IP 与 CIDR 分离，提升匹配效率
    exact_ips: set[str] = field(default_factory=set)
    cidrs: list[ipaddress._BaseNetwork] = field(default_factory=list)
    upstream: Upstream = field(default_factory=lambda: Upstream(base_url=""))

    def matches(self, ip: str) -> bool:
        if ip in self.exact_ips:
            return True
        for net in self.cidrs:
            try:
                if ipaddress.ip_address(ip) in net:
                    return True
            except ValueError:
                continue
        return False


@dataclass
class ServerCfg:
    host: str = "0.0.0.0"
    port: int = 8000


@dataclass
class LoggingCfg:
    db_path: str = "data/llm_gateway.db"
    max_body_kb: int = 512
    mask_keys: bool = True


@dataclass
class RateLimitCfg:
    enabled: bool = False
    requests_per_minute: int = 60


@dataclass
class RetentionCfg:
    enabled: bool = False
    days: int = 30


@dataclass
class Config:
    server: ServerCfg = field(default_factory=ServerCfg)
    trust_forwarded_for: bool = False
    upstream_timeout: int = 300
    logging: LoggingCfg = field(default_factory=LoggingCfg)
    rate_limit: RateLimitCfg = field(default_factory=RateLimitCfg)
    retention: RetentionCfg = field(default_factory=RetentionCfg)
    routes: list[Route] = field(default_factory=list)
    # 加载来源信息
    config_path: str = str(CONFIG_PATH)
    last_loaded_at: float = 0.0
    last_error: Optional[str] = None


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _resolve_env(value: str) -> str:
    """解析字符串中的 ${VAR} 占位，从环境变量取值；未设置则保留原占位。"""
    if not isinstance(value, str) or "${" not in value:
        return value
    return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), m.group(0)), value)


def _build_route(raw: dict) -> Route:
    name = raw.get("name", "unnamed")
    srcs = raw.get("source_ips") or []
    up_raw = raw.get("upstream") or {}
    base_url = _resolve_env(str(up_raw.get("base_url", ""))).rstrip("/")
    upstream = Upstream(
        base_url=base_url,
    )
    exact: set[str] = set()
    cidrs: list[ipaddress._BaseNetwork] = []
    for s in srcs:
        s = str(s).strip()
        if not s:
            continue
        try:
            net = ipaddress.ip_network(s, strict=False)
            if net.prefixlen == net.max_prefixlen and net.network_address.version == net.network_address.version:
                # /32 (IPv4) 或 /128 (IPv6) 视为精确 IP
                exact.add(str(net.network_address))
            else:
                cidrs.append(net)
        except ValueError:
            # 不是 CIDR，尝试当作精确 IP
            try:
                ipaddress.ip_address(s)
                exact.add(s)
            except ValueError:
                logger.warning("路由 %s 中非法 IP/CIDR 已忽略: %s", name, s)
    return Route(name=name, source_ips=srcs, exact_ips=exact, cidrs=cidrs, upstream=upstream)


def _parse_config(raw: dict) -> Config:
    cfg = Config()
    sraw = raw.get("server") or {}
    cfg.server = ServerCfg(
        host=str(sraw.get("host", "0.0.0.0")),
        port=int(sraw.get("port", 8000)),
    )
    cfg.trust_forwarded_for = bool(raw.get("trust_forwarded_for", False))
    cfg.upstream_timeout = int(raw.get("upstream_timeout", 300))
    lraw = raw.get("logging") or {}
    cfg.logging = LoggingCfg(
        db_path=str(lraw.get("db_path", "data/llm_gateway.db")),
        max_body_kb=int(lraw.get("max_body_kb", 512)),
        mask_keys=bool(lraw.get("mask_keys", True)),
    )
    rraw = raw.get("rate_limit") or {}
    cfg.rate_limit = RateLimitCfg(
        enabled=bool(rraw.get("enabled", False)),
        requests_per_minute=int(rraw.get("requests_per_minute", 60)),
    )
    retraw = raw.get("retention") or {}
    cfg.retention = RetentionCfg(
        enabled=bool(retraw.get("enabled", False)),
        days=int(retraw.get("days", 30)),
    )
    routes_raw = raw.get("routes") or []
    if not routes_raw:
        # 兜底默认路由
        routes_raw = [{"name": "default", "source_ips": ["0.0.0.0/0"],
                       "upstream": {"base_url": "https://api.deepseek.com"}}]
    cfg.routes = [_build_route(r) for r in routes_raw]
    return cfg


def _validate(cfg: Config) -> Optional[str]:
    """返回错误信息字符串；None 表示通过。"""
    for r in cfg.routes:
        if not r.upstream.base_url:
            return f"路由 '{r.name}' 缺少 upstream.base_url"
    return None


def _ensure_template() -> None:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(_TEMPLATE, encoding="utf-8")
        logger.info("未发现 config.yaml，已生成模板于 %s，请按需修改", CONFIG_PATH)


class ConfigManager:
    """单例。current 为当前生效配置（原子替换）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: Optional[Config] = None
        self._mtime: float = 0.0
        self._task: Optional[asyncio.Task] = None

    # ---- 加载 ----
    def load(self) -> Config:
        _ensure_template()
        try:
            raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
            cfg = _parse_config(raw)
            err = _validate(cfg)
            if err:
                logger.error("配置校验失败，保留旧配置: %s", err)
                if self._current is None:
                    # 首次加载即非法，给个空配置避免崩溃
                    self._current = cfg
                    cfg.last_error = err
                else:
                    self._current.last_error = err
                return self._current
            cfg.config_path = str(CONFIG_PATH)
            cfg.last_loaded_at = os.path.getmtime(CONFIG_PATH)
            cfg.last_error = None
            with self._lock:
                self._current = cfg
                self._mtime = cfg.last_loaded_at
            logger.info("配置已加载: %d 条路由, db=%s", len(cfg.routes), cfg.logging.db_path)
            return cfg
        except Exception as e:
            logger.exception("配置加载异常: %s", e)
            if self._current is None:
                # 首次加载失败，生成空配置兜底
                cfg = Config()
                cfg.last_error = str(e)
                self._current = cfg
            else:
                self._current.last_error = str(e)
            return self._current

    def reload(self) -> tuple[bool, str]:
        """手动触发重载，返回 (是否成功, 消息)。"""
        before = id(self._current)
        self.load()
        ok = self._current is not None and self._current.last_error is None
        changed = id(self._current) != before or self._current is not None
        msg = "已重载" if ok else f"重载失败: {self._current.last_error if self._current else '未知'}"
        return ok, msg

    @property
    def current(self) -> Config:
        if self._current is None:
            self.load()
        assert self._current is not None
        return self._current

    # ---- 热重载轮询 ----
    async def _watch(self) -> None:
        while True:
            try:
                if CONFIG_PATH.exists():
                    m = os.path.getmtime(CONFIG_PATH)
                    if m != self._mtime:
                        logger.info("检测到 config.yaml 变更，热重载中...")
                        self.load()
            except Exception as e:
                logger.warning("热重载轮询异常: %s", e)
            await asyncio.sleep(2)

    def start_watcher(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if self._task is not None:
            return
        loop = loop or asyncio.get_event_loop()
        self._task = loop.create_task(self._watch())
        logger.info("配置热重载监听已启动（每 2 秒轮询）")

    async def stop_watcher(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ---- 生效配置视图（给 /admin/config）----
    def masked_view(self) -> dict:
        cfg = self.current
        return {
            "server": {"host": cfg.server.host, "port": cfg.server.port},
            "trust_forwarded_for": cfg.trust_forwarded_for,
            "upstream_timeout": cfg.upstream_timeout,
            "logging": {
                "db_path": cfg.logging.db_path,
                "max_body_kb": cfg.logging.max_body_kb,
                "mask_keys": cfg.logging.mask_keys,
            },
            "rate_limit": {"enabled": cfg.rate_limit.enabled,
                           "requests_per_minute": cfg.rate_limit.requests_per_minute},
            "retention": {"enabled": cfg.retention.enabled, "days": cfg.retention.days},
            "routes": [
                {"name": r.name, "source_ips": r.source_ips,
                 "upstream": {"base_url": r.upstream.base_url}}
                for r in cfg.routes
            ],
            "last_loaded_at": cfg.last_loaded_at,
            "last_error": cfg.last_error,
        }


_singleton: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    global _singleton
    if _singleton is None:
        _singleton = ConfigManager()
    return _singleton
