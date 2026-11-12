"""FastAPI 入口：组装路由、静态托管、启动任务。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api import admin, logs, proxy
from app.config import get_config_manager
from app.db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("llm_gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg_mgr = get_config_manager()
    cfg = cfg_mgr.load()
    init_db(cfg.logging.db_path)
    cfg_mgr.start_watcher()
    logger.info("LLM Gateway 启动完成 -> http://%s:%d", cfg.server.host, cfg.server.port)
    logger.info("前台面板: http://%s:%d/  |  转发端点: /v1/chat/completions", cfg.server.host, cfg.server.port)
    yield
    await cfg_mgr.stop_watcher()
    logger.info("LLM Gateway 已停止")


app = FastAPI(title="LLM Gateway", version="1.0.0", lifespan=lifespan)
app.include_router(proxy.router)
app.include_router(logs.router)
app.include_router(admin.router)

# 静态资源
_STATIC = Path(__file__).parent / "web" / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


@app.get("/")
async def index():
    idx = _STATIC / "index.html"
    if idx.exists():
        return FileResponse(str(idx))
    return {"message": "LLM Gateway running. Frontend not found."}


@app.get("/health")
async def health_root():
    """根路径健康检查（/admin/health 的别名）。"""
    from app.config import get_config_manager
    cfg = get_config_manager().current
    return {"status": "ok", "routes": len(cfg.routes), "config_error": cfg.last_error}
