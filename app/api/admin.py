"""管理端点：健康检查、生效配置（结构化）、配置文件读写、手动重载。"""
from __future__ import annotations

import yaml
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.config import CONFIG_PATH, get_config_manager
from app.db import cleanup_old

router = APIRouter(prefix="/admin")


@router.get("/health")
async def health():
    cfg = get_config_manager().current
    return {
        "status": "ok",
        "routes": len(cfg.routes),
        "config_error": cfg.last_error,
        "db_path": cfg.logging.db_path,
    }


@router.get("/config")
async def effective_config():
    """返回当前生效配置（结构化）。"""
    return get_config_manager().masked_view()


@router.get("/config/raw")
async def config_raw():
    """返回 config.yaml 原文，供前端编辑。"""
    try:
        text = CONFIG_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        text = ""
    return {"text": text, "path": str(CONFIG_PATH)}


@router.put("/config")
async def config_save(payload: dict = Body(...)):
    """保存 config.yaml：先做 YAML 语法校验，通过后写入并立即重载。

    语法非法时不写文件，直接返回错误，避免坏配置覆盖好文件。
    """
    text = payload.get("text", "")
    if not isinstance(text, str):
        return JSONResponse({"ok": False, "message": "text 必须是字符串"}, status_code=400)
    # 语法校验
    try:
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        return JSONResponse({"ok": False, "message": f"YAML 语法错误: {e}"}, status_code=400)
    # 写入
    try:
        CONFIG_PATH.write_text(text, encoding="utf-8")
    except Exception as e:
        return JSONResponse({"ok": False, "message": f"写入失败: {e}"}, status_code=500)
    # 立即重载
    ok, msg = get_config_manager().reload()
    return {"ok": ok, "message": "已保存并重载" if ok else f"已写入但重载失败: {msg}"}


@router.post("/reload")
async def reload_config():
    ok, msg = get_config_manager().reload()
    return {"ok": ok, "message": msg}


@router.post("/retention/cleanup")
async def retention_cleanup(days: int = 30):
    n = cleanup_old(days)
    return {"deleted": n}
