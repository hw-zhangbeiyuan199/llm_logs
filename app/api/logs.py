"""日志查询/详情/统计 API（供前台调用）。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from app.db import get_log, get_stats, query_logs

router = APIRouter(prefix="/api")


@router.get("/logs")
async def list_logs(
    keyword: str | None = None,
    source_ip: str | None = None,
    model: str | None = None,
    status: str | None = Query(None, pattern="^(success|error|all)$"),
    is_stream: bool | None = None,
    route_name: str | None = None,
    start: str | None = None,
    end: str | None = None,
    page: int = 1,
    page_size: int = 20,
):
    return query_logs(
        keyword=keyword, source_ip=source_ip, model=model, status=status,
        is_stream=is_stream, route_name=route_name, start=start, end=end,
        page=page, page_size=min(page_size, 200),
    )


@router.get("/logs/{log_id}")
async def log_detail(log_id: int):
    item = get_log(log_id)
    if not item:
        return {"error": "not found"}
    return item


@router.get("/stats")
async def stats(hours: int = 24):
    return get_stats(hours=hours)
