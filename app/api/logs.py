"""日志查询/详情/统计/导出 API（供前台调用）。"""
from __future__ import annotations

import io
from datetime import datetime

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.db import export_logs, get_log, get_stats, query_logs

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


@router.get("/logs/export")
async def export_logs_excel(limit: int = Query(100, ge=1, le=10000)):
    """导出最新 N 条记录为 Excel(.xlsx)。"""
    rows = export_logs(limit=limit)
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "请求日志"
    headers = ["ID", "时间", "来源IP", "路由", "session_id", "parent_session_id",
               "上游", "方法", "路径", "模型", "状态码", "耗时ms", "首Token ms",
               "模式", "输入tokens", "输出tokens", "总计tokens", "缓存命中", "推理tokens",
               "请求体", "响应", "推理思维链", "错误", "User-Agent"]
    ws.append(headers)
    for r in rows:
        ws.append([
            r.get("id"), r.get("ts"), r.get("source_ip"), r.get("route_name"),
            r.get("session_id"), r.get("parent_session_id"),
            r.get("upstream_url"), r.get("method"), r.get("path"), r.get("model"),
            r.get("status_code"), r.get("latency_ms"), r.get("ttft_ms"),
            "流式" if r.get("is_stream") else "阻塞",
            r.get("prompt_tokens"), r.get("completion_tokens"), r.get("total_tokens"),
            r.get("cached_tokens"), r.get("reasoning_tokens"),
            r.get("request_body"), r.get("response_body"), r.get("reasoning"),
            r.get("error"), r.get("user_agent"),
        ])
    # 简单列宽
    for i, _ in enumerate(headers, 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = 18

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fname = f"llm_gateway_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
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
