"""SQLite 日志存储（WAL 模式）。

每次操作打开独立连接（WAL 下并发安全），避免跨线程/async 问题。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("llm_gateway.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS request_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,                  -- ISO8601
    source_ip TEXT,
    route_name TEXT,
    session_id TEXT,
    parent_session_id TEXT,
    upstream_url TEXT,
    method TEXT,
    path TEXT,
    model TEXT,
    status_code INTEGER,
    latency_ms INTEGER,
    ttft_ms INTEGER,
    is_stream INTEGER DEFAULT 0,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    cached_tokens INTEGER,
    reasoning_tokens INTEGER,
    request_body TEXT,
    response_body TEXT,
    reasoning TEXT,
    error TEXT,
    client_disconnected INTEGER DEFAULT 0,
    user_agent TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON request_logs(ts);
CREATE INDEX IF NOT EXISTS idx_logs_source_ip ON request_logs(source_ip);
CREATE INDEX IF NOT EXISTS idx_logs_model ON request_logs(model);
CREATE INDEX IF NOT EXISTS idx_logs_status ON request_logs(status_code);
"""

_db_path: str = "data/llm_gateway.db"


def init_db(path: str) -> None:
    global _db_path
    _db_path = path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    try:
        conn.executescript(SCHEMA)
        # 轻量迁移：为旧库补列
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(request_logs)")}
        for col in ("reasoning", "cached_tokens", "reasoning_tokens", "ttft_ms", "session_id", "parent_session_id"):
            if col not in cols:
                conn.execute(f"ALTER TABLE request_logs ADD COLUMN {col} TEXT")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()
        logger.info("SQLite 已初始化: %s", path)
    finally:
        conn.close()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def insert_log(data: dict[str, Any]) -> int:
    conn = _conn()
    try:
        cur = conn.execute(
            """INSERT INTO request_logs
            (ts, source_ip, route_name, session_id, parent_session_id, upstream_url, method, path, model,
             status_code, latency_ms, ttft_ms, is_stream, prompt_tokens, completion_tokens,
             total_tokens, cached_tokens, reasoning_tokens,
             request_body, response_body, reasoning, error, client_disconnected, user_agent)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data.get("ts") or datetime.utcnow().isoformat(),
                data.get("source_ip"),
                data.get("route_name"),
                data.get("session_id"),
                data.get("parent_session_id"),
                data.get("upstream_url"),
                data.get("method"),
                data.get("path"),
                data.get("model"),
                data.get("status_code"),
                data.get("latency_ms"),
                data.get("ttft_ms"),
                1 if data.get("is_stream") else 0,
                data.get("prompt_tokens"),
                data.get("completion_tokens"),
                data.get("total_tokens"),
                data.get("cached_tokens"),
                data.get("reasoning_tokens"),
                data.get("request_body"),
                data.get("response_body"),
                data.get("reasoning"),
                data.get("error"),
                1 if data.get("client_disconnected") else 0,
                data.get("user_agent"),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    return d


def query_logs(
    *,
    keyword: Optional[str] = None,
    source_ip: Optional[str] = None,
    model: Optional[str] = None,
    status: Optional[str] = None,      # 'success' | 'error' | 'all'
    is_stream: Optional[bool] = None,
    route_name: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    where: list[str] = []
    params: list[Any] = []
    if keyword:
        where.append("(request_body LIKE ? OR response_body LIKE ? OR model LIKE ?)")
        kw = f"%{keyword}%"
        params += [kw, kw, kw]
    if source_ip:
        where.append("source_ip = ?")
        params.append(source_ip)
    if model:
        where.append("model = ?")
        params.append(model)
    if route_name:
        where.append("route_name = ?")
        params.append(route_name)
    if is_stream is not None:
        where.append("is_stream = ?")
        params.append(1 if is_stream else 0)
    if status == "success":
        where.append("status_code >= 200 AND status_code < 300")
    elif status == "error":
        where.append("(status_code IS NULL OR status_code >= 400)")
    if start:
        where.append("ts >= ?")
        params.append(start)
    if end:
        where.append("ts <= ?")
        params.append(end)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    conn = _conn()
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM request_logs{where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"""SELECT id, ts, source_ip, route_name, session_id, parent_session_id, upstream_url, method, path, model,
                      status_code, latency_ms, ttft_ms, is_stream, prompt_tokens, completion_tokens,
                      total_tokens, cached_tokens, reasoning_tokens,
                      error, client_disconnected, user_agent
               FROM request_logs{where_sql}
               ORDER BY id DESC LIMIT ? OFFSET ?""",
            params + [page_size, (page - 1) * page_size],
        ).fetchall()
        return {"items": [_row_to_dict(r) for r in rows], "total": total,
                "page": page, "page_size": page_size}
    finally:
        conn.close()


def export_logs(limit: int = 100) -> list[dict]:
    """取最新 N 条记录（全字段），供导出。"""
    limit = max(1, min(limit, 10000))
    conn = _conn()
    try:
        rows = conn.execute(
            """SELECT id, ts, source_ip, route_name, session_id, parent_session_id,
                      upstream_url, method, path, model, status_code, latency_ms, ttft_ms,
                      is_stream, prompt_tokens, completion_tokens, total_tokens,
                      cached_tokens, reasoning_tokens, request_body, response_body,
                      reasoning, error, user_agent
               FROM request_logs ORDER BY id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_log(log_id: int) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM request_logs WHERE id = ?", (log_id,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def get_stats(hours: int = 24) -> dict:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    conn = _conn()
    try:
        agg = conn.execute(
            f"""SELECT COUNT(*) AS total,
                      COALESCE(SUM(total_tokens),0) AS tokens,
                      COALESCE(AVG(latency_ms),0) AS avg_latency,
                      SUM(CASE WHEN status_code IS NULL OR status_code >= 400 THEN 1 ELSE 0 END) AS errors
               FROM request_logs WHERE ts >= ?""",
            (since,),
        ).fetchone()
        total = agg["total"]
        tokens = agg["tokens"]
        avg_lat = round(agg["avg_latency"], 1)
        errors = agg["errors"]
        err_rate = round(errors / total * 100, 1) if total else 0.0
        by_model = [dict(r) for r in conn.execute(
            """SELECT model AS name, COUNT(*) AS count,
                      COALESCE(SUM(total_tokens),0) AS tokens
               FROM request_logs WHERE ts >= ? GROUP BY model ORDER BY count DESC""",
            (since,),
        ).fetchall()]
        by_route = [dict(r) for r in conn.execute(
            """SELECT COALESCE(route_name,'?') AS name, COUNT(*) AS count
               FROM request_logs WHERE ts >= ? GROUP BY route_name ORDER BY count DESC""",
            (since,),
        ).fetchall()]
        return {"hours": hours, "total": total, "total_tokens": tokens,
                "avg_latency_ms": avg_lat, "error_rate": err_rate,
                "by_model": by_model, "by_route": by_route}
    finally:
        conn.close()


def cleanup_old(days: int) -> int:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conn = _conn()
    try:
        cur = conn.execute("DELETE FROM request_logs WHERE ts < ?", (cutoff,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
