"""端到端测试：真实 agent -> 网关 -> IP 路由 -> 真实 DeepSeek。

链路：
  agent(OpenAI SDK, base_url=http://127.0.0.1:8801)
    -> 网关接收 /chat/completions
    -> 按源 IP 127.0.0.1 命中配置的 local-agent 路由
    -> 转发到 https://api.deepseek.com
    -> 响应回流并落库

- api_key 取自环境变量 DEEPSEEK_API_KEY_OPENJIUWEN（未设则 skip）
- model = deepseek-flash
- 网关固定端口 8801
"""
from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn
from openai import OpenAI

MODEL = "deepseek-flash"
UPSTREAM_BASE_URL = "https://api.deepseek.com"
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8801
GATEWAY_BASE = f"http://{GATEWAY_HOST}:{GATEWAY_PORT}"
ENV_KEY = "DEEPSEEK_API_KEY_OPENJIUWEN"
ROUTE_NAME = "local-agent"   # 配置中匹配 127.0.0.1 的路由名，用于佐证 IP 路由


def _get_api_key() -> str | None:
    key = os.environ.get(ENV_KEY)
    if key:
        return key
    # Windows: 若当前进程未继承，尝试从用户级环境变量读取
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as h:
            val, _ = winreg.QueryValueEx(h, ENV_KEY)
            if val:
                return val
    except Exception:
        pass
    return None


# ---------- 服务器辅助 ----------
def _start_server(app_obj, port: int) -> uvicorn.Server:
    cfg = uvicorn.Config(app=app_obj, host=GATEWAY_HOST, port=port, log_level="warning")
    server = uvicorn.Server(cfg)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    return server


def _wait_ready(url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(0.1)
    return False


def _port_in_use(host: str, port: int) -> bool:
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


# ---------- 模块级 fixture：启动网关 ----------
@pytest.fixture(scope="module")
def gw():
    api_key = _get_api_key()
    if not api_key:
        pytest.skip(f"环境变量 {ENV_KEY} 未设置，跳过真实链路测试")
    if _port_in_use(GATEWAY_HOST, GATEWAY_PORT):
        pytest.skip(f"端口 {GATEWAY_PORT} 被占用，请先释放")

    tmp = Path(tempfile.mkdtemp(prefix="llm_gw_test_"))
    cwd = os.getcwd()
    os.chdir(tmp)

    (tmp / "config.yaml").write_text(
        f"""
server:
  host: "{GATEWAY_HOST}"
  port: {GATEWAY_PORT}
trust_forwarded_for: false
upstream_timeout: 60
logging:
  db_path: "data/test.db"
  max_body_kb: 256
  mask_keys: true
rate_limit:
  enabled: false
retention:
  enabled: false
routes:
  - name: {ROUTE_NAME}
    source_ips: ["127.0.0.1"]
    upstream:
      base_url: "{UPSTREAM_BASE_URL}"
  - name: default
    source_ips: ["0.0.0.0/0"]
    upstream:
      base_url: "{UPSTREAM_BASE_URL}"
""",
        encoding="utf-8",
    )

    # 重置配置单例，使其在网关启动时重新读取临时 config.yaml
    import app.config as cm
    cm._singleton = None

    from app.main import app as gateway_app
    gw_server = _start_server(gateway_app, GATEWAY_PORT)

    try:
        assert _wait_ready(GATEWAY_BASE + "/admin/health"), "网关未就绪"
        yield {"base": GATEWAY_BASE}
    finally:
        gw_server.should_exit = True
        time.sleep(0.6)
        os.chdir(cwd)


@pytest.fixture(scope="module")
def agent(gw) -> OpenAI:
    """真实 agent：base_url 指向网关，自带真实 DeepSeek key（网关只改指向，不替换 key）。"""
    key = _get_api_key()
    return OpenAI(base_url=gw["base"], api_key=key, timeout=60)


# ---------- 测试用例 ----------
def test_agent_blocking(agent):
    """阻塞模式：agent -> 网关 -> DeepSeek，返回非空 content 与 usage。"""
    resp = agent.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "用一句话介绍你自己"}],
        max_tokens=512,
        stream=False,
    )
    content = resp.choices[0].message.content or ""
    assert content.strip(), f"content 为空: {resp}"
    assert resp.usage and resp.usage.total_tokens > 0, f"usage 异常: {resp.usage}"


def test_agent_streaming(agent):
    """流式模式：agent -> 网关 -> DeepSeek，逐 chunk 接收，内容非空。

    deepseek-flash 为推理模型，流式先吐 reasoning_content（思维链）再吐 content。
    """
    stream = agent.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "用一句话介绍你自己"}],
        max_tokens=512,
        stream=True,
        stream_options={"include_usage": True},
    )
    collected: list[str] = []
    finish_reason = None
    usage_total = 0
    for chunk in stream:
        if chunk.choices:
            delta = chunk.choices[0].delta
            if delta and delta.content:
                collected.append(delta.content)
            if chunk.choices[0].finish_reason:
                finish_reason = chunk.choices[0].finish_reason
        if chunk.usage:
            usage_total = chunk.usage.total_tokens
    assert "".join(collected).strip(), "流式 content 为空"
    assert finish_reason, "未收到 finish_reason"
    assert usage_total > 0, f"未收到 usage: {usage_total}"


def test_gateway_captured_and_routed(gw):
    """网关捕获两条请求，并按源 IP 127.0.0.1 命中 local-agent 路由。"""
    base = gw["base"]
    with httpx.Client(timeout=30) as c:
        r = c.get(f"{base}/api/logs", params={"page": 1, "page_size": 10})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] >= 2, f"期望至少 2 条日志，实际 {d['total']}"

    items = d["items"]
    streams = [it for it in items if it["is_stream"]]
    blocks = [it for it in items if not it["is_stream"]]
    assert blocks, "缺少阻塞日志"
    assert streams, "缺少流式日志"

    # 阻塞记录：IP 路由命中、模型正确、tokens 正常
    b = blocks[0]
    assert b["source_ip"] == "127.0.0.1", f"源 IP 异常: {b['source_ip']}"
    assert b["route_name"] == ROUTE_NAME, f"路由未命中: {b['route_name']}"
    assert b["model"] == MODEL, f"model 异常: {b['model']}"
    assert b["status_code"] == 200, f"状态异常: {b['status_code']}"
    assert b["total_tokens"] and b["total_tokens"] > 0
    # 阻塞详情：缓存命中 / 推理 tokens 已捕获（get_log 返回全字段）
    with httpx.Client(timeout=30) as c:
        bdetail = c.get(f"{base}/api/logs/{b['id']}").json()
    assert bdetail["reasoning_tokens"] is not None and bdetail["reasoning_tokens"] > 0, \
        f"阻塞 reasoning_tokens 未捕获: {bdetail.get('reasoning_tokens')}"
    assert bdetail["cached_tokens"] is not None, f"阻塞 cached_tokens 未捕获"

    # 流式详情：汇总内容非空、推理思维链已捕获、tokens 正常
    with httpx.Client(timeout=30) as c:
        detail = c.get(f"{base}/api/logs/{streams[0]['id']}").json()
    assert detail["is_stream"] == 1
    assert detail["route_name"] == ROUTE_NAME
    assert detail["status_code"] == 200
    assert detail["response_body"] and detail["response_body"].strip(), "流式汇总 content 为空"
    assert detail["reasoning"] and detail["reasoning"].strip(), "流式 reasoning 未捕获"
    assert detail["total_tokens"] and detail["total_tokens"] > 0
    assert detail["reasoning_tokens"] is not None and detail["reasoning_tokens"] > 0, \
        f"流式 reasoning_tokens 未捕获: {detail.get('reasoning_tokens')}"
    assert detail["cached_tokens"] is not None, f"流式 cached_tokens 未捕获"


def test_agent_hint_session(gw):
    """agent_hint 中的 session_id / parent_session_id 被捕获；且 body 原样落库（铁律：不改输入）。"""
    base = gw["base"]
    key = _get_api_key()
    sid, pid = "test-sess-123", "test-parent-456"
    with httpx.Client(timeout=60) as c:
        r = c.post(
            f"{base}/v1/chat/completions",
            json={"model": MODEL, "max_tokens": 16,
                  "messages": [{"role": "user", "content": "hi"}],
                  "agent_hint": {"session_id": sid, "parent_session_id": pid}},
            headers={"Authorization": f"Bearer {key}"},
        )
    assert r.status_code == 200, f"期望 200，实际 {r.status_code}: {r.text[:200]}"

    with httpx.Client(timeout=30) as c:
        d = c.get(f"{base}/api/logs", params={"page": 1, "page_size": 1}).json()
        it = d["items"][0]
        detail = c.get(f"{base}/api/logs/{it['id']}").json()
    assert it["session_id"] == sid, f"session_id 未捕获: {it.get('session_id')}"
    assert it["parent_session_id"] == pid, f"parent_session_id 未捕获: {it.get('parent_session_id')}"
    # 铁律：agent_hint 仍保留在落库的 request_body 中（网关未修改输入）
    import json as _json
    body = _json.loads(detail["request_body"]) if isinstance(detail["request_body"], str) else detail["request_body"]
    assert body.get("agent_hint", {}).get("session_id") == sid, "request_body 中 agent_hint 丢失（违反铁律）"
