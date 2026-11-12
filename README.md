# LLM Gateway

一个简单可扩展的 LLM 请求转发网关：转发 OpenAI 兼容格式的请求到上游 LLM，记录输入输出日志，前台界面展示转发记录。支持阻塞与流式（SSE）两种模式、按源 IP 配置路由映射、配置文件热重载（不重启生效）。

## 快速开始

```bash
pip install -r requirements.txt
python run.py
```

首次运行会自动生成 `config.yaml` 与 `data/` 目录。默认路由指向本地 `http://localhost:11434/v1`（Ollama 的 OpenAI 兼容端点），本地模型用户零配置可用。

- 前台面板：http://localhost:8000/
- 转发端点：`POST http://localhost:8000/v1/chat/completions`、`/v1/completions`、`GET /v1/models`
- 用法与 OpenAI 官方 API 完全一致，把客户端的 `base_url` 指向本网关即可。

### 示例（阻塞）

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3","messages":[{"role":"user","content":"你好"}]}'
```

### 示例（流式）

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

## 配置（config.yaml）

所有配置均可**热重载**——修改保存后约 2 秒内自动生效，无需重启（监听地址/端口与 DB 路径除外，需重启）。

| 配置项 | 说明 | 默认 |
|---|---|---|
| `server.host/port` | 监听地址与端口（重启生效） | 0.0.0.0:8000 |
| `trust_forwarded_for` | 是否信任 `X-Forwarded-For` 取真实 IP（反代后置 true） | false |
| `upstream_timeout` | 上游请求超时秒数 | 300 |
| `logging.db_path` | SQLite 路径（重启生效） | data/llm_gateway.db |
| `logging.max_body_kb` | 单条请求/响应体存储上限，超出截断 | 512 |
| `logging.mask_keys` | 日志与配置展示中对 api_key 脱敏 | true |
| `rate_limit.enabled` / `requests_per_minute` | 按 IP 每分钟限流（默认关闭） | false / 60 |
| `retention.enabled` / `days` | 日志保留清理（默认关闭） | false / 30 |
| `routes` | 路由规则链，按序匹配，首条命中生效 | 见下 |

### 路由规则

```yaml
routes:
  - name: office
    source_ips:               # 支持精确 IP 与 CIDR
      - "192.168.1.0/24"
      - "10.0.0.5"
    upstream:
      base_url: "https://api.openai.com/v1"   # 含 /v1，与 OpenAI SDK 一致
      api_key: "sk-xxx"
      model_mapping: {"gpt-4": "gpt-4o"}      # 可选：模型名重映射
  - name: default
    source_ips: ["0.0.0.0/0"]
    upstream:
      base_url: "http://localhost:11434/v1"
      api_key: ""
```

未命中任何 `source_ips` 时走最后一条（约定为 `default` 兜底）。

## 管理接口

| 端点 | 说明 |
|---|---|
| `GET /admin/health` | 健康检查，含路由数与配置错误 |
| `GET /admin/config` | 当前生效配置（api_key 已脱敏），用于确认热重载结果 |
| `POST /admin/reload` | 手动触发重载 |
| `POST /admin/retention/cleanup?days=30` | 手动清理旧日志 |

## 架构与扩展

```
app/
├── main.py              入口，组装路由与静态托管、启动任务
├── config.py            配置加载 + 热重载（mtime 轮询 / 校验 / 原子替换）
├── db.py                SQLite(WAL) 日志读写统计
├── api/
│   ├── proxy.py         /v1/* 转发端点（阻塞 + SSE 流式）
│   ├── logs.py          日志查询 / 详情 / 统计 API
│   └── admin.py         health / config / reload
├── core/
│   ├── router.py        源 IP(CIDR/精确) 路由解析
│   ├── forwarder.py     httpx 转发 + SSE 增量汇总
│   └── pipeline.py      ★ Hook 管道（核心扩展点）
└── web/static/index.html   Vue3 内嵌前台
```

### 扩展新功能

在 `core/pipeline.py` 实现 `Hook` 基类的 `on_request` / `on_response` / `on_error` 任意方法，注册到 `get_pipeline()` 即可。内置 Hook：

- `RateLimitHook` 按 IP 限流（配置开关）
- `MaskHook` 日志中 api_key 脱敏
- `RetentionHook` 定时清理旧日志

预留但 v1 未实现（可作为 Hook 加入）：网关鉴权、CORS、故障转移/重试、多协议转换。

## 日志字段（request_logs 表）

`id, ts, source_ip, route_name, upstream_url, method, path, model, status_code, latency_ms, is_stream, prompt_tokens, completion_tokens, total_tokens, request_body, response_body, error, client_disconnected, user_agent`

流式请求的 `response_body` 存放解析 SSE delta 后汇总的完整文本，`tokens` 仅在上游发送 usage 时填充。

## 验证

- 阻塞与 `stream: true` 请求均已通过烟测（mock 上游）
- 热重载：运行中修改 `config.yaml` 路由名 → `/admin/config` 实时反映
- IP 路由：`X-Forwarded-For: 10.0.0.5` 正确命中 office 规则
- 日志与统计 API 正常返回，前台面板可查询、过滤、查看详情
