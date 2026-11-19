# LLM Gateway

一个简单可扩展的 LLM 请求转发网关：转发 OpenAI 兼容格式的请求到上游 LLM，记录输入输出日志，前台界面展示转发记录。支持阻塞与流式（SSE）两种模式、按源 IP 配置路由映射、配置文件热重载（不重启生效）。

## 快速开始

```bash
pip install -r requirements.txt
python run.py
```

首次运行会自动生成 `config.yaml` 与 `data/` 目录；也可直接复制 `config.example.yaml` 为 `config.yaml` 后修改。默认监听 `127.0.0.1:8000`（仅本机访问；对外服务改 `server.host: 0.0.0.0` 后重启）。默认路由 office→OpenAI、default→DeepSeek，见模板文件。

- 前台面板：http://localhost:8000/
- 转发端点：`POST http://localhost:8000/v1/chat/completions`、`/v1/completions`、`GET /v1/models`
- 用法与 OpenAI 官方 API 完全一致，把客户端的 `base_url` 指向本网关即可，agent 自带的 `Authorization` 原样透传给上游。

### 示例（阻塞）

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}]}'
```

### 示例（流式）

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","stream":true,"messages":[{"role":"user","content":"你好"}]}'
```

## 配置（config.yaml）

所有配置均可**热重载**——修改保存后约 2 秒内自动生效，无需重启（监听地址/端口与 DB 路径除外，需重启）。

| 配置项 | 说明 | 默认 |
|---|---|---|
| `server.host/port` | 监听地址与端口（重启生效） | 127.0.0.1:8000 |
| `trust_forwarded_for` | 是否信任 `X-Forwarded-For` 取真实 IP（反代后置 true） | false |
| `upstream_timeout` | 上游请求超时秒数 | 300 |
| `logging.db_path` | SQLite 路径（重启生效） | data/llm_gateway.db |
| `logging.max_body_kb` | 单条请求/响应体存储上限，超出截断 | 512 |
| `logging.mask_keys` | 日志与配置展示中对 api_key 脱敏 | true |
| `rate_limit.enabled` / `requests_per_minute` | 按 IP 每分钟限流（默认关闭） | false / 60 |
| `retention.enabled` / `days` | 日志保留清理（默认关闭） | false / 30 |
| `routes` | 路由规则链，按序匹配，首条命中生效 | 见 `config.example.yaml` |

### 路由规则

```yaml
routes:
  - name: office
    source_ips:               # 支持精确 IP 与 CIDR
      - "192.168.1.0/24"
      - "10.0.0.5"
    upstream:
      base_url: "https://api.openai.com/v1"   # 含 /v1，与 OpenAI SDK 一致
  - name: default
    source_ips: ["0.0.0.0/0"]
    upstream:
      base_url: "https://api.deepseek.com"    # DeepSeek 无需 /v1
```

未命中任何 `source_ips` 时走最后一条（约定为 `default` 兜底）。

网关只改指向，不替换 api_key：路由配置不含密钥字段，agent 请求头中的 `Authorization` 原样透传给上游。

## 管理接口

| 端点 | 说明 |
|---|---|
| `GET /admin/health` | 健康检查，含路由数与配置错误 |
| `GET /admin/config` | 当前生效配置（结构化，api_key 已脱敏） |
| `GET /admin/config/raw` | config.yaml 原文，供在线编辑 |
| `PUT /admin/config` | 保存 config.yaml：YAML 语法校验通过才写入并立即重载 |
| `POST /admin/reload` | 手动触发重载 |
| `POST /admin/retention/cleanup?days=30` | 手动清理旧日志 |
| `POST /admin/logs/clear` | 清空全部请求日志（不可恢复） |

## 日志查询 API（前台使用）

| 端点 | 说明 |
|---|---|
| `GET /api/logs` | 分页查询，支持 keyword / source_ip / model / status / is_stream / route_name / 起止时间过滤 |
| `GET /api/logs/{id}` | 单条详情（含请求体、响应、推理思维链全文） |
| `GET /api/logs/export?limit=100` | 导出最新 N 条（≤10000）为 Excel(.xlsx) |
| `GET /api/stats?hours=24` | 汇总统计：总量、tokens、平均时延、错误率、按模型/路由分布 |

## 前台面板

- 记录查询与多条件过滤、详情查看（请求体 / 响应 / 推理思维链）
- 首Token时延、缓存命中 / 推理 tokens 统计列；自动刷新（默认开启，5s）
- Excel 导出、清空全部日志
- 在线编辑 config.yaml：保存时校验语法，通过即热重载
- session_id 缩写展示，便于追踪同一 agent 会话链

## agent_hint 会话标识（可选）

agent 在请求体中附带 `agent_hint` 字段，网关**只读提取**（不修改请求体，原样转发上游）：

```json
{"model": "...", "messages": [...],
 "agent_hint": {"session_id": "sess-1", "parent_session_id": "sess-0"}}
```

提取结果记入 `session_id` / `parent_session_id` 列，供前台过滤与 Excel 导出追踪会话链路。

## 架构与扩展

```
app/
├── main.py              入口，组装路由与静态托管、启动任务
├── config.py            配置加载 + 热重载（mtime 轮询 / 校验 / 原子替换）
├── db.py                SQLite(WAL) 日志读写统计
├── api/
│   ├── proxy.py         /v1/* 转发端点（阻塞 + SSE 流式）
│   ├── logs.py          日志查询 / 详情 / 统计 / Excel 导出 API
│   └── admin.py         health / config 读写 / reload / 日志清理
├── core/
│   ├── router.py        源 IP(CIDR/精确) 路由解析
│   ├── forwarder.py     httpx 转发 + SSE 流式聚合（完整重构响应）
│   └── pipeline.py      ★ Hook 管道（核心扩展点）
└── web/static/index.html   Vue3 内嵌前台
tests/                   流式聚合单元测试 + 真实链路 e2e 测试
```

### 扩展新功能

在 `core/pipeline.py` 实现 `Hook` 基类的 `on_request` / `on_response` / `on_error` 任意方法，注册到 `get_pipeline()` 即可。内置 Hook：

- `RateLimitHook` 按 IP 限流（配置开关）
- `MaskHook` 日志中 api_key 脱敏
- `RetentionHook` 定时清理旧日志

预留但 v1 未实现（可作为 Hook 加入）：网关鉴权、CORS、故障转移/重试、多协议转换。

## 日志字段（request_logs 表）

`id, ts, source_ip, route_name, session_id, parent_session_id, upstream_url, method, path, model, status_code, latency_ms, ttft_ms, is_stream, prompt_tokens, completion_tokens, total_tokens, cached_tokens, reasoning_tokens, request_body, response_body, reasoning, error, client_disconnected, user_agent`

- 阻塞请求：`response_body` 为上游原始响应 JSON
- 流式请求：`response_body` 为聚合重构的完整响应 JSON（与阻塞同构）——`content`、`reasoning_content`、`tool_calls`（分片拼接）、`usage`（上游原样，含缓存命中/推理 tokens 等扩展字段）全部保留；`reasoning` 列另存思维链全文
- `ttft_ms` 为流式首 Token 时延；tokens 仅在上游发送 usage 时填充
- 请求/响应体超过 `max_body_kb` 时截断；`mask_keys` 开启时请求体中的疑似密钥字段脱敏存储

## 测试

```bash
pip install -r requirements-dev.txt
pytest
```

- `tests/test_forwarder_stream.py`：流式聚合单元测试（mock 上游，无外部依赖）——tool_calls 聚合、usage 原样回填、中文跨 chunk 解码、末行无换行等
- `tests/test_proxy.py`：真实链路 e2e（OpenAI SDK → 网关 → DeepSeek，需环境变量 `DEEPSEEK_API_KEY_OPENJIUWEN`，未设置自动跳过）
