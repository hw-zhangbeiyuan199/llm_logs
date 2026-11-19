# AGENTS.md

LLM 请求转发网关（FastAPI + httpx + SQLite + Vue3 单文件前台）。架构与配置说明见 README.md，本文件只记容易踩坑的事实。

## 常用命令

- 安装依赖（系统 Python 无 pip，必须走 uv）：
  `uv pip install --python "C:\Users\s60113001\AppData\Local\PythonEnv\Python\python.exe" -r requirements.txt`
  开发依赖另加 `-r requirements-dev.txt`（pytest/openai）
- 启动：`python run.py`（run.py 开头的 sys.path 自举兼容嵌入式 Python `._pth` 隔离模式，勿删）
- 测试：`python -m pytest tests/ -q`；单文件 `python -m pytest tests/test_forwarder_stream.py -v`
- 服务验证：`curl.exe http://localhost:8990/admin/health`（本地 config.yaml 用 8990 端口，绑定 127.0.0.1）
- 服务日志：`server.log` / `server.err.log`（已 gitignore）；重启前先查 8990 端口占用——用户可能自己在终端跑着实例

## 环境坑

- Shell 是 PowerShell 5.1：不支持 `&&` 与 heredoc；`curl` 是 Invoke-WebRequest 别名，下文件用 `curl.exe`；多行 commit 信息用 `git commit -F <msgfile>`
- 复杂查询别写长 `python -c "..."`（引号嵌套地狱），写临时 .py 文件执行
- 直接下载 get-pip.py 会被网络重置；PyPI 走华为云镜像（本机已配 `UV_INDEX_URL`），统一用 uv
- SQLite 只读排查：`sqlite3.connect('file:data/llm_gateway.db?mode=ro', uri=True)`；注意 `cached_tokens`/`reasoning_tokens` 等轻量迁移加的列是 TEXT（存 `'4096'` 这类字符串）
- 项目内有 `.venv`（uv 建，自带内嵌 .gitignore 不入库）与系统 Python 两套环境；测试与服务此前用系统 Python 跑（pytest 只装在系统 Python）

## 工作流约定

- `config.yaml` 是本地环境配置（端口 8990、内网上游 `172.24.107.130:7000/v1`），**不要提交**；模板为 `config.example.yaml`
- `git pull` 时本地 config.yaml 几乎必冲突（远程也改它）：stash → pull → pop，冲突解决原则——保留本地端口与上游地址，其余采远程
- commit 信息：中文单行主题 + 要点列表；仅在用户明确要求时 commit / push
- 代码风格：black/ruff 默认（88 列、双引号）

## 设计铁律（改代码前必读）

- 网关只改指向，不替换 api_key：路由无密钥字段，agent 的 `Authorization` 头原样透传上游
- 不改请求体：`agent_hint` 只读提取落库（session_id/parent_session_id），body 原样转发上游
- `response_body` = 全部返回结果：
  - 阻塞：上游原始响应 JSON 原文
  - 流式：聚合重构的完整响应 JSON（与阻塞同构）——content、reasoning_content、tool_calls（按 index 拼接分片）、usage（上游原样回填，含 prompt_cache_hit_tokens 等扩展字段）
- 流式解析三要点：增量 UTF-8 解码（防中文跨 chunk 乱码）、流结束 flush 无换行末行、客户端断连时先 `await gen.aclose()` 再落库
- 配置热重载：除 `server.*` 与 `logging.db_path` 外全部改动约 2 秒内自动生效，无需重启

## 测试

- `tests/test_forwarder_stream.py`：流式聚合单测，monkeypatch `httpx.AsyncClient` mock 上游，无外部依赖；新增流式解析逻辑必须在此补用例
- `tests/test_proxy.py`：真实 DeepSeek e2e，需环境变量 `DEEPSEEK_API_KEY_OPENJIUWEN`（进程未继承时从 HKCU\Environment 注册表兜底读取），未设自动 skip；网关固定占用 8801 端口
