"""LLM Gateway - 一条命令启动。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn
from app.config import get_config_manager


def main() -> None:
    cfg = get_config_manager()
    server = cfg.current.server
    uvicorn.run(
        "app.main:app",
        host=server.host,
        port=server.port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
