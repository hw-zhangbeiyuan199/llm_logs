# 确保项目根目录在 sys.path 上，便于 tests/ 中 `import app.*`
import sys
from pathlib import Path

_root = str(Path(__file__).resolve().parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
