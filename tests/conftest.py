# -*- coding: utf-8 -*-
"""
pytest 共享夹具:把 DATA_DIR 指到 pytest 临时目录,再 import 被测模块。

注意:main.py 在模块导入时按 env 解析路径与开关默认值(TOKEN_AUTO_REFRESH_*),
因此必须在 import main 之前设置 DATA_DIR / 环境变量,故 conftest 用
os.environ 预设而非 fixture。
"""
import os
import sys
import tempfile
from pathlib import Path

# 确保 import main 时指向临时数据目录(不污染仓库 data/)
_TMP_DATA = tempfile.mkdtemp(prefix="outlook_target_test_")
os.environ["DATA_DIR"] = _TMP_DATA

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
