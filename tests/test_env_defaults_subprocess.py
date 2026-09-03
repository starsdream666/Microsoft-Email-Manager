# -*- coding: utf-8 -*-
"""P1-1 补充:环境变量在真实进程启动时驱动模块常量(不经 monkeypatch)。"""
import os
import subprocess
import sys
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_python_with_env(code: str, env: dict[str, str]) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PROJECT_ROOT),
        env={**os.environ, "PYTHONIOENCODING": "utf-8", **env},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, f"subprocess failed:\n{proc.stderr}"
    return proc.stdout.strip()


def test_env_interval_drives_module_constant():
    out = _run_python_with_env(
        textwrap.dedent(
            """
            import os, sys, tempfile
            os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='sub_')
            sys.path.insert(0, r'%s')
            import main as M
            print(M.TOKEN_AUTO_REFRESH_INTERVAL_HOURS)
            print(M.TOKEN_AUTO_REFRESH_ENABLED)
            print(M.get_default_site_settings()['token_auto_refresh_interval_hours'])
            print(M.get_default_site_settings()['token_auto_refresh_enabled'])
            """
        ).strip()
        % str(PROJECT_ROOT),
        {"TOKEN_AUTO_REFRESH_INTERVAL_HOURS": "6", "TOKEN_AUTO_REFRESH_ENABLED": "false"},
    )
    lines = out.splitlines()
    assert lines[0] == "6", f"TOKEN_AUTO_REFRESH_INTERVAL_HOURS 常量应=6, got {lines[0]}"
    assert lines[1] == "False", f"TOKEN_AUTO_REFRESH_ENABLED 常量应=False, got {lines[1]}"
    # 默认 site_settings 也取 env
    assert lines[2] == "6"
    assert lines[3] == "False"


def test_invalid_env_interval_falls_back_to_code_default():
    out = _run_python_with_env(
        textwrap.dedent(
            """
            import os, sys, tempfile
            os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='sub_')
            sys.path.insert(0, r'%s')
            import main as M
            print(M.TOKEN_AUTO_REFRESH_INTERVAL_HOURS)
            """
        ).strip()
        % str(PROJECT_ROOT),
        {"TOKEN_AUTO_REFRESH_INTERVAL_HOURS": "not-a-number"},
    )
    assert out == "24", f"非法 env 应回退代码兜底 24, got {out}"
