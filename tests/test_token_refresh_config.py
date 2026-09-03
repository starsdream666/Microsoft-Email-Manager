# -*- coding: utf-8 -*-
"""P1-1 / P2-2 验收:环境变量默认值、配置规范化、损坏配置容错。"""
import pytest


@pytest.fixture(autouse=True)
def _isolated_settings(tmp_path, monkeypatch):
    """每个用例独立 DATA_DIR,并把 main 指向该目录(通过重设模块内路径常量)。

    直接 monkeypatch 模块常量(而非 env + reload),避免重复导入 main 的副作用。
    """
    import main as M

    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "SITE_SETTINGS_FILE", tmp_path / "site_settings.json")
    monkeypatch.setattr(M, "ACCOUNTS_FILE", tmp_path / "accounts.json")
    return tmp_path


def test_defaults_use_env_when_first_created(monkeypatch):
    """首次初始化时,默认值应取环境变量解析结果而非硬编码 24/True。"""
    import main as M

    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_INTERVAL_HOURS", 6)
    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_ENABLED", True)
    defaults = M.get_default_site_settings()
    assert defaults["token_auto_refresh_interval_hours"] == 6
    assert defaults["token_auto_refresh_enabled"] is True


def test_env_false_disables_default(monkeypatch):
    import main as M

    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_ENABLED", False)
    assert M.get_default_site_settings()["token_auto_refresh_enabled"] is False


def test_saved_setting_wins_over_env(_isolated_settings, monkeypatch):
    """已保存的 site_settings 显式配置(12/false)不被 env 默认(6/true)覆盖。"""
    import main as M

    payload = M.get_default_site_settings()
    payload["token_auto_refresh_interval_hours"] = 12
    payload["token_auto_refresh_enabled"] = False
    M.save_site_settings(payload)

    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_INTERVAL_HOURS", 6)
    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_ENABLED", True)

    loaded = M.load_site_settings()
    assert loaded["token_auto_refresh_interval_hours"] == 12
    assert loaded["token_auto_refresh_enabled"] is False


@pytest.mark.parametrize("bad", ["24h", {}, [], None, {"a": 1}, 3.14])
def test_corrupt_interval_falls_back(_isolated_settings, bad):
    """手工损坏配置不应抛异常,应回退默认且不使调度解析崩溃。"""
    import main as M

    payload = M.get_default_site_settings()
    payload["token_auto_refresh_interval_hours"] = bad
    M._write_json_file(M.SITE_SETTINGS_FILE, payload)
    loaded = M.load_site_settings()
    assert isinstance(loaded["token_auto_refresh_interval_hours"], int)
    assert 1 <= loaded["token_auto_refresh_interval_hours"] <= 720
    enabled, interval = M.resolve_token_auto_refresh_config(loaded)
    assert isinstance(enabled, bool)
    assert 1 <= interval <= 720


def test_interval_bounds():
    import main as M

    assert M.normalize_token_auto_refresh_interval(0, 24) == 1
    assert M.normalize_token_auto_refresh_interval(-5, 24) == 1
    assert M.normalize_token_auto_refresh_interval(1000, 24) == 720
    assert M.normalize_token_auto_refresh_interval("24.0", 24) == 24
    assert M.normalize_token_auto_refresh_interval(None, 7) == 7
    assert M.normalize_token_auto_refresh_interval("abc", 7) == 7


def test_enabled_string_parsing():
    import main as M

    assert M.normalize_token_auto_refresh_enabled("false", True) is False
    assert M.normalize_token_auto_refresh_enabled("0", True) is False
    assert M.normalize_token_auto_refresh_enabled("off", True) is False
    assert M.normalize_token_auto_refresh_enabled("true", False) is True
    assert M.normalize_token_auto_refresh_enabled("1", False) is True
    assert M.normalize_token_auto_refresh_enabled("yes", False) is True
    assert M.normalize_token_auto_refresh_enabled("garbage", True) is True  # 无法解析回退默认
    assert M.normalize_token_auto_refresh_enabled(True, False) is True
    assert M.normalize_token_auto_refresh_enabled(False, True) is False
    assert M.normalize_token_auto_refresh_enabled(0, True) is False
    assert M.normalize_token_auto_refresh_enabled(5, False) is True
    assert M.normalize_token_auto_refresh_enabled(None, True) is True


def test_old_config_missing_fields_falls_back(_isolated_settings):
    """旧配置文件缺新增字段:load 回退默认且不抛异常。"""
    import main as M

    old = {
        "home_title": "x",
        "home_intro": "y",
        "admin_login_path": "/admin",
        "share_domain_enabled": False,
        "share_domain": "",
    }
    M._write_json_file(M.SITE_SETTINGS_FILE, old)
    loaded = M.load_site_settings()
    assert loaded["token_auto_refresh_interval_hours"] == M.TOKEN_AUTO_REFRESH_INTERVAL_HOURS
    assert loaded["token_auto_refresh_enabled"] == M.TOKEN_AUTO_REFRESH_ENABLED


def test_updated_at_preserved_and_bool_normalized(_isolated_settings):
    import main as M

    payload = M.get_default_site_settings()
    payload["token_auto_refresh_enabled"] = "false"  # 字符串 false 应规范化为 False
    saved = M.save_site_settings(payload)
    assert saved["token_auto_refresh_enabled"] is False
    assert M.load_site_settings()["token_auto_refresh_enabled"] is False
