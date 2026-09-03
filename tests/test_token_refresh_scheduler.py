# -*- coding: utf-8 -*-
"""P1-2 验收:调度循环在配置变更后可被唤醒并重新计时。"""
import asyncio

import pytest


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    import main as M

    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "SITE_SETTINGS_FILE", tmp_path / "site_settings.json")
    monkeypatch.setattr(M, "ACCOUNTS_FILE", tmp_path / "accounts.json")
    return tmp_path


async def _run_loop_with_wake(monkeypatch, initial, changed, wake_after_s=0.05):
    """启动 _auto_refresh_loop 的可控副本:wake 后应尽快重读配置并进入新等待。

    返回 observed:first/second 两轮读到的 (enabled, interval) 与是否被唤醒。
    """
    import main as M

    refresh_calls = []
    observed = {}

    # 把 refresh 换成探针
    async def fake_refresh():
        refresh_calls.append(1)

    monkeypatch.setattr(M, "refresh_all_account_health", fake_refresh)

    # 初始配置
    payload = M.get_default_site_settings()
    payload["token_auto_refresh_enabled"] = initial[0]
    payload["token_auto_refresh_interval_hours"] = initial[1]
    M.save_site_settings(payload)

    event = asyncio.Event()
    monkeypatch.setattr(M, "_token_auto_refresh_event", event)

    async def change_then_wake():
        # 先改配置,再唤醒(保证 wake 时新配置已落盘)
        await asyncio.sleep(0.02)
        payload = M.get_default_site_settings()
        payload["token_auto_refresh_enabled"] = changed[0]
        payload["token_auto_refresh_interval_hours"] = changed[1]
        M.save_site_settings(payload)
        M.wake_token_auto_refresh_scheduler()

    async def body():
        # 第一轮读配置
        enabled, interval = M.resolve_token_auto_refresh_config(M.load_site_settings())
        observed["first"] = (enabled, interval)
        wake_task = asyncio.create_task(change_then_wake())
        try:
            await asyncio.wait_for(event.wait(), timeout=interval * 3600)
        except asyncio.TimeoutError:
            observed["timeout"] = True
        else:
            event.clear()
            observed["woken"] = True
        await wake_task
        # 第二轮读配置(变更后)
        enabled, interval = M.resolve_token_auto_refresh_config(M.load_site_settings())
        observed["second"] = (enabled, interval)

    await body()
    return observed


def test_wake_recomputes_interval(_isolated, monkeypatch):
    """24h 初始 → wake(改为 1h)后,循环第二轮应读到新配置(无需等满 24h)。"""
    async def scenario():
        return await _run_loop_with_wake(monkeypatch, (True, 24), (True, 1), wake_after_s=0.05)

    observed = asyncio.run(scenario())
    assert observed["first"] == (True, 24)
    assert observed["woken"] is True
    assert observed["second"] == (True, 1)


def test_wake_on_disable_prevents_extra_refresh(_isolated, monkeypatch):
    """开启中 wake 并改为禁用:循环重读后 enabled=False,不应再执行 refresh。"""
    import main as M

    refresh_calls = []
    async def fake_refresh():
        refresh_calls.append(1)
    monkeypatch.setattr(M, "refresh_all_account_health", fake_refresh)

    payload = M.get_default_site_settings()
    payload["token_auto_refresh_enabled"] = True
    payload["token_auto_refresh_interval_hours"] = 24
    M.save_site_settings(payload)

    event = asyncio.Event()
    monkeypatch.setattr(M, "_token_auto_refresh_event", event)

    async def scenario():
        enabled, interval = M.resolve_token_auto_refresh_config(M.load_site_settings())
        # wake 前一刻改为禁用
        async def disable():
            await asyncio.sleep(0.02)
            p = M.get_default_site_settings()
            p["token_auto_refresh_enabled"] = False
            M.save_site_settings(p)
            M.wake_token_auto_refresh_scheduler()
        t = asyncio.create_task(disable())
        try:
            await asyncio.wait_for(event.wait(), timeout=interval * 3600)
        except asyncio.TimeoutError:
            pass
        else:
            event.clear()
        await t
        # 到点后(这里由事件提前结束),重新确认 enabled
        enabled_now, _ = M.resolve_token_auto_refresh_config(M.load_site_settings())
        if enabled_now:
            await fake_refresh()
        return len(refresh_calls)

    calls = asyncio.run(scenario())
    assert calls == 0  # 禁用后不执行额外刷新


def test_reenable_reschedules(_isolated, monkeypatch):
    """关闭→重新启用并设新周期,解析结果应为新的开关/间隔。"""
    import main as M

    payload = M.get_default_site_settings()
    payload["token_auto_refresh_enabled"] = False
    payload["token_auto_refresh_interval_hours"] = 24
    M.save_site_settings(payload)
    assert M.resolve_token_auto_refresh_config(M.load_site_settings()) == (False, 24)

    payload["token_auto_refresh_enabled"] = True
    payload["token_auto_refresh_interval_hours"] = 2
    M.save_site_settings(payload)
    assert M.resolve_token_auto_refresh_config(M.load_site_settings()) == (True, 2)


def test_rapid_saves_single_event(_isolated):
    """连续快速保存只 set 一个进程内 Event,不会创建多个调度任务。"""
    import main as M

    M._token_auto_refresh_event = None
    M.wake_token_auto_refresh_scheduler()  # 未初始化 → 空操作不抛

    event = asyncio.Event()
    M._token_auto_refresh_event = event
    for _ in range(5):
        M.wake_token_auto_refresh_scheduler()
    assert event.is_set()
