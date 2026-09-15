# -*- coding: utf-8 -*-
"""直接验证生产调度循环、240 小时持久化计划和健康检查状态，不复制调度实现。"""
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from starlette.requests import Request


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    import main as M

    for name, value in list(vars(M).items()):
        if name.endswith("_FILE") and isinstance(value, Path):
            monkeypatch.setattr(M, name, tmp_path / value.name)
    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "ICON_CACHE_DIR", tmp_path / "icon_cache")
    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_ENABLED", True)
    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_INTERVAL_HOURS", 24)
    monkeypatch.setattr(M, "_token_auto_refresh_event", None)
    monkeypatch.setattr(M, "_token_auto_refresh_loop", None)
    monkeypatch.setattr(M, "account_health_check_state", {
        "running": False, "results": {}, "checked": 0, "total": 0,
    })
    monkeypatch.setattr(M, "require_authenticated", lambda *a, **k: None)
    return M


def settings(M, enabled=True, hours=240):
    return {**M.get_default_site_settings(), "token_auto_refresh_enabled": enabled,
            "token_auto_refresh_interval_hours": hours}


def request():
    return Request({"type": "http", "method": "PUT", "scheme": "http",
                    "path": "/api/site-settings", "headers": [(b"host", b"testserver")]})


def test_save_240_and_read_back_with_exact_deadline(isolated):
    M = isolated
    async def scenario():
        before = datetime.utcnow()
        result = await M.update_site_settings(M.SiteSettingsPayload(**settings(M)), request())
        after = datetime.utcnow()
        assert M.load_site_settings()["token_auto_refresh_interval_hours"] == 240
        deadline = M.parse_stored_datetime(result["token_auto_refresh_next_run_at"])
        assert before + timedelta(hours=240) <= deadline <= after + timedelta(hours=240)
        loaded = await M.get_site_settings(request())
        assert loaded["token_auto_refresh_next_run_at"] == result["token_auto_refresh_next_run_at"]
    asyncio.run(scenario())


def test_unrelated_or_identical_save_does_not_postpone(isolated, monkeypatch):
    M = isolated
    calls = []
    monkeypatch.setattr(M, "wake_token_auto_refresh_scheduler", lambda: calls.append(1))
    async def scenario():
        payload = settings(M)
        first = await M.update_site_settings(M.SiteSettingsPayload(**payload), request())
        calls.clear()
        payload["home_title"] = "Updated title"
        second = await M.update_site_settings(M.SiteSettingsPayload(**payload), request())
        third = await M.update_site_settings(M.SiteSettingsPayload(**payload), request())
        assert first["token_auto_refresh_next_run_at"] == second["token_auto_refresh_next_run_at"] == third["token_auto_refresh_next_run_at"]
        assert calls == []
    asyncio.run(scenario())


def test_restart_keeps_deadline_even_when_overdue(isolated):
    M = isolated
    M.save_site_settings(settings(M))
    start = datetime(2026, 1, 1)
    first = M.sync_token_auto_refresh_schedule(now=start)
    later = M.sync_token_auto_refresh_schedule(now=start + timedelta(days=11))
    assert later["next_run_at"] == first["next_run_at"]
    assert M.parse_stored_datetime(later["next_run_at"]) == start + timedelta(hours=240)


def test_interval_change_disable_and_reenable(isolated):
    M = isolated
    start = datetime(2026, 1, 1)
    M.save_site_settings(settings(M, hours=24))
    M.sync_token_auto_refresh_schedule(now=start)
    M.save_site_settings(settings(M))
    changed = M.sync_token_auto_refresh_schedule(now=start + timedelta(hours=1))
    assert M.parse_stored_datetime(changed["next_run_at"]) == start + timedelta(hours=241)
    M.save_site_settings(settings(M, enabled=False))
    assert M.sync_token_auto_refresh_schedule(now=start)["next_run_at"] is None
    M.save_site_settings(settings(M))
    reenabled = M.sync_token_auto_refresh_schedule(now=start + timedelta(days=2))
    assert M.parse_stored_datetime(reenabled["next_run_at"]) == start + timedelta(days=12)


def test_actual_loop_runs_overdue_once_and_updates_task_state(isolated, monkeypatch):
    M = isolated
    M.save_site_settings(settings(M))
    M.sync_token_auto_refresh_schedule(now=datetime.utcnow() - timedelta(days=11))
    calls = []
    monkeypatch.setattr(M, "load_accounts_data", lambda: {"test@example.com": {}})
    async def refresh(email):
        calls.append(email)
        return M.build_account_health_record("healthy", 100, "OK")
    monkeypatch.setattr(M, "refresh_account_health", refresh)
    async def scenario():
        monkeypatch.setattr(M, "_token_auto_refresh_event", asyncio.Event())
        task = asyncio.create_task(M.run_token_auto_refresh_loop())
        try:
            await asyncio.sleep(0)  # 交给真实循环运行到第一次等待
            assert calls == ["test@example.com"]
            assert M.get_account_health_check_state()["checked"] == 1
            assert not M.get_account_health_check_state()["running"]
            assert M.parse_stored_datetime(M.sync_token_auto_refresh_schedule()["next_run_at"]) > datetime.utcnow()
            M.wake_token_auto_refresh_scheduler()
            await asyncio.sleep(0)
            assert len(calls) == 1
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


@pytest.mark.parametrize("initial_enabled", [False, True])
def test_lifespan_starts_even_when_env_default_disabled(isolated, monkeypatch, initial_enabled):
    M = isolated
    monkeypatch.setattr(M, "TOKEN_AUTO_REFRESH_ENABLED", False)
    M.save_site_settings(settings(M, enabled=initial_enabled))
    async def scenario():
        async with M.lifespan(M.app):
            assert M._token_auto_refresh_event is not None
            assert M._token_auto_refresh_loop is asyncio.get_running_loop()
            response = await M.update_site_settings(M.SiteSettingsPayload(**settings(M)), request())
            assert response["token_auto_refresh_next_run_at"]
            assert M.sync_token_auto_refresh_schedule()["enabled"]
        assert M._token_auto_refresh_event is None
    asyncio.run(scenario())


def test_finish_preserves_settings_changed_during_execution(isolated):
    M = isolated
    M.save_site_settings(settings(M, hours=24))
    old = M.sync_token_auto_refresh_schedule()
    M.save_site_settings(settings(M))
    changed = M.sync_token_auto_refresh_schedule()
    M.complete_token_auto_refresh_schedule(old)
    assert M.sync_token_auto_refresh_schedule()["next_run_at"] == changed["next_run_at"]


def test_actual_loop_skips_manual_running_task(isolated, monkeypatch):
    M = isolated
    M.save_site_settings(settings(M))
    M.sync_token_auto_refresh_schedule(now=datetime.utcnow() - timedelta(days=11))
    M.update_account_health_check_state(running=True)
    async def forbidden(*args):
        pytest.fail("manual and automatic full refresh must not overlap")
    monkeypatch.setattr(M, "run_account_health_check_task", forbidden)
    async def scenario():
        monkeypatch.setattr(M, "_token_auto_refresh_event", asyncio.Event())
        task = asyncio.create_task(M.run_token_auto_refresh_loop())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(scenario())


def test_disabled_loop_wakes_and_executes_new_due_schedule(isolated, monkeypatch):
    M = isolated
    M.save_site_settings(settings(M, enabled=False))
    calls = []
    async def refresh(task_id):
        calls.append(task_id)
    monkeypatch.setattr(M, "run_account_health_check_task", refresh)
    async def scenario():
        event = asyncio.Event()
        monkeypatch.setattr(M, "_token_auto_refresh_event", event)
        monkeypatch.setattr(M, "_token_auto_refresh_loop", asyncio.get_running_loop())
        task = asyncio.create_task(M.run_token_auto_refresh_loop())
        try:
            await asyncio.sleep(0)
            assert calls == []
            M.save_site_settings(settings(M))
            M.sync_token_auto_refresh_schedule(now=datetime.utcnow() - timedelta(days=11))
            await asyncio.to_thread(M.wake_token_auto_refresh_scheduler)
            # 有界让出事件循环,等待跨线程通知和 wait_for 的取消收尾。
            for _ in range(20):
                if calls:
                    break
                await asyncio.sleep(0)
            assert len(calls) == 1
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


def test_start_reserves_running_state_before_task_execution(isolated, monkeypatch):
    M = isolated
    entered = asyncio.Event()
    async def worker(task_id):
        entered.set()
    monkeypatch.setattr(M, "run_account_health_check_task", worker)
    async def scenario():
        first = M.start_account_health_check()
        second = M.start_account_health_check()
        assert first["running"] is True
        assert first["task_id"] == second["task_id"]
        await asyncio.sleep(0)
        assert entered.is_set()
    asyncio.run(scenario())


def test_single_retry_persists_success_without_rewriting_old_batch(isolated, monkeypatch):
    M = isolated
    email = "test@example.com"
    old = M.build_account_health_record("auth_error", 20, "Failed", checked_at="2026-01-01T00:00:00")
    M.save_account_health_record(email, old)
    M.update_account_health_check_state(results={email: old})
    async def credentials(*args):
        return object()
    async def evaluate(*args):
        return M.build_account_health_record("healthy", 100, "OK", checked_at="2026-01-02T00:00:00")
    monkeypatch.setattr(M, "get_account_credentials", credentials)
    monkeypatch.setattr(M, "evaluate_account_health", evaluate)
    result = asyncio.run(M.refresh_single_account({"email": email}, request()))
    assert result["record"]["status"] == "healthy"
    assert M.get_account_health_record(email)["status"] == "healthy"
    assert M.get_account_health_check_state()["results"][email]["status"] == "auth_error"
