"""并发凭据提交、结果顺序和协作停止的行为回归。"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

import httpx
import pytest
from starlette.requests import Request

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="refresh_review_")
EMAIL = "review@example.com"


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    import main as M
    for name, value in list(vars(M).items()):
        if name.endswith("_FILE") and isinstance(value, Path):
            monkeypatch.setattr(M, name, tmp_path / value.name)
    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "_graph_token_scope_cache", {})
    monkeypatch.setattr(M, "require_authenticated", lambda *a, **k: None)
    monkeypatch.setattr(M, "_health_check_sequence", 0)
    monkeypatch.setattr(M, "account_health_check_state", {"running": False, "results": {}, "checked": 0, "total": 0})
    M.save_accounts_data({EMAIL: {"client_id": "test-client", "refresh_token": "initial-token", "auth_method": "graph", "category_key": None, "tag_keys": []}})
    return M


def credentials(M):
    return M.AccountCredentials(email=EMAIL, client_id="test-client", refresh_token="initial-token", auth_method="graph")


def mock_oauth(M, monkeypatch, handler):
    client = httpx.AsyncClient
    monkeypatch.setattr(M.httpx, "AsyncClient", lambda **kw: client(transport=httpx.MockTransport(handler), trust_env=False, **kw))


def test_import_cannot_restore_token_older_than_completed_background_refresh(app_module, monkeypatch):
    M = app_module
    async def scenario():
        validation_waiting = asyncio.Event()
        finish_validation = asyncio.Event()
        async def oauth(request):
            importing = asyncio.current_task().get_name() == "importing"
            return httpx.Response(200, json={"access_token": "import-access" if importing else "refresh-access", "refresh_token": "initial-token" if importing else "latest-rotated-token"})
        async def graph(access_token, *args, **kwargs):
            if access_token == "import-access":
                validation_waiting.set()
                await finish_validation.wait()
            return {}
        mock_oauth(M, monkeypatch, oauth)
        monkeypatch.setattr(M, "graph_api_get", graph)
        request = Request({"type": "http", "method": "POST", "path": "/accounts", "headers": []})
        importing = asyncio.create_task(M.register_account(credentials(M), request), name="importing")
        async with asyncio.timeout(3):
            await validation_waiting.wait()
            refreshed = await M.refresh_account_health(EMAIL)
            assert refreshed["token_rotated"] is True
            assert M.load_accounts_data()[EMAIL]["refresh_token"] == "latest-rotated-token"
            finish_validation.set()
            with pytest.raises(M.HTTPException) as error:
                await importing
            assert error.value.status_code == 409
        assert M.load_accounts_data()[EMAIL]["refresh_token"] == "latest-rotated-token", "delayed import replaced the newly saved token with an older one"
    asyncio.run(scenario())


def test_old_failed_request_cannot_overwrite_new_success(app_module, monkeypatch):
    M = app_module
    async def scenario():
        old_request_waiting = asyncio.Event()
        finish_old_request = asyncio.Event()
        async def oauth(request):
            if asyncio.current_task().get_name() == "older-request":
                old_request_waiting.set()
                await finish_old_request.wait()
                return httpx.Response(400, json={"error": "invalid_grant", "error_description": "old credential response"})
            return httpx.Response(200, json={"access_token": "working-access", "refresh_token": "latest-rotated-token"})
        async def graph(*a, **kw):
            return {}
        mock_oauth(M, monkeypatch, oauth)
        monkeypatch.setattr(M, "graph_api_get", graph)
        old = asyncio.create_task(M.refresh_account_health(EMAIL), name="older-request")
        async with asyncio.timeout(3):
            await old_request_waiting.wait()
            newer = await M.refresh_account_health(EMAIL)
            assert newer["refresh_status"] == "success"
            finish_old_request.set()
            await old
        assert M.load_accounts_data()[EMAIL]["refresh_token"] == "latest-rotated-token"
        stored = M.get_account_health_record(EMAIL)
        assert stored["refresh_status"] == "success", "late old response changed a newer success into failure"
        assert stored["last_refresh_at"] == newer["last_refresh_at"]
    asyncio.run(scenario())


def test_late_health_probe_cannot_move_success_time_backwards(app_module, monkeypatch):
    M = app_module
    from datetime import timedelta
    class Clock(M.datetime):
        value = M.datetime(2026, 11, 9, 0, 0, 0)
        @classmethod
        def utcnow(cls):
            cls.value += timedelta(milliseconds=1)
            return cls.value
    monkeypatch.setattr(M, "datetime", Clock)
    async def scenario():
        probe_waiting = asyncio.Event()
        finish_probe = asyncio.Event()
        async def oauth(request):
            return httpx.Response(200, json={"access_token": "working-access", "refresh_token": "initial-token"})
        async def graph(*a, **kw):
            if asyncio.current_task().get_name() == "older-probe":
                probe_waiting.set()
                await finish_probe.wait()
            return {}
        mock_oauth(M, monkeypatch, oauth)
        monkeypatch.setattr(M, "graph_api_get", graph)
        old = asyncio.create_task(M.refresh_account_health(EMAIL), name="older-probe")
        async with asyncio.timeout(3):
            await probe_waiting.wait()
            newer = await M.refresh_account_health(EMAIL)
            finish_probe.set()
            await old
        stored = M.get_account_health_record(EMAIL)
        assert stored["last_refresh_success_at"] >= newer["last_refresh_success_at"], "last success timestamp went backwards"
    asyncio.run(scenario())


def test_deliberate_reimport_replaces_credentials_when_snapshot_unchanged(app_module, monkeypatch):
    M = app_module
    calls = []
    async def oauth(request):
        calls.append(request)
        return httpx.Response(200, json={"access_token": "new-access", "refresh_token": "new-import-rotated"})
    async def graph(*a, **kw):
        return {}
    mock_oauth(M, monkeypatch, oauth)
    monkeypatch.setattr(M, "graph_api_get", graph)
    creds = credentials(M)
    creds.refresh_token = "new-import-token"
    creds.client_id = "new-client"
    request = Request({"type": "http", "method": "POST", "path": "/accounts", "headers": []})
    asyncio.run(M.register_account(creds, request))
    stored = M.load_accounts_data()[EMAIL]
    assert stored["refresh_token"] == "new-import-rotated"
    assert stored["client_id"] == "new-client"
    assert stored["credential_revision"]
    assert len(calls) == 1


def test_delete_during_import_does_not_recreate_account(app_module, monkeypatch):
    M = app_module
    async def oauth(request):
        return httpx.Response(200, json={"access_token": "access", "refresh_token": "initial-token"})
    async def graph(*a, **kw):
        M.save_accounts_data({})
        M.remove_account_health_record(EMAIL)
        return {}
    mock_oauth(M, monkeypatch, oauth)
    monkeypatch.setattr(M, "graph_api_get", graph)
    request = Request({"type": "http", "method": "POST", "path": "/accounts", "headers": []})
    with pytest.raises(M.HTTPException) as error:
        asyncio.run(M.register_account(credentials(M), request))
    assert error.value.status_code == 409
    assert M.load_accounts_data() == {}
    assert EMAIL not in M.load_account_health_data()["accounts"]


def test_revision_blocks_aba_and_client_only_changes(app_module):
    M = app_module
    async def scenario():
        stale = await M.get_account_credentials(EMAIL)
        original_version = stale._source_version
        newer = await M.get_account_credentials(EMAIL)
        # 管理员重新保存同一 token，也产生新版本。
        await M.save_account_credentials(EMAIL, newer)
        assert M.account_credential_version(M.load_accounts_data()[EMAIL]) != original_version
        assert not await M.persist_rotated_refresh_token(EMAIL, "stale-rotated", "initial-token", expected_version=original_version)
        with pytest.raises(M.HTTPException) as error:
            await M.save_account_credentials(EMAIL, stale)
        assert error.value.status_code == 409
        # 即使磁盘 token 没变，client_id 改变仍应拒绝旧轮换。
        current = await M.get_account_credentials(EMAIL)
        accounts = M.load_accounts_data()
        accounts[EMAIL]["client_id"] = "different-client"
        M.save_accounts_data(accounts)
        assert not await M.persist_rotated_refresh_token(EMAIL, "stale-rotated", "initial-token", expected_version=current._source_version)
    asyncio.run(scenario())


def test_deleted_account_cannot_be_recreated_by_late_health_record(app_module, monkeypatch):
    M = app_module
    async def oauth(request):
        return httpx.Response(200, json={"access_token": "access", "refresh_token": "initial-token"})
    async def graph(*a, **kw):
        M.save_accounts_data({})
        M.remove_account_health_record(EMAIL)
        return {}
    mock_oauth(M, monkeypatch, oauth)
    monkeypatch.setattr(M, "graph_api_get", graph)
    asyncio.run(M.refresh_account_health(EMAIL))
    assert EMAIL not in M.load_account_health_data()["accounts"]


def test_stale_result_preserves_new_status_and_monotonic_times(app_module):
    M = app_module
    newer = {**M.build_account_health_record("auth_error", 20, "newer failure"),
             "refresh_status": "failed", "refresh_sequence": 12,
             "last_refresh_success_at": "2026-11-09T00:00:04", "refresh_token_updated_at": "2026-11-09T00:00:04"}
    M.save_account_health_record(EMAIL, newer)
    older = {**M.build_account_health_record("healthy", 100, "old success"),
             "refresh_status": "success", "refresh_sequence": 11,
             "last_refresh_success_at": "2026-11-09T00:00:02", "refresh_token_updated_at": "2026-11-09T00:00:02"}
    effective = M.save_account_health_record(EMAIL, older)
    assert effective["refresh_status"] == "failed"
    assert effective["last_refresh_success_at"] == "2026-11-09T00:00:04"
    assert effective["refresh_token_updated_at"] == "2026-11-09T00:00:04"
    M._health_check_sequence = 0
    assert M.next_health_check_sequence(EMAIL) == 13


def seed_two(M):
    accounts = M.load_accounts_data()
    accounts["second@example.com"] = dict(accounts[EMAIL])
    M.save_accounts_data(accounts)


def test_stop_waits_for_current_token_save_then_skips_next_account(app_module, monkeypatch):
    M = app_module
    seed_two(M)
    attempts = []
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        async def oauth(request):
            attempts.append(request)
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"access_token": "working", "refresh_token": "saved-before-stop"})
        async def graph(*a, **kw):
            return {}
        mock_oauth(M, monkeypatch, oauth)
        monkeypatch.setattr(M, "graph_api_get", graph)
        task = asyncio.create_task(M.run_account_health_check_task("task-one"))
        async with asyncio.timeout(3):
            await entered.wait()
            requested = M.request_account_health_check_stop("task-one")
            assert requested["running"] and requested["stop_requested"]
            # 再次停止幂等；启动按钮不能在停止中创建另一轮。
            assert M.request_account_health_check_stop("task-one")["task_id"] == "task-one"
            assert M.start_account_health_check()["task_id"] == "task-one"
            assert M.load_accounts_data()[EMAIL]["refresh_token"] == "initial-token"
            release.set()
            await task
        state = M.get_account_health_check_state()
        assert not state["running"] and state["stopped"] and not state["stop_requested"]
        assert state["checked"] == 1 and state["total"] == 2
        assert len(attempts) == 1
        assert M.load_accounts_data()[EMAIL]["refresh_token"] == "saved-before-stop"
        assert M.load_accounts_data()["second@example.com"]["refresh_token"] == "initial-token"
    asyncio.run(scenario())


def test_immediate_stop_before_worker_starts_and_restart(app_module, monkeypatch):
    M = app_module
    seed_two(M)
    calls = []
    async def refresh(email):
        calls.append(email)
        return M.build_account_health_record("healthy", 100, "OK")
    monkeypatch.setattr(M, "refresh_account_health", refresh)
    async def scenario():
        first = M.start_account_health_check()
        M.request_account_health_check_stop(first["task_id"])
        await asyncio.sleep(0)
        state = M.get_account_health_check_state()
        assert calls == [] and state["stopped"] and state["checked"] == 0
        second = M.start_account_health_check()
        assert second["task_id"] != first["task_id"] and not second["stop_requested"]
        await asyncio.sleep(0)
        assert calls == [EMAIL, "second@example.com"]
        assert not M.get_account_health_check_state()["stopped"]
    asyncio.run(scenario())


def test_stop_old_task_does_not_stop_new_task_and_requires_auth(app_module, monkeypatch):
    M = app_module
    request = Request({"type": "http", "method": "POST", "path": "/accounts/health-check/stop", "headers": []})
    M.update_account_health_check_state(task_id="current-task", running=True, stop_requested=False)
    with pytest.raises(M.HTTPException) as error:
        asyncio.run(M.stop_accounts_health_check({"task_id": "previous-task"}, request))
    assert error.value.status_code == 409
    assert not M.get_account_health_check_state()["stop_requested"]
    def unauthorized(*a, **kw):
        raise M.HTTPException(status_code=401, detail="unauthorized")
    monkeypatch.setattr(M, "require_authenticated", unauthorized)
    with pytest.raises(M.HTTPException) as error:
        asyncio.run(M.stop_accounts_health_check({"task_id": "current-task"}, request))
    assert error.value.status_code == 401
    assert not M.get_account_health_check_state()["stop_requested"]


def test_stopped_scheduled_run_advances_deadline_instead_of_restarting(app_module, monkeypatch):
    from datetime import timedelta
    M = app_module
    seed_two(M)
    M.save_site_settings({**M.get_default_site_settings(), "token_auto_refresh_enabled": True, "token_auto_refresh_interval_hours": 24})
    M.sync_token_auto_refresh_schedule(now=M.datetime.utcnow() - timedelta(days=2))
    calls = []
    async def refresh(email):
        calls.append(email)
        M.request_account_health_check_stop(M.get_account_health_check_state()["task_id"])
        return M.build_account_health_record("healthy", 100, "OK")
    monkeypatch.setattr(M, "refresh_account_health", refresh)
    async def scenario():
        monkeypatch.setattr(M, "_token_auto_refresh_event", asyncio.Event())
        task = asyncio.create_task(M.run_token_auto_refresh_loop())
        try:
            await asyncio.sleep(0)
            assert calls == [EMAIL]
            assert M.get_account_health_check_state()["stopped"]
            assert M.parse_stored_datetime(M.sync_token_auto_refresh_schedule()["next_run_at"]) > M.datetime.utcnow()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


def test_stop_http_endpoint_enforces_auth_and_returns_partial_state(app_module, monkeypatch):
    M = app_module
    M.update_account_health_check_state(task_id="http-run", running=True, total=3, checked=1, stop_requested=False)
    authenticated = False
    def auth(*a, **kw):
        if not authenticated:
            raise M.HTTPException(status_code=401, detail="unauthorized")
    monkeypatch.setattr(M, "require_authenticated", auth)
    async def scenario():
        nonlocal authenticated
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=M.app), base_url="http://testserver") as client:
            denied = await client.post("/accounts/health-check/stop", json={"task_id": "http-run"})
            assert denied.status_code == 401
            assert not M.get_account_health_check_state()["stop_requested"]
            authenticated = True
            accepted = await client.post("/accounts/health-check/stop", json={"task_id": "http-run"})
            assert accepted.status_code == 200
            body = accepted.json()
            assert body["task_id"] == "http-run" and body["stop_requested"] and body["running"]
            assert body["checked"] == 1 and body["total"] == 3
            wrong = await client.post("/accounts/health-check/stop", json={"task_id": "old-run"})
            assert wrong.status_code == 409
    asyncio.run(scenario())


def test_stop_on_final_account_reports_completed_and_idle_stop_is_noop(app_module, monkeypatch):
    M = app_module
    async def refresh(email):
        M.request_account_health_check_stop("single-run")
        return M.build_account_health_record("healthy", 100, "OK")
    monkeypatch.setattr(M, "refresh_account_health", refresh)
    asyncio.run(M.run_account_health_check_task("single-run"))
    state = M.get_account_health_check_state()
    assert state["checked"] == state["total"] == 1
    assert not state["running"] and not state["stopped"] and not state["stop_requested"]
    assert M.request_account_health_check_stop("single-run") == state
