"""失败后实时取信、单次重试、持久暂停和人工恢复；不访问真实邮箱。"""
import asyncio
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
from starlette.requests import Request

EMAIL = "policy@example.com"
SECURITY_ERROR = "AADSTS70000: Account security interrupt for collecting proof, when user account is found as compromised"


@pytest.fixture
def M(tmp_path, monkeypatch):
    import main as module
    for name, value in list(vars(module).items()):
        if name.endswith("_FILE") and isinstance(value, Path):
            monkeypatch.setattr(module, name, tmp_path / value.name)
    monkeypatch.setattr(module, "_graph_token_scope_cache", {})
    monkeypatch.setattr(module, "_health_check_sequence", 0)
    monkeypatch.setattr(module, "account_health_check_state", {"running": False, "results": {}, "total": 0, "checked": 0})
    monkeypatch.setattr(module, "require_authenticated", lambda *a, **kw: None)
    module.save_accounts_data({EMAIL: {"client_id": "client", "refresh_token": "original", "auth_method": "graph"}})
    return module


def oauth(M, monkeypatch, handler):
    calls = []
    client = httpx.AsyncClient
    async def respond(request):
        data = {key: values[0] for key, values in parse_qs(request.content.decode()).items()}
        calls.append(data)
        return handler(request, data, len(calls))
    monkeypatch.setattr(M.httpx, "AsyncClient", lambda **kw: client(transport=httpx.MockTransport(respond), trust_env=False, **kw))
    return calls


def ok(token="original"):
    return httpx.Response(200, json={"access_token": "access", "refresh_token": token})


def test_refresh_failure_reads_mail_then_retries_with_current_token(M, monkeypatch):
    def response(request, data, number):
        if data.get("scope") == M.GRAPH_DEFAULT_TOKEN_SCOPE and data["refresh_token"] == "original":
            return httpx.Response(400, json={"error": "invalid_grant"})
        return ok("read-rotated" if data["refresh_token"] == "original" else "retry-rotated")
    calls = oauth(M, monkeypatch, response)
    probes = []
    async def graph(access, endpoint, params):
        probes.append((endpoint, params))
        return {"value": []}
    monkeypatch.setattr(M, "graph_api_get", graph)
    record = asyncio.run(M.refresh_account_health(EMAIL))
    assert record["status"] == "healthy" and record["refresh_status"] == "success"
    assert record["refresh_retry_count"] == 1 and record["recovery_attempted"]
    assert record["mailbox_check_status"] == "readable"
    assert not record["manual_review_required"]
    assert len(calls) == 4
    assert calls[-1]["refresh_token"] == "read-rotated"
    assert M.load_accounts_data()[EMAIL]["refresh_token"] == "retry-rotated"
    assert probes == [("/me/mailFolders/inbox/messages", {"$top": 1, "$select": "id,subject"})]
    listed = asyncio.run(M.get_all_accounts()).accounts[0]
    assert listed.recovery_attempted and listed.refresh_retry_count == 1


def test_security_interrupt_pauses_once_and_survives_reload(M, monkeypatch):
    calls = oauth(M, monkeypatch, lambda *a: httpx.Response(400, json={"error": "invalid_grant", "error_description": SECURITY_ERROR}))
    def forbidden_cache(*args):
        pytest.fail("verification must not accept cached messages")
    monkeypatch.setattr(M, "get_cached_emails", forbidden_cache)
    record = asyncio.run(M.refresh_account_health(EMAIL))
    assert record["status"] == "manual_review" and record["manual_review_required"]
    assert record["mailbox_check_status"] == "unreadable" and record["refresh_retry_count"] == 0
    assert "安全验证" in record["manual_review_reason"] and SECURITY_ERROR in record["detail"]
    assert len(calls) == 4  # 不为 AADSTS70000 穷举权限；取信也失败后不重试刷新。
    loaded = M.get_account_health_record(EMAIL)
    assert loaded["manual_review_required"]
    asyncio.run(M.refresh_account_health(EMAIL))
    asyncio.run(M.run_account_health_check_task("scheduled"))
    assert len(calls) == 4
    state = M.get_account_health_check_state()
    assert state["skipped"] == 1 and state["checked"] == 1
    listed = asyncio.run(M.get_all_accounts()).accounts[0]
    assert listed.manual_review_required and "安全验证" in listed.manual_review_reason


def test_70000_security_interrupt_is_not_scope_retryable(M):
    response = httpx.Response(400, json={"error": "invalid_grant", "error_description": SECURITY_ERROR})
    assert not M.is_token_scope_retryable(response)
    assert M.is_token_scope_retryable(httpx.Response(400, json={"error": "invalid_scope"}))


def test_imap_noop_failure_recovers_using_fresh_real_mail_read(M, monkeypatch):
    accounts = M.load_accounts_data()
    accounts[EMAIL]["auth_method"] = "imap"
    M.save_accounts_data(accounts)
    calls = oauth(M, monkeypatch, lambda *a: ok())
    class Connection:
        def __init__(self):
            self.events = []
        def select(self, folder, readonly):
            self.events.append((folder, readonly))
            return "OK", [b"2"]
        def fetch(self, number, query):
            self.events.append((number, query))
            return "OK", [(b"2", b"Subject: hello")]
        def logout(self):
            self.events.append("logout")
    connection = Connection()
    monkeypatch.setattr(M.imap_pool, "get_connection", lambda *a: (_ for _ in ()).throw(RuntimeError("stale connection")))
    monkeypatch.setattr(M.imap_pool, "_create_connection", lambda *a: connection)
    monkeypatch.setattr(M, "get_cached_emails", lambda *a: pytest.fail("must not use cache"))
    record = asyncio.run(M.refresh_account_health(EMAIL))
    assert record["status"] == "healthy" and record["refresh_retry_count"] == 1
    assert not record["manual_review_required"]
    assert len(calls) == 3
    assert connection.events[0] == ('"INBOX"', True)
    assert "BODY.PEEK" in connection.events[1][1]
    assert connection.events[-1] == "logout"


@pytest.mark.parametrize("count,select_status,fetch_status,headers,readable", [
    (b"0", "OK", "OK", [], True),
    (b"1", "NO", "OK", [], False),
    (b"1", "OK", "NO", [], False),
    (b"1", "OK", "OK", [b")"], False),
    (b"bad", "OK", "OK", [], False),
])
def test_imap_empty_mailbox_is_healthy_but_protocol_errors_are_not(M, monkeypatch, count, select_status, fetch_status, headers, readable):
    closed = []
    class Connection:
        def select(self, *a, **kw): return select_status, [count]
        def fetch(self, *a): return fetch_status, headers
        def logout(self): closed.append(True)
    monkeypatch.setattr(M.imap_pool, "_create_connection", lambda *a: Connection())
    creds = M.AccountCredentials(email=EMAIL, client_id="client", refresh_token="original", auth_method="imap")
    if readable:
        M.read_imap_mailbox_for_verification(creds, "access")
    else:
        with pytest.raises(Exception): M.read_imap_mailbox_for_verification(creds, "access")
    assert closed == [True]


def test_readable_but_second_refresh_fails_does_not_loop_or_mark_invalid(M, monkeypatch):
    calls = oauth(M, monkeypatch, lambda request, data, number: httpx.Response(400, json={"error": "invalid_grant"})
                  if data.get("scope") == M.GRAPH_DEFAULT_TOKEN_SCOPE else ok())
    async def graph(*a, **kw): return {"value": [{"id": "mail"}]}
    monkeypatch.setattr(M, "graph_api_get", graph)
    record = asyncio.run(M.refresh_account_health(EMAIL))
    assert record["status"] == "refresh_error" and record["refresh_status"] == "failed"
    assert record["refresh_retry_count"] == 1 and record["mailbox_check_status"] == "readable"
    assert not record["manual_review_required"]
    assert len(calls) == 5
    assert record["last_refresh_success_at"]


def test_manual_recheck_requires_real_read_then_clears_pause(M, monkeypatch):
    M.save_account_health_record(EMAIL, M.mark_mailbox_for_review(M.build_account_health_record("error", 0, "bad"), "previous error"))
    calls = oauth(M, monkeypatch, lambda *a: ok())
    endpoints = []
    async def graph(access, endpoint, params):
        endpoints.append(endpoint)
        return {"value": []} if endpoint.endswith("/messages") else {}
    monkeypatch.setattr(M, "graph_api_get", graph)
    request = Request({"type": "http", "method": "POST", "path": "/accounts/refresh-one", "headers": []})
    result = asyncio.run(M.refresh_single_account({"email": EMAIL, "recheck": True}, request))["record"]
    assert result["status"] == "healthy" and not result["manual_review_required"]
    assert "/me/mailFolders/inbox/messages" in endpoints
    assert len(calls) == 3
    assert not M.get_account_health_record(EMAIL)["manual_review_required"]


def test_failed_manual_recheck_stays_paused(M, monkeypatch):
    M.save_account_health_record(EMAIL, M.mark_mailbox_for_review(M.build_account_health_record("error", 0, "bad"), "previous error"))
    calls = oauth(M, monkeypatch, lambda *a: httpx.Response(400, json={"error": "invalid_grant"}))
    result = asyncio.run(M.refresh_account_health(EMAIL, force=True))
    assert result["manual_review_required"] and len(calls) == 4


def test_batch_skips_paused_account_but_continues_other_accounts(M, monkeypatch):
    M.save_account_health_record(EMAIL, M.mark_mailbox_for_review(M.build_account_health_record("error", 0, "bad"), "manual"))
    accounts = M.load_accounts_data()
    accounts["other@example.com"] = dict(accounts[EMAIL])
    M.save_accounts_data(accounts)
    calls = []
    async def evaluate(creds):
        calls.append(creds.email)
        return {**M.build_account_health_record("healthy", 100, "OK"), "refresh_status": "success"}
    monkeypatch.setattr(M, "evaluate_account_health", evaluate)
    asyncio.run(M.run_account_health_check_task("batch"))
    assert calls == ["other@example.com"]
    assert M.get_account_health_check_state()["skipped"] == 1


def test_graph_read_failure_preserves_oauth_success_but_pauses_mailbox(M, monkeypatch):
    calls = oauth(M, monkeypatch, lambda *a: ok())
    async def graph(*a, **kw): raise M.HTTPException(status_code=403, detail="mailbox denied")
    monkeypatch.setattr(M, "graph_api_get", graph)
    record = asyncio.run(M.refresh_account_health(EMAIL))
    assert record["refresh_status"] == "success" and record["manual_review_required"]
    assert len(calls) == 2 and record["refresh_retry_count"] == 0


def test_invalid_graph_payload_is_not_treated_as_empty_mailbox(M, monkeypatch):
    oauth(M, monkeypatch, lambda *a: ok())
    async def graph(*a, **kw): return {}
    monkeypatch.setattr(M, "graph_api_get", graph)
    creds = asyncio.run(M.get_account_credentials(EMAIL))
    result = asyncio.run(M.verify_mailbox_readability(creds))
    assert not result["readable"] and "无效" in result["detail"]


def test_broken_account_config_is_paused_and_skipped(M):
    M.save_accounts_data({EMAIL: {"auth_method": "imap"}})
    result = asyncio.run(M.refresh_account_health(EMAIL))
    assert result["manual_review_required"]
    assert asyncio.run(M.refresh_account_health(EMAIL))["manual_review_required"]


def test_stale_bulk_retry_cannot_bypass_manual_pause(M, monkeypatch):
    M.save_account_health_record(EMAIL, M.mark_mailbox_for_review(M.build_account_health_record("error", 0, "bad"), "manual"))
    async def forbidden(*a, **kw):
        pytest.fail("paused account must not automatically retry")
    monkeypatch.setattr(M, "evaluate_account_health", forbidden)
    request = Request({"type": "http", "method": "POST", "path": "/accounts/refresh-one", "headers": []})
    result = asyncio.run(M.refresh_single_account({"email": EMAIL}, request))
    assert result["record"]["manual_review_required"]


def test_network_failure_is_paused_for_review_without_claiming_security_interrupt(M, monkeypatch):
    def response(request, data, number):
        raise httpx.ReadTimeout("connection timed out", request=request)
    calls = oauth(M, monkeypatch, response)
    result = asyncio.run(M.refresh_account_health(EMAIL))
    assert len(calls) == 2  # 首次失败 + 实时取信复核；没有第三次盲重试。
    assert result["manual_review_required"] and result["mailbox_check_status"] == "unreadable"
    assert "Network error" in result["detail"] and "账号安全验证" not in result["detail"]


def test_stop_during_recovery_finishes_current_account_only(M, monkeypatch):
    accounts = M.load_accounts_data()
    accounts["second@example.com"] = dict(accounts[EMAIL])
    M.save_accounts_data(accounts)
    calls = oauth(M, monkeypatch, lambda *a: httpx.Response(400, json={"error": "invalid_grant"}))
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        async def verify(creds):
            entered.set()
            await release.wait()
            return {"readable": False, "detail": SECURITY_ERROR, "checked_at": M.datetime.utcnow().isoformat()}
        monkeypatch.setattr(M, "verify_mailbox_readability", verify)
        task = asyncio.create_task(M.run_account_health_check_task("recover-stop"))
        async with asyncio.timeout(3):
            await entered.wait()
            M.request_account_health_check_stop("recover-stop")
            release.set()
            await task
        assert M.get_account_health_check_state()["checked"] == 1
        assert M.get_account_health_check_state()["stopped"]
        assert M.get_account_health_record(EMAIL)["manual_review_required"]
        assert not M.get_account_health_record("second@example.com")["manual_review_required"]
        assert len(calls) == 2
    asyncio.run(scenario())


def test_changed_credentials_during_verification_do_not_get_old_pause(M, monkeypatch):
    oauth(M, monkeypatch, lambda *a: httpx.Response(400, json={"error": "invalid_grant"}))
    async def verify(creds):
        accounts = M.load_accounts_data()
        accounts[EMAIL]["refresh_token"] = "admin-replaced"
        M.save_accounts_data(accounts)
        M.save_account_health_record(EMAIL, {**M.build_account_health_record("healthy", 100, "new credentials"),
            "refresh_status": "success", "refresh_sequence": M.next_health_check_sequence(EMAIL)})
        return {"readable": False, "detail": SECURITY_ERROR, "checked_at": M.datetime.utcnow().isoformat()}
    monkeypatch.setattr(M, "verify_mailbox_readability", verify)
    result = asyncio.run(M.refresh_account_health(EMAIL))
    assert result["status"] == "healthy" and not result["manual_review_required"]
    assert M.load_accounts_data()[EMAIL]["refresh_token"] == "admin-replaced"
