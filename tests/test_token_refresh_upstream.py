"""移植刷新轮换语义与 Graph scope 优选的行为回归；不访问真实微软账户。"""
import asyncio
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
from starlette.requests import Request


@pytest.fixture
def app_module(tmp_path, monkeypatch):
    import main as M
    for name, value in list(vars(M).items()):
        if name.endswith("_FILE") and isinstance(value, Path):
            monkeypatch.setattr(M, name, tmp_path / value.name)
    monkeypatch.setattr(M, "_graph_token_scope_cache", {})
    monkeypatch.setattr(M, "require_authenticated", lambda *a, **k: None)
    M.save_accounts_data({"one@outlook.com": {
        "client_id": "client", "refresh_token": "original", "auth_method": "graph",
        "category_key": None, "tag_keys": [], "custom_field": "preserve",
    }})
    return M


def credentials(M, **overrides):
    return M.AccountCredentials(**{
        "email": "one@outlook.com", "client_id": "client", "refresh_token": "original",
        "auth_method": "graph", **overrides,
    })


def mock_oauth(M, monkeypatch, handler):
    calls = []
    real_client = httpx.AsyncClient
    def transport(request):
        data = {key: value[0] for key, value in parse_qs(request.content.decode()).items()}
        calls.append((str(request.url), data))
        return handler(request, data, len(calls))
    monkeypatch.setattr(M.httpx, "AsyncClient", lambda **kwargs: real_client(
        transport=httpx.MockTransport(transport), trust_env=False, **kwargs))
    return calls


def ok(payload):
    return httpx.Response(200, json=payload)


def test_rotation_is_persisted_reported_and_visible_without_secrets(app_module, monkeypatch):
    M = app_module
    mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "access-secret", "refresh_token": "rotated-secret"}))
    async def graph(*args, **kwargs):
        return {}
    monkeypatch.setattr(M, "graph_api_get", graph)
    record = asyncio.run(M.refresh_account_health("one@outlook.com"))
    assert record["token_rotated"] is True
    assert record["refresh_status"] == "success"
    assert record["last_refresh_at"] <= record["last_refresh_success_at"]
    account = M.load_accounts_data()["one@outlook.com"]
    assert account["refresh_token"] == "rotated-secret"
    assert account["custom_field"] == "preserve"
    assert account["refresh_token_updated_at"]
    listed = asyncio.run(M.get_all_accounts()).model_dump()
    assert listed["accounts"][0]["token_rotated"] is True
    assert listed["accounts"][0]["refresh_token_updated_at"] == account["refresh_token_updated_at"]
    assert "access-secret" not in str(listed) and "rotated-secret" not in str(listed)
    assert M.get_account_health_record("one@outlook.com")["refresh_message"] == "刷新成功，Token 已更新"


@pytest.mark.parametrize("token", [None, "", "   ", 123, True, {}, [], "original"])
def test_absent_invalid_or_unchanged_refresh_token_preserves_disk(app_module, monkeypatch, token):
    M = app_module
    mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "access", "refresh_token": token}))
    creds = credentials(M)
    assert asyncio.run(M.get_access_token(creds, prefer_rotation=True)) == "access"
    assert M.load_accounts_data()[creds.email]["refresh_token"] == "original"
    assert creds._refresh_result["token_rotated"] is False
    assert "未轮换" in creds._refresh_result["refresh_message"]
    assert "refresh_token_updated_at" not in creds._refresh_result


@pytest.mark.parametrize("fallback", ["http_error", "timeout", "invalid_json", "no_access", "no_rotation"])
def test_success_is_kept_when_original_scope_fallback_fails(app_module, monkeypatch, fallback):
    M = app_module
    def respond(request, data, index):
        if index == 1:
            assert data["scope"] == M.GRAPH_DEFAULT_TOKEN_SCOPE
            return ok({"access_token": "usable"})
        assert "scope" not in data
        if fallback == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        if fallback == "http_error":
            return httpx.Response(400, json={"error": "invalid_grant"})
        if fallback == "invalid_json":
            return httpx.Response(200, text="not json")
        return ok({} if fallback == "no_access" else {"access_token": "fallback"})
    calls = mock_oauth(M, monkeypatch, respond)
    creds = credentials(M)
    assert asyncio.run(M.get_access_token(creds, prefer_rotation=True)) == "usable"
    assert len(calls) == 2
    assert creds._refresh_result["refresh_status"] == "success"


def test_original_scope_can_supply_rotation(app_module, monkeypatch):
    M = app_module
    calls = mock_oauth(M, monkeypatch, lambda request, data, index: ok(
        {"access_token": "first"} if index == 1 else {"access_token": "second", "refresh_token": "next"}))
    creds = credentials(M)
    assert asyncio.run(M.get_access_token(creds, prefer_rotation=True)) == "second"
    assert len(calls) == 2 and "scope" not in calls[1][1]
    assert M.load_accounts_data()[creds.email]["refresh_token"] == "next"


def test_scope_cache_survives_rotation_and_isolated_from_refresh_mode(app_module, monkeypatch):
    M = app_module
    def respond(request, data, index):
        if data.get("scope") == M.GRAPH_OAUTH_SCOPE:
            return httpx.Response(400, json={"error": "invalid_scope"})
        return ok({"access_token": "access", "refresh_token": "rotated"})
    calls = mock_oauth(M, monkeypatch, respond)
    creds = credentials(M)
    asyncio.run(M.get_access_token(creds))
    assert len(calls) == 2
    loaded = asyncio.run(M.get_account_credentials(creds.email))
    asyncio.run(M.get_access_token(loaded))
    assert len(calls) == 3 and calls[2][1]["scope"] == calls[1][1]["scope"]
    asyncio.run(M.get_access_token(loaded, prefer_rotation=True))
    assert calls[3][1]["scope"] == M.GRAPH_DEFAULT_TOKEN_SCOPE
    assert all("original" not in key and "rotated" not in key for key in M._graph_token_scope_cache)
    assert all(len(key) == 64 for key in M._graph_token_scope_cache)
    # 重新导入不同令牌不得沿用旧凭据的缓存。
    asyncio.run(M.get_access_token(credentials(M, refresh_token="reimported"), persist_rotation=False))
    assert calls[4][1]["scope"] == M.GRAPH_OAUTH_SCOPE


def test_scope_cache_is_bounded(app_module, monkeypatch):
    M = app_module
    monkeypatch.setattr(M, "GRAPH_TOKEN_SCOPE_CACHE_LIMIT", 2)
    creds = credentials(M)
    for token in ("a", "b", "c"):
        M.remember_graph_token_scope(creds, token, M.TOKEN_URL, M.GRAPH_OAUTH_SCOPE)
    assert len(M._graph_token_scope_cache) == 2
    assert M.graph_token_cache_key("client", "a") not in M._graph_token_scope_cache


@pytest.mark.parametrize("failure", ["write", "conflict", "deleted"])
def test_persistence_failure_is_not_reported_as_success(app_module, monkeypatch, failure):
    M = app_module
    def respond(*args):
        if failure == "conflict":
            accounts = M.load_accounts_data()
            accounts["one@outlook.com"]["refresh_token"] = "admin-updated"
            M.save_accounts_data(accounts)
        elif failure == "deleted":
            M.save_accounts_data({})
        return ok({"access_token": "access", "refresh_token": "new"})
    mock_oauth(M, monkeypatch, respond)
    if failure == "write":
        async def fail(*a, **k):
            return False
        monkeypatch.setattr(M, "persist_rotated_refresh_token", fail)
    creds = credentials(M)
    with pytest.raises(M.HTTPException, match="保存失败"):
        asyncio.run(M.get_access_token(creds, prefer_rotation=True))
    assert creds._refresh_result["refresh_status"] == "failed"
    assert creds._refresh_result["token_rotated"] is False
    assert "last_refresh_success_at" not in creds._refresh_result
    assert creds.refresh_token == "original"
    stored = M.load_accounts_data()
    assert not stored if failure == "deleted" else stored[creds.email]["refresh_token"] != "new"


def test_mailbox_failure_does_not_hide_successful_token_refresh(app_module, monkeypatch):
    M = app_module
    mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "access", "refresh_token": "original"}))
    async def fail_graph(*args, **kwargs):
        raise M.HTTPException(status_code=403, detail="mailbox unavailable")
    monkeypatch.setattr(M, "graph_api_get", fail_graph)
    record = asyncio.run(M.refresh_account_health("one@outlook.com"))
    assert record["status"] == "manual_review" and record["score"] == 0
    assert record["manual_review_required"]
    assert record["mailbox_check_status"] == "unreadable"
    assert record["refresh_status"] == "success" and record["token_rotated"] is False
    listed = asyncio.run(M.get_all_accounts()).accounts[0]
    assert "mailbox unavailable" in listed.health_detail
    assert listed.manual_review_required
    assert listed.refresh_status == "success"


def test_failure_preserves_previous_success_and_rotation_times(app_module, monkeypatch):
    M = app_module
    M.save_account_health_record("one@outlook.com", {
        **M.build_account_health_record("healthy", 100, "OK"),
        "last_refresh_success_at": "2026-01-01T01:00:00",
        "refresh_token_updated_at": "2026-01-01T00:00:00",
    })
    calls = mock_oauth(M, monkeypatch, lambda *a: httpx.Response(400, json={"error": "invalid_grant"}))
    record = asyncio.run(M.refresh_account_health("one@outlook.com"))
    assert len(calls) == 4  # 首次刷新和取信复核各两个端点；取信失败后不再重试。
    assert record["manual_review_required"]
    assert record["refresh_status"] == "failed"
    assert record["last_refresh_success_at"] == "2026-01-01T01:00:00"
    assert record["refresh_token_updated_at"] == "2026-01-01T00:00:00"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_transient_service_error_stops_scope_retries(app_module, monkeypatch, status):
    M = app_module
    calls = mock_oauth(M, monkeypatch, lambda *a: httpx.Response(status, json={"error": "temporary"}))
    with pytest.raises(M.HTTPException) as exc:
        asyncio.run(M.get_access_token(credentials(M)))
    assert exc.value.status_code == 503
    assert len(calls) == 1


def test_registration_keeps_token_rotated_during_validation(app_module, monkeypatch):
    M = app_module
    M.save_accounts_data({})
    mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "access", "refresh_token": "rotated-on-registration"}))
    async def graph(*a, **k):
        return {}
    monkeypatch.setattr(M, "graph_api_get", graph)
    request = Request({"type": "http", "method": "POST", "path": "/accounts", "headers": []})
    asyncio.run(M.register_account(credentials(M), request))
    stored = M.load_accounts_data()["one@outlook.com"]
    assert stored["refresh_token"] == "rotated-on-registration"
    assert stored["refresh_token_updated_at"]
    assert M.get_account_health_record("one@outlook.com")["token_rotated"] is True


def test_legacy_health_record_keeps_rotation_unknown(app_module):
    M = app_module
    M.save_account_health_record("one@outlook.com", M.build_account_health_record("healthy", 100, "OK"))
    listed = asyncio.run(M.get_all_accounts()).accounts[0]
    assert listed.token_rotated is None
    assert listed.refresh_status is None
    assert listed.last_refresh_success_at is None


def test_normal_mail_access_remains_usable_when_rotation_cannot_be_saved(app_module, monkeypatch):
    M = app_module
    mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "usable", "refresh_token": "rotated"}))
    async def fail(*a, **k):
        return False
    monkeypatch.setattr(M, "persist_rotated_refresh_token", fail)
    creds = credentials(M)
    assert asyncio.run(M.get_access_token(creds)) == "usable"
    assert creds.refresh_token == "original"
    assert creds._refresh_result["refresh_status"] == "failed"


def test_registration_validation_uses_mail_scopes_not_rotation_only_scopes(app_module, monkeypatch):
    M = app_module
    calls = mock_oauth(M, monkeypatch, lambda *a: ok({"access_token": "access", "refresh_token": "original"}))
    async def graph(*a, **k):
        return {}
    monkeypatch.setattr(M, "graph_api_get", graph)
    asyncio.run(M.validate_account_credentials(credentials(M)))
    assert calls[0][1]["scope"] == M.GRAPH_OAUTH_SCOPE
