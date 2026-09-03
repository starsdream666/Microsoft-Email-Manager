# -*- coding: utf-8 -*-
"""P2-1 / P2-4 验收:CAS 轮换回存防覆盖、导出仅 session + 审计 + 字段过滤。"""
import json

import pytest


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    import main as M

    monkeypatch.setattr(M, "DATA_DIR", tmp_path)
    monkeypatch.setattr(M, "SITE_SETTINGS_FILE", tmp_path / "site_settings.json")
    monkeypatch.setattr(M, "ACCOUNTS_FILE", tmp_path / "accounts.json")
    monkeypatch.setattr(M, "AUTH_FILE", tmp_path / "auth.json")
    monkeypatch.setattr(M, "SESSIONS_FILE", tmp_path / "sessions.json")
    monkeypatch.setattr(M, "API_KEYS_FILE", tmp_path / "api_keys.json")
    return tmp_path


def _seed_account(email="a@outlook.com", rt="OLD_RT_123", client_id="11111111-2222-3333-4444-555555555555"):
    import main as M

    M._write_json_file(M.ACCOUNTS_FILE, {
        email: {
            "refresh_token": rt,
            "client_id": client_id,
            "auth_method": "imap",
            "category_key": None,
            "tag_keys": [],
        }
    })


def test_normal_rotation_persists(_isolated):
    """正常 OAuth 轮换(expected=磁盘当前值)应写回新 token。"""
    import main as M
    import asyncio

    _seed_account(rt="OLD_RT_123")
    ok = asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "NEW_RT_456", "OLD_RT_123"))
    assert ok is True
    accounts = M.load_accounts_data()
    assert accounts["a@outlook.com"]["refresh_token"] == "NEW_RT_456"
    # 其它字段保留
    assert accounts["a@outlook.com"]["client_id"] == "11111111-2222-3333-4444-555555555555"
    assert accounts["a@outlook.com"]["auth_method"] == "imap"


def test_conflict_does_not_overwrite(_isolated):
    """OAuth 期间管理员已更新凭据(expected 过期):不得覆盖新 token。"""
    import main as M
    import asyncio

    _seed_account(rt="OLD_RT_123")
    # 模拟管理员在请求期间保存了新 token
    _seed_account(rt="ADMIN_NEW_RT_789")
    ok = asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "STALE_ROTATED_000", "OLD_RT_123"))
    assert ok is False
    accounts = M.load_accounts_data()
    assert accounts["a@outlook.com"]["refresh_token"] == "ADMIN_NEW_RT_789"  # 未被旧响应覆盖


def test_missing_account_no_recreate(_isolated):
    """OAuth 期间账号被删:回存不得重建账号。"""
    import main as M
    import asyncio

    _seed_account(rt="OLD_RT_123")
    M._write_json_file(M.ACCOUNTS_FILE, {})  # 删除
    ok = asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "NEW_RT_456", "OLD_RT_123"))
    assert ok is False
    assert M.load_accounts_data() == {}


def test_idempotent_cases(_isolated):
    """空 token / 新旧相同 / expected 为空(旧调用方)应幂等不抛。"""
    import main as M
    import asyncio

    _seed_account(rt="SAME_RT")
    # 空新 token
    assert asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "", "SAME_RT")) is False
    # 新旧相同
    assert asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "SAME_RT", "SAME_RT")) is False
    # expected 为空字符串 → 视为旧调用方,不做 CAS 直接写(向后兼容)
    ok = asyncio.run(M.persist_rotated_refresh_token("a@outlook.com", "NEW_RT_2", ""))
    assert ok is True
    assert M.load_accounts_data()["a@outlook.com"]["refresh_token"] == "NEW_RT_2"


def test_get_access_token_passes_expected(_isolated, monkeypatch):
    """get_access_token 调用回存时传入 credentials.refresh_token 作为 expected。"""
    import main as M

    captured = {}

    async def fake_post(url, data):
        class Resp:
            status_code = 200
            is_success = True

            def json(self):
                return {"access_token": "AT_123", "refresh_token": "ROTATED_NEW"}

        return Resp()

    async def fake_persist(email_id, new_rt, expected_current_refresh_token=""):
        captured["email"] = email_id
        captured["new_rt"] = new_rt
        captured["expected"] = expected_current_refresh_token
        return True

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, data):
            return await fake_post(url, data)

    monkeypatch.setattr(M.httpx, "AsyncClient", lambda *a, **k: FakeClient())
    monkeypatch.setattr(M, "persist_rotated_refresh_token", fake_persist)

    import asyncio
    creds = M.AccountCredentials(email="a@outlook.com", refresh_token="OLD_RT_123",
                                 client_id="11111111-2222-3333-4444-555555555555", auth_method="imap")
    token = asyncio.run(M.get_access_token(creds))
    assert token == "AT_123"
    assert captured["email"] == "a@outlook.com"
    assert captured["expected"] == "OLD_RT_123"
    assert captured["new_rt"] == "ROTATED_NEW"


# ---------------- 导出 P2-4 ----------------

def _auth_setup():
    """配置管理员密码 + session。"""
    import main as M

    M._write_json_file(M.AUTH_FILE, {
        "admin_password_hash": M.hash_password("verify-pass-123"),
        "agreement_accepted": True,
        "setup_mode": "normal",
        "updated_at": None,
    })
    raw_token, expires_at = M.create_session_token()
    sessions = M.load_sessions()
    sessions["sessions"][raw_token] = {"expires_at": expires_at, "created_at": M.datetime.utcnow().isoformat()}
    M.save_sessions(sessions)
    return raw_token


def test_export_requires_session_and_rejects_api_key(_isolated):
    """未认证 401;仅 API key 401;session 200。"""
    import main as M
    from fastapi.testclient import TestClient

    _seed_account()
    raw_token = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw_token)

    # 无认证
    anon = TestClient(M.app)
    assert anon.get("/accounts/export").status_code == 401

    # 仅 API key(伪造一个合法 key)
    import secrets
    api_key = "om_" + secrets.token_hex(24)
    api_data = M.load_api_keys_data()
    key_id = secrets.token_hex(8)
    api_data["keys"][key_id] = {
        "key_hash": M.hash_api_key(api_key),
        "name": "t",
        "created_at": M.datetime.utcnow().isoformat(),
        "status": "active",
        "max_requests": 100,
        "used_requests": 0,
        "unlimited_requests": False,
    }
    M.save_api_keys_data(api_data)
    api_client = TestClient(M.app)
    r = api_client.get("/accounts/export", headers={"Authorization": f"Bearer {api_key}"})
    assert r.status_code == 401  # 导出仅允许 session

    # session 200
    r2 = client.get("/accounts/export")
    assert r2.status_code == 200
    assert "alice" not in r2.text
    assert "a@outlook.com" in r2.text
    assert "password-not-stored" in r2.text


def test_export_skips_malformed_fields(_isolated):
    """字段含换行/---- 的账号被跳过,不产生可注入的额外行。"""
    import main as M
    from fastapi.testclient import TestClient

    M._write_json_file(M.ACCOUNTS_FILE, {
        "good@outlook.com": {
            "refresh_token": "RT_GOOD",
            "client_id": "11111111-2222-3333-4444-555555555555",
            "auth_method": "imap",
            "category_key": None,
            "tag_keys": [],
        },
        "bad@outlook.com": {
            # 注入: refresh_token 内嵌换行与伪造行
            "refresh_token": "RT_BAD\nfake@evil.com----pw----cid----rt",
            "client_id": "22222222-2222-3333-4444-555555555555",
            "auth_method": "imap",
            "category_key": None,
            "tag_keys": [],
        },
    })
    raw_token = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw_token)

    r = client.get("/accounts/export")
    assert r.status_code == 200
    lines = [l for l in r.text.strip().split("\n") if l.strip()]
    assert len(lines) == 1
    assert lines[0].startswith("good@outlook.com")
    assert "fake@evil.com" not in r.text


def test_export_empty_accounts(_isolated):
    """空账号返回空体 200。"""
    import main as M
    from fastapi.testclient import TestClient

    M._write_json_file(M.ACCOUNTS_FILE, {})
    raw_token = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw_token)
    r = client.get("/accounts/export")
    assert r.status_code == 200
    assert r.text == ""


def test_export_reimportable_four_segments(_isolated):
    """正常账号导出行可被前端 4 段启发式解析(邮箱----密码----client_id----rt)。"""
    import main as M
    from fastapi.testclient import TestClient

    _seed_account(rt="M.REAL_TOKEN_DATA_ABC", client_id="9e5f94bc-e8a4-4e73-b8be-63364c29d753")
    raw_token = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw_token)
    r = client.get("/accounts/export")
    parts = r.text.strip().split("----")
    assert len(parts) == 4
    email, password, client_id, rt = parts
    assert email == "a@outlook.com"
    assert password == "password-not-stored"
    assert client_id == "9e5f94bc-e8a4-4e73-b8be-63364c29d753"
    assert rt == "M.REAL_TOKEN_DATA_ABC"


# ---------------- 选中/单账号导出 + 单账号刷新 (P2-4 扩展) ----------------

@pytest.fixture()
def _two_accounts(_isolated):
    import main as M
    from fastapi.testclient import TestClient

    M._write_json_file(M.ACCOUNTS_FILE, {
        "alice@outlook.com": {
            "refresh_token": "RT_ALICE", "client_id": "9e5f94bc-e8a4-4e73-b8be-63364c29d753",
            "auth_method": "imap", "category_key": None, "tag_keys": [],
        },
        "bob@outlook.com": {
            "refresh_token": "RT_BOB", "client_id": "11111111-2222-3333-4444-555555555555",
            "auth_method": "imap", "category_key": None, "tag_keys": [],
        },
    })
    raw = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw)
    return client


def test_export_single(_two_accounts):
    client = _two_accounts
    r = client.get("/accounts/export", params={"emails": "alice@outlook.com"})
    assert r.status_code == 200
    lines = [l for l in r.text.strip().split("\n") if l]
    assert len(lines) == 1 and lines[0].startswith("alice@outlook.com")


def test_export_selected(_two_accounts):
    client = _two_accounts
    r = client.get("/accounts/export", params={"emails": "alice@outlook.com,bob@outlook.com"})
    assert r.status_code == 200
    lines = [l for l in r.text.strip().split("\n") if l]
    assert len(lines) == 2


def test_export_unknown_email_404(_two_accounts):
    client = _two_accounts
    r = client.get("/accounts/export", params={"emails": "ghost@outlook.com"})
    assert r.status_code == 404


def test_export_all_still_works(_two_accounts):
    client = _two_accounts
    r = client.get("/accounts/export")
    assert r.status_code == 200
    assert len([l for l in r.text.strip().split("\n") if l]) == 2


def test_refresh_one_missing_email_400(_two_accounts):
    client = _two_accounts
    r = client.post("/accounts/refresh-one", json={})
    assert r.status_code == 400


def test_refresh_one_unknown_email_404(_two_accounts):
    client = _two_accounts
    r = client.post("/accounts/refresh-one", json={"email": "ghost@outlook.com"})
    assert r.status_code == 404


# ---------------- 单封邮件 .eml 下载 ----------------

def _seed_one_account(_isolated, auth_method="imap"):
    import main as M
    from fastapi.testclient import TestClient

    M._write_json_file(M.ACCOUNTS_FILE, {
        "mail@outlook.com": {
            "refresh_token": "RT_MAIL", "client_id": "9e5f94bc-e8a4-4e73-b8be-63364c29d753",
            "auth_method": auth_method, "category_key": None, "tag_keys": [],
        }
    })
    raw = _auth_setup()
    client = TestClient(M.app)
    client.cookies.set(M.SESSION_COOKIE, raw)
    return client, M


def test_eml_download_requires_auth(_isolated):
    client, M = _seed_one_account(_isolated)
    from fastapi.testclient import TestClient
    anon = TestClient(M.app)
    r = anon.get("/emails/mail@outlook.com/INBOX-123/download")
    assert r.status_code == 401


def test_eml_filename_sanitizer():
    import main as M

    assert M._build_eml_filename('你好/世界:测试?').endswith('.eml')
    assert '/' not in M._build_eml_filename('a/b')
    assert ':' not in M._build_eml_filename('a:b')
    assert M._build_eml_filename('') == 'email.eml'


def test_eml_download_imap_unknown_account_404(_isolated):
    client, _ = _seed_one_account(_isolated)
    r = client.get("/emails/ghost@outlook.com/INBOX-1/download")
    assert r.status_code == 404


def test_eml_download_graph_mock(_isolated, monkeypatch):
    """Graph 通道:mock $value 端点返回 MIME 字节,验证响应为 message/rfc822 附件。"""
    import main as M

    client, M = _seed_one_account(_isolated, auth_method="graph")

    captured = {}

    async def fake_get_access_token(credentials):
        return "FAKE_AT"

    async def fake_download_graph_eml_bytes(credentials, graph_message_id):
        captured["gid"] = graph_message_id
        return b"From: a@b.com\r\nSubject: Test\r\n\r\nBody"

    monkeypatch.setattr(M, "get_access_token", fake_get_access_token)
    monkeypatch.setattr(M, "_download_graph_eml_bytes", fake_download_graph_eml_bytes)

    # message_id 格式 graph:folder:graphid
    r = client.get("/emails/mail@outlook.com/graph%3Ainbox%3AABC123/download")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("message/rfc822")
    assert "attachment" in r.headers.get("content-disposition", "")
    assert ".eml" in r.headers.get("content-disposition", "")
    assert r.content == b"From: a@b.com\r\nSubject: Test\r\n\r\nBody"
    assert captured["gid"] == "ABC123"


def test_eml_download_invalid_graph_id(_isolated):
    client, _ = _seed_one_account(_isolated, auth_method="graph")
    r = client.get("/emails/mail@outlook.com/badformat/download")
    assert r.status_code == 400
