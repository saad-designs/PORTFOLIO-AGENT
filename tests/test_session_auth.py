import asyncio

import pytest
from fastapi import HTTPException, Response
from starlette.requests import Request

import main


def _request(method="GET", scheme="http", headers=None, session=None):
    all_headers = {"host": "portfolio.example"}
    all_headers.update(headers or {})
    if session:
        all_headers["cookie"] = f"dashboard_session={session}"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": scheme,
        "path": "/api/test",
        "raw_path": b"/api/test",
        "query_string": b"",
        "headers": [(key.lower().encode(), value.encode()) for key, value in all_headers.items()],
        "server": ("portfolio.example", 443 if scheme == "https" else 80),
        "client": ("127.0.0.1", 50000),
        "root_path": "",
    }
    return Request(scope)


def test_session_token_is_signed_and_expires(monkeypatch):
    monkeypatch.setattr(main, "DASHBOARD_KEY", "unit-test-key")
    now = 1_800_000_000
    monkeypatch.setattr(main.time, "time", lambda: now)

    token = main._create_dashboard_session_token()
    assert main._is_valid_dashboard_session(token)
    assert not main._is_valid_dashboard_session(token[:-1] + ("0" if token[-1] != "0" else "1"))

    monkeypatch.setattr(
        main.time,
        "time",
        lambda: now + main.DASHBOARD_SESSION_TTL_SECONDS,
    )
    assert not main._is_valid_dashboard_session(token)


def test_cookie_auth_requires_same_origin_for_state_changes(monkeypatch):
    monkeypatch.setattr(main, "DASHBOARD_KEY", "unit-test-key")
    token = main._create_dashboard_session_token()

    with pytest.raises(HTTPException) as missing_origin:
        main.verify_dashboard_auth(_request("POST", session=token))
    assert missing_origin.value.status_code == 403

    with pytest.raises(HTTPException) as cross_origin:
        main.verify_dashboard_auth(_request(
            "POST",
            headers={"origin": "https://attacker.example"},
            session=token,
        ))
    assert cross_origin.value.status_code == 403

    assert main.verify_dashboard_auth(_request(
        "POST",
        headers={"origin": "http://portfolio.example"},
        session=token,
    ))


def test_session_cookie_has_expiry_and_https_dependent_secure_flag(monkeypatch):
    monkeypatch.setattr(main, "DASHBOARD_KEY", "unit-test-key")
    monkeypatch.setattr(main, "SESSION_COOKIE_SECURE_MODE", "auto")

    http_response = Response()
    asyncio.run(main.create_dashboard_session(
        _request("POST", headers={"x-dashboard-key": "unit-test-key"}),
        http_response,
    ))
    http_cookie = http_response.headers["set-cookie"]
    assert "HttpOnly" in http_cookie
    assert "SameSite=strict" in http_cookie
    assert "Max-Age=43200" in http_cookie
    assert "secure" not in http_cookie.lower()

    https_response = Response()
    asyncio.run(main.create_dashboard_session(
        _request(
            "POST",
            headers={
                "x-dashboard-key": "unit-test-key",
                "x-forwarded-proto": "https",
            },
        ),
        https_response,
    ))
    assert "secure" in https_response.headers["set-cookie"].lower()


def test_logout_clears_session_cookie(monkeypatch):
    monkeypatch.setattr(main, "SESSION_COOKIE_SECURE_MODE", "auto")
    response = Response()

    result = asyncio.run(main.logout_dashboard_session(
        _request(
            "POST",
            headers={"origin": "http://portfolio.example"},
        ),
        response,
    ))

    assert result == {"status": "logged_out"}
    assert "dashboard_session=" in response.headers["set-cookie"]
    assert "Max-Age=0" in response.headers["set-cookie"]


def test_database_connector_is_blocked_by_test_fixture():
    import portfolio_manager

    with pytest.raises(AssertionError, match="real database connections are forbidden"):
        portfolio_manager.get_db_connection()
