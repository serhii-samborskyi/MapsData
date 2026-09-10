"""Connection-management tests using only isolated mapsdata_mcp_test schemas.

Run with MAPSDATA_MCP_TEST_DSN; the real host and its workers are never imported.
"""

import hashlib
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import httpx
import psycopg2
import pytest
import requests
import tomllib
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn
from psycopg2.extras import RealDictCursor

import mcp_connections
from remote_mcp.server import MCPSettings

ORIGIN = "https://maps.example.test"
PUBLIC_URL = ORIGIN + "/mcp/"
API = "/api/mcp/connection"
ENV_TOKEN = "environment-test-token-0123456789abcdef"
AJAX_HEADERS = {"Origin": ORIGIN, "X-Requested-With": "XMLHttpRequest"}


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


def assert_public(payload, *secrets):
    serialized = json.dumps(payload)
    assert "token_digest" not in serialized
    assert '"token"' not in serialized
    assert "blocks" not in payload
    for secret in secrets:
        assert secret not in serialized


def assert_no_store(response):
    assert "no-store" in response.headers.get("cache-control", "").lower()


def assert_blocks(blocks, public_url, token):
    assert set(blocks) == {"codex", "claude_code", "assistant"}
    codex = {
        "mcp_servers": {
            "mapsdata": {
                "url": public_url,
                "http_headers": {"Authorization": f"Bearer {token}"},
            }
        }
    }
    assert tomllib.loads(blocks["codex"]) == codex
    assert json.loads(blocks["claude_code"]) == {
        "mcpServers": {
            "mapsdata": {
                "type": "http",
                "url": public_url,
                "headers": {"Authorization": f"Bearer {token}"},
            }
        }
    }
    _, separator, remaining = blocks["assistant"].partition("```toml\n")
    assert separator
    snippet, closing, _ = remaining.partition("```")
    assert closing
    assert tomllib.loads(snippet) == codex


class ConnectionHarness:
    def __init__(self, dsn):
        self.dsn = dsn
        self.store = mcp_connections.ConnectionStore(self.get_db)

    @contextmanager
    def get_db(self):
        connection = psycopg2.connect(self.dsn, cursor_factory=RealDictCursor)
        try:
            yield connection
        finally:
            connection.close()

    def execute(self, query, parameters=()):
        with self.get_db() as conn, conn.cursor() as cursor:
            cursor.execute(query, parameters)
            result = (
                [dict(row) for row in cursor.fetchall()] if cursor.description else []
            )
            conn.commit()
            return result

    def rows(self):
        return self.execute("SELECT * FROM mcp_connections ORDER BY id")

    def environment_store(self):
        return mcp_connections.ConnectionStore(
            self.get_db,
            environment_settings=MCPSettings(token=ENV_TOKEN, public_url=PUBLIC_URL),
        )

    @contextmanager
    def client(
        self,
        *,
        store=None,
        authenticated=True,
        auth_enabled=True,
        origin=ORIGIN,
        client_ip=None,
    ):
        app = FastAPI()
        host = SimpleNamespace(
            app=app,
            UI_AUTH_ENABLED=auth_enabled,
            _is_ui_authenticated=Mock(return_value=authenticated),
        )
        app.include_router(mcp_connections.router(host, store or self.store))

        async def transport(scope, receive, send):
            if client_ip is not None and scope["type"] == "http":
                scope = {**scope, "client": (client_ip, 12345)}
            await app(scope, receive, send)

        with TestClient(transport, base_url=origin) as client:
            yield client


@pytest.fixture(autouse=True)
def no_external_http(monkeypatch):
    def blocked(*_args, **_kwargs):
        raise AssertionError("Connection tests must not make external HTTP requests")

    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", blocked)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", blocked)
    monkeypatch.delenv("MAPSDATA_MCP_TOKEN", raising=False)
    monkeypatch.delenv("MAPSDATA_MCP_PUBLIC_URL", raising=False)


@pytest.fixture
def connections():
    dsn = os.environ.get("MAPSDATA_MCP_TEST_DSN")
    if not dsn:
        pytest.skip(
            "Set MAPSDATA_MCP_TEST_DSN to the dedicated mapsdata_mcp_test database"
        )
    if parse_dsn(dsn).get("dbname") != "mapsdata_mcp_test":
        pytest.fail(
            "Connection tests only run against mapsdata_mcp_test, never the preview DB"
        )
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    schema = "mcp_connections_test_" + uuid4().hex
    created = False
    try:
        with admin.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            assert cursor.fetchone()[0] == "mapsdata_mcp_test"
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            created = True
        harness = ConnectionHarness(
            make_dsn(
                dsn,
                options=f"-c search_path={schema} -c statement_timeout=5000 -c lock_timeout=3000",
                connect_timeout=5,
            )
        )
        with harness.get_db() as conn, conn.cursor() as cursor:
            mcp_connections.init_schema(cursor)
            conn.commit()
        yield harness
    finally:
        if created:
            with admin.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )
        admin.close()


def test_schema_initializes_disabled_singleton_and_is_idempotent(connections):
    rows = connections.rows()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["public_url"] is None
    assert rows[0]["token_digest"] is None
    assert set(rows[0]) == {
        "id",
        "public_url",
        "token_digest",
        "created_at",
        "updated_at",
    }
    assert connections.store.active_configuration() is None

    generated = connections.store.generate(PUBLIC_URL)
    before = connections.rows()
    with connections.get_db() as conn, conn.cursor() as cursor:
        mcp_connections.init_schema(cursor)
        mcp_connections.init_schema(cursor)
        conn.commit()
    assert connections.rows() == before
    assert before[0]["token_digest"] == digest(generated["token"])
    with pytest.raises(psycopg2.IntegrityError):
        connections.execute("INSERT INTO mcp_connections (id) VALUES (2)")
    with pytest.raises(psycopg2.IntegrityError):
        connections.execute("INSERT INTO mcp_connections (id) VALUES (1)")
    assert connections.rows() == before


def test_generate_uses_32_bytes_of_entropy_and_persists_only_digest(
    connections, monkeypatch
):
    entropy = Mock(return_value="A" * 43)
    monkeypatch.setattr(mcp_connections.secrets, "token_urlsafe", entropy)
    generated = connections.store.generate(PUBLIC_URL)
    entropy.assert_called_once_with(32)
    assert generated["token"] == "mapsdata_" + "A" * 43
    assert generated["enabled"] is True
    assert generated["managed"] is True
    assert generated["public_url"] == PUBLIC_URL
    row = connections.rows()[0]
    assert row["token_digest"] == digest(generated["token"])
    assert re.fullmatch(r"[0-9a-f]{64}", row["token_digest"])
    assert generated["token"] not in json.dumps(row, default=str)
    assert row["created_at"] is not None
    assert row["updated_at"] is not None
    fresh = mcp_connections.ConnectionStore(connections.get_db)
    assert fresh.active_configuration() == {
        "public_url": PUBLIC_URL,
        "token_digest": digest(generated["token"]),
    }


def test_metadata_is_secret_free_before_and_after_generation(connections):
    disabled = connections.store.metadata(default_url=PUBLIC_URL)
    assert disabled == {
        "enabled": False,
        "managed": True,
        "environment_managed": False,
        "public_url": PUBLIC_URL,
        "token_available": False,
    }
    assert_public(disabled)
    generated = connections.store.generate(PUBLIC_URL)
    enabled = connections.store.metadata(default_url="https://other.example.test/mcp/")
    assert enabled == {
        "enabled": True,
        "managed": True,
        "environment_managed": False,
        "public_url": PUBLIC_URL,
        "token_available": False,
    }
    assert_public(enabled, generated["token"], digest(generated["token"]))


def test_rotation_and_revocation_are_persistent(connections):
    first = connections.store.generate(PUBLIC_URL)
    second_url = "https://maps.example.test/reverse/proxy/mcp/"
    second = connections.store.generate(second_url, replace_confirmed=True)
    assert second["token"] != first["token"]
    assert second["public_url"] == second_url
    assert connections.store.active_configuration() == {
        "public_url": second_url,
        "token_digest": digest(second["token"]),
    }
    assert len(connections.rows()) == 1
    revoked = connections.store.revoke(confirmed=True)
    assert revoked == connections.store.metadata()
    assert revoked["enabled"] is False
    assert revoked["managed"] is True
    assert_public(revoked, first["token"], second["token"])
    assert connections.store.active_configuration() is None
    assert connections.rows()[0]["token_digest"] is None
    assert (
        mcp_connections.ConnectionStore(connections.get_db).active_configuration()
        is None
    )
    third = connections.store.generate(PUBLIC_URL)
    assert third["token"] not in (first["token"], second["token"])
    assert len(connections.rows()) == 1


def test_environment_takes_precedence_without_persisting_plaintext(connections):
    managed = connections.store.generate("https://managed.example.test/mcp/")
    before = connections.rows()
    store = connections.environment_store()
    assert store.active_configuration() == {
        "public_url": PUBLIC_URL,
        "token_digest": digest(ENV_TOKEN),
    }
    metadata = store.metadata(default_url="https://unused.example.test/mcp/")
    assert metadata == {
        "enabled": True,
        "managed": False,
        "environment_managed": True,
        "public_url": PUBLIC_URL,
        "token_available": True,
    }
    assert_public(metadata, ENV_TOKEN, digest(ENV_TOKEN), managed["token"])
    revealed = store.reveal()
    assert revealed["token"] == ENV_TOKEN
    assert revealed["public_url"] == PUBLIC_URL
    assert_blocks(revealed["blocks"], PUBLIC_URL, ENV_TOKEN)
    assert connections.rows() == before
    assert connections.store.active_configuration()["token_digest"] == digest(
        managed["token"]
    )


@pytest.mark.parametrize(
    "url,normalized",
    [
        (PUBLIC_URL, PUBLIC_URL),
        ("https://maps.example.test:8443/mcp/", "https://maps.example.test:8443/mcp/"),
        (
            "https://maps.example.test/reverse/proxy/mcp/",
            "https://maps.example.test/reverse/proxy/mcp/",
        ),
        ("https://MAPS.EXAMPLE.TEST:443/mcp/", PUBLIC_URL),
        (
            "https://b\u00fccher.example.test/mcp/",
            "https://xn--bcher-kva.example.test/mcp/",
        ),
        (
            "https://b\u00fccher.example.test:8443/reverse/proxy/mcp/",
            "https://xn--bcher-kva.example.test:8443/reverse/proxy/mcp/",
        ),
    ],
)
def test_generated_urls_normalize_and_pass_sdk_validation(connections, url, normalized):
    generated = connections.store.generate(url)
    assert generated["public_url"] == normalized
    assert connections.store.active_configuration()["public_url"] == normalized
    assert_blocks(generated["blocks"], normalized, generated["token"])
    settings = MCPSettings(public_url=generated["public_url"], token=generated["token"])
    assert settings.public_url == normalized
    assert settings.public_url.isascii()


@pytest.mark.parametrize(
    "url",
    [
        "http://maps.example.test/mcp/",
        "//maps.example.test/mcp/",
        "https:///mcp/",
        "https://maps.example.test/",
        "https://maps.example.test/mcp",
        "https://maps.example.test/not-mcp/",
        "https://maps.example.test/mcp/extra/",
        "https://user:password@maps.example.test/mcp/",
        "https://user@maps.example.test/mcp/",
        "https://maps.example.test/mcp/?token=secret",
        "https://maps.example.test/mcp/#fragment",
        "https://maps.example.test:invalid/mcp/",
        "https://maps.example.test:99999/mcp/",
        "https://*.example.test/mcp/",
        "https://maps.*.example.test/mcp/",
        "https://maps.example.test*/mcp/",
        "https://maps.example.test:*/mcp/",
        "https://maps.example.test/\nmcp/",
        "https://maps.example.test/\tmcp/",
        "https://maps.example.test/\x00/mcp/",
        "https://maps.example.test/\x7f/mcp/",
        "https://maps\x7f.example.test/mcp/",
        "https://maps.example.test\r/mcp/",
        "https://maps.example.test/with space/mcp/",
        "https://maps.example.test/caf\u00e9/mcp/",
        "https://maps.example.test/\u4e2d\u6587/mcp/",
        "https://maps.example.test/zero\u200bwidth/mcp/",
        "https://maps.example.test/../mcp/",
        "https://maps.example.test/./mcp/",
        "https://maps.example.test\\attacker.example.test/mcp/",
        "https://@maps.example.test/mcp/",
    ],
)
def test_invalid_public_url_cannot_create_connection(connections, url):
    before = connections.rows()
    with connections.client() as client:
        response = client.post(
            API + "/token", json={"public_url": url}, headers=AJAX_HEADERS
        )
    assert response.status_code == 400
    assert_no_store(response)
    assert url not in response.text
    assert connections.rows() == before
    assert connections.store.active_configuration() is None


@pytest.mark.parametrize("operation", ["", "/token", "/reveal", "/revoke"])
def test_unauthenticated_requests_are_rejected_without_secrets(connections, operation):
    generated = connections.store.generate(PUBLIC_URL)
    before = connections.rows()
    with connections.client(authenticated=False) as client:
        if operation:
            response = client.post(
                API + operation,
                json={"public_url": PUBLIC_URL, "confirmed": True},
                headers=AJAX_HEADERS,
            )
        else:
            response = client.get(API)
    assert response.status_code == 401, response.text
    assert_no_store(response)
    assert_public(
        response.json(), generated["token"], digest(generated["token"]), PUBLIC_URL
    )
    assert connections.rows() == before


@pytest.mark.parametrize("operation", ["", "/token", "/reveal", "/revoke"])
def test_missing_ui_login_never_exposes_connection_or_allows_changes(
    connections, operation
):
    store = connections.environment_store()
    before = connections.rows()
    with connections.client(store=store, auth_enabled=False) as client:
        if operation:
            response = client.post(
                API + operation,
                json={"public_url": PUBLIC_URL, "confirmed": True},
                headers=AJAX_HEADERS,
            )
            assert response.status_code == 403, response.text
        else:
            response = client.get(API)
            assert response.status_code == 200, response.text
            assert response.json()["can_manage"] is False
            assert response.json()["message"]
            assert_no_store(response)
    assert_public(response.json(), ENV_TOKEN, digest(ENV_TOKEN), PUBLIC_URL)
    assert_no_store(response)
    assert connections.rows() == before


@pytest.mark.parametrize(
    "token", ["mapsdata_" + "A" * 43, 'token_"quoted"_\\_\n_\t_end']
)
def test_connection_blocks_round_trip_without_toml_or_json_injection(token):
    url = 'https://maps.example.test/prefix/"quoted"/mcp/'
    assert_blocks(mcp_connections.connection_blocks(url, token), url, token)


def test_api_generate_get_rotate_revoke_and_regenerate(connections, caplog, capsys):
    with connections.client() as client:
        initial = client.get(API)
        assert initial.status_code == 200
        assert_no_store(initial)
        assert initial.json()["can_manage"] is True
        assert initial.json()["enabled"] is False
        assert initial.json()["public_url"] == PUBLIC_URL
        assert_public(initial.json())

        response = client.post(
            API + "/token", json={"public_url": PUBLIC_URL}, headers=AJAX_HEADERS
        )
        assert response.status_code == 200, response.text
        assert_no_store(response)
        first = response.json()
        assert re.fullmatch(r"mapsdata_[A-Za-z0-9_-]{43}", first["token"])
        assert_blocks(first["blocks"], PUBLIC_URL, first["token"])
        assert connections.store.active_configuration()["token_digest"] == digest(
            first["token"]
        )

        metadata = client.get(API)
        assert metadata.status_code == 200
        assert_no_store(metadata)
        assert metadata.json()["enabled"] is True
        assert metadata.json()["token_available"] is False
        assert_public(metadata.json(), first["token"], digest(first["token"]))

        reveal = client.post(API + "/reveal", json={}, headers=AJAX_HEADERS)
        assert reveal.status_code == 409
        assert_no_store(reveal)
        assert_public(reveal.json(), first["token"], digest(first["token"]))

        response = client.post(
            API + "/token",
            json={"public_url": PUBLIC_URL, "replace_confirmed": True},
            headers=AJAX_HEADERS,
        )
        assert response.status_code == 200, response.text
        assert_no_store(response)
        second = response.json()
        assert second["token"] != first["token"]
        assert_blocks(second["blocks"], PUBLIC_URL, second["token"])
        assert connections.store.active_configuration()["token_digest"] == digest(
            second["token"]
        )

        response = client.post(
            API + "/revoke", json={"confirmed": True}, headers=AJAX_HEADERS
        )
        assert response.status_code == 200, response.text
        assert_no_store(response)
        assert response.json()["enabled"] is False
        assert_public(response.json(), first["token"], second["token"])
        assert connections.store.active_configuration() is None
        metadata = client.get(API)
        assert metadata.json()["enabled"] is False
        assert_public(metadata.json(), first["token"], second["token"])

        response = client.post(
            API + "/token", json={"public_url": PUBLIC_URL}, headers=AJAX_HEADERS
        )
        assert response.status_code == 200, response.text
        assert_no_store(response)
        third = response.json()
        assert third["token"] not in (first["token"], second["token"])
        assert len(connections.rows()) == 1

    output = capsys.readouterr()
    logs = caplog.text + output.out + output.err
    for result in (first, second, third):
        assert result["token"] not in logs
        assert digest(result["token"]) not in logs


@pytest.mark.parametrize(
    "confirmed", [False, None, 0, 1, "true", "false", "1", [], {}, [True]]
)
@pytest.mark.parametrize(
    "operation,field,expected",
    [
        ("/token", "replace_confirmed", 409),
        ("/revoke", "confirmed", 400),
    ],
)
def test_mutations_require_literal_confirmation_true(
    connections, confirmed, operation, field, expected
):
    generated = connections.store.generate(PUBLIC_URL)
    before = connections.rows()
    with connections.client() as client:
        response = client.post(
            API + operation,
            json={"public_url": PUBLIC_URL, field: confirmed},
            headers=AJAX_HEADERS,
        )
    assert response.status_code == expected, response.text
    assert_no_store(response)
    assert_public(response.json(), generated["token"], digest(generated["token"]))
    assert connections.rows() == before


def test_store_methods_also_enforce_confirmation_and_no_managed_reveal(connections):
    connections.store.generate(PUBLIC_URL)
    before = connections.rows()
    for method, arguments, expected in (
        (connections.store.generate, {"public_url": PUBLIC_URL}, 409),
        (
            connections.store.generate,
            {"public_url": PUBLIC_URL, "replace_confirmed": 1},
            409,
        ),
        (connections.store.revoke, {}, 400),
        (connections.store.revoke, {"confirmed": "true"}, 400),
        (connections.store.reveal, {}, 409),
    ):
        with pytest.raises(HTTPException) as raised:
            method(**arguments)
        assert raised.value.status_code == expected
        assert "no-store" in raised.value.headers["Cache-Control"]
        assert connections.rows() == before


def test_environment_api_reveals_only_explicitly_and_rejects_mutation(connections):
    store = connections.environment_store()
    before = connections.rows()
    with connections.client(store=store) as client:
        metadata = client.get(API)
        assert metadata.status_code == 200
        assert_no_store(metadata)
        assert metadata.json()["environment_managed"] is True
        assert metadata.json()["token_available"] is True
        assert_public(metadata.json(), ENV_TOKEN, digest(ENV_TOKEN))
        response = client.post(API + "/reveal", json={}, headers=AJAX_HEADERS)
        assert response.status_code == 200, response.text
        assert_no_store(response)
        assert response.json()["token"] == ENV_TOKEN
        assert_blocks(response.json()["blocks"], PUBLIC_URL, ENV_TOKEN)
        for operation in ("/token", "/revoke"):
            response = client.post(
                API + operation,
                json={
                    "public_url": PUBLIC_URL,
                    "replace_confirmed": True,
                    "confirmed": True,
                },
                headers=AJAX_HEADERS,
            )
            assert response.status_code == 409, response.text
            assert_no_store(response)
            assert_public(response.json(), ENV_TOKEN, digest(ENV_TOKEN))
        assert_public(client.get(API).json(), ENV_TOKEN, digest(ENV_TOKEN))
    assert connections.rows() == before


@pytest.mark.parametrize("operation", ["/token", "/reveal", "/revoke"])
@pytest.mark.parametrize(
    "headers",
    [
        {"X-Requested-With": "XMLHttpRequest"},
        {"Origin": ORIGIN},
        {"Origin": ORIGIN, "X-Requested-With": "fetch"},
        {**AJAX_HEADERS, "Origin": "https://attacker.example.test"},
        {**AJAX_HEADERS, "Origin": "http://maps.example.test"},
        {**AJAX_HEADERS, "Origin": "https://maps.example.test:8443"},
        {**AJAX_HEADERS, "Origin": "null"},
        {**AJAX_HEADERS, "Origin": ORIGIN + "/"},
        {**AJAX_HEADERS, "Origin": ORIGIN + "?anything=1"},
        {**AJAX_HEADERS, "Origin": ORIGIN + "#fragment"},
        {**AJAX_HEADERS, "Origin": "https://user:password@maps.example.test"},
        {**AJAX_HEADERS, "Origin": "https://maps.example.test:invalid"},
        {**AJAX_HEADERS, "Origin": "https://[malformed"},
        {**AJAX_HEADERS, "Sec-Fetch-Site": "cross-site"},
        {**AJAX_HEADERS, "Sec-Fetch-Site": "same-site"},
        {
            **AJAX_HEADERS,
            "Origin": "https://attacker.example.test",
            "X-Forwarded-Host": "attacker.example.test",
        },
    ],
)
def test_post_security_rejects_cross_origin_and_missing_browser_headers(
    connections, operation, headers
):
    store = (
        connections.environment_store() if operation == "/reveal" else connections.store
    )
    generated = connections.store.generate(PUBLIC_URL)
    before = connections.rows()
    with connections.client(store=store) as client:
        response = client.post(
            API + operation,
            json={
                "public_url": PUBLIC_URL,
                "replace_confirmed": True,
                "confirmed": True,
            },
            headers=headers,
        )
    assert response.status_code == 403, response.text
    assert_no_store(response)
    assert_public(
        response.json(), ENV_TOKEN, generated["token"], digest(generated["token"])
    )
    assert connections.rows() == before


@pytest.mark.parametrize(
    "scope_origin,browser_origin,client_ip",
    [
        (ORIGIN, ORIGIN, None),
        (ORIGIN, ORIGIN + ":443", None),
        ("http://maps.example.test", ORIGIN, None),
        ("http://maps.example.test:443", ORIGIN, None),
        (ORIGIN + ":8443", ORIGIN + ":8443", None),
        ("http://localhost:8000", "http://localhost:8000", "127.0.0.1"),
        ("http://127.0.0.1:8000", "http://127.0.0.1:8000", "127.0.0.1"),
    ],
)
def test_same_origin_accepts_https_proxy_defaults_and_loopback(
    connections, scope_origin, browser_origin, client_ip
):
    with connections.client(origin=scope_origin, client_ip=client_ip) as client:
        response = client.post(
            API + "/token",
            json={"public_url": PUBLIC_URL},
            headers={**AJAX_HEADERS, "Origin": browser_origin},
        )
    assert response.status_code == 200, response.text
    assert_no_store(response)


def test_ipv6_loopback_origin_uses_request_host(connections):
    # This Starlette version cannot parse IPv6 TestClient base URLs.
    with connections.client(origin="http://localhost:8000", client_ip="::1") as client:
        response = client.post(
            API + "/token",
            json={"public_url": PUBLIC_URL},
            headers={
                **AJAX_HEADERS,
                "Host": "[::1]:8000",
                "Origin": "http://[::1]:8000",
            },
        )
    assert response.status_code == 200, response.text
    assert_no_store(response)


def test_http_localhost_is_not_trusted_from_a_remote_client(connections):
    with connections.client(
        origin="http://localhost:8000", client_ip="203.0.113.10"
    ) as client:
        response = client.post(
            API + "/token",
            json={"public_url": PUBLIC_URL},
            headers={**AJAX_HEADERS, "Origin": "http://localhost:8000"},
        )
    assert response.status_code == 403
    assert_no_store(response)
    assert connections.store.active_configuration() is None


@pytest.mark.parametrize(
    "content_type",
    [
        None,
        "text/plain",
        "application/x-www-form-urlencoded",
        "multipart/form-data",
        "application/jsonp",
    ],
)
@pytest.mark.parametrize("operation", ["/token", "/reveal", "/revoke"])
def test_post_requires_json_content_type(connections, content_type, operation):
    headers = dict(AJAX_HEADERS)
    if content_type:
        headers["Content-Type"] = content_type
    with connections.client() as client:
        response = client.post(
            API + operation,
            content=json.dumps({"public_url": PUBLIC_URL, "confirmed": True}),
            headers=headers,
        )
    assert response.status_code == 415, response.text
    assert_no_store(response)
    assert connections.store.active_configuration() is None


@pytest.mark.parametrize(
    "body", [b"", b"{", b"{\xff}", b"null", b"true", b"42", b'"url"', b"[]"]
)
def test_invalid_json_bodies_fail_without_mutation(connections, body):
    with connections.client() as client:
        response = client.post(
            API + "/token",
            content=body,
            headers={**AJAX_HEADERS, "Content-Type": "application/json"},
        )
    assert response.status_code == 400, response.text
    assert_no_store(response)
    assert connections.store.active_configuration() is None


@pytest.mark.parametrize(
    "url",
    [None, 42, True, [], {}, "", "https://maps.example.test/" + "x" * 2048 + "/mcp/"],
)
def test_invalid_url_types_and_lengths_return_400_without_echoing_input(
    connections, url
):
    with connections.client() as client:
        response = client.post(
            API + "/token", json={"public_url": url}, headers=AJAX_HEADERS
        )
    assert response.status_code == 400, response.text
    assert_no_store(response)
    assert connections.store.active_configuration() is None
    if isinstance(url, str) and url:
        assert url not in response.text


@pytest.mark.parametrize("operation", ["/token", "/reveal", "/revoke"])
def test_post_enforces_8k_body_limit_even_with_underreported_length(
    connections, operation
):
    body = json.dumps(
        {"public_url": PUBLIC_URL, "confirmed": True, "padding": "x" * 8192}
    ).encode()

    def chunks():
        yield body[:4096]
        yield body[4096:]

    with connections.client() as client:
        response = client.post(
            API + operation,
            content=chunks(),
            headers={
                **AJAX_HEADERS,
                "Content-Type": "application/json",
                "Content-Length": "10",
            },
        )
    assert response.status_code == 413, response.text
    assert_no_store(response)
    assert connections.store.active_configuration() is None


def test_post_accepts_json_charset_and_exactly_8k(connections):
    body = json.dumps({"public_url": PUBLIC_URL}).encode()
    body += b" " * (8192 - len(body))
    with connections.client() as client:
        response = client.post(
            API + "/token",
            content=body,
            headers={**AJAX_HEADERS, "Content-Type": "application/json; charset=utf-8"},
        )
    assert response.status_code == 200, response.text
    assert_no_store(response)


def concurrent_generations(connections, *, confirmed, workers=6):
    ready = threading.Barrier(workers)

    def generate(index):
        store = mcp_connections.ConnectionStore(connections.get_db)
        with connections.client(store=store) as client:
            ready.wait(timeout=10)
            return client.post(
                API + "/token",
                json={
                    "public_url": f"https://maps.example.test/client-{index}/mcp/",
                    "replace_confirmed": confirmed,
                },
                headers=AJAX_HEADERS,
            )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(generate, index) for index in range(workers)]
        return [future.result(timeout=15) for future in futures]


def test_concurrent_creation_has_one_winner_without_silent_rotation(connections):
    responses = concurrent_generations(connections, confirmed=False)
    assert sorted(response.status_code for response in responses) == [
        200,
        409,
        409,
        409,
        409,
        409,
    ]
    for response in responses:
        assert_no_store(response)
    winner = next(
        response.json() for response in responses if response.status_code == 200
    )
    assert connections.store.active_configuration() == {
        "public_url": winner["public_url"],
        "token_digest": digest(winner["token"]),
    }
    assert len(connections.rows()) == 1


def test_environment_settings_from_env_remain_compatible(connections, monkeypatch):
    monkeypatch.setenv("MAPSDATA_MCP_TOKEN", ENV_TOKEN)
    monkeypatch.setenv("MAPSDATA_MCP_PUBLIC_URL", PUBLIC_URL)
    settings = MCPSettings.from_env()
    store = mcp_connections.ConnectionStore(connections.get_db, settings)
    assert store.active_configuration() == {
        "public_url": PUBLIC_URL,
        "token_digest": digest(ENV_TOKEN),
    }
    assert store.reveal()["token"] == ENV_TOKEN
    assert_public(store.metadata(), ENV_TOKEN, digest(ENV_TOKEN))
    assert connections.rows()[0]["token_digest"] is None


@pytest.mark.parametrize("method", ["generate", "revoke"])
def test_environment_store_rejects_direct_mutation(connections, method):
    store = connections.environment_store()
    before = connections.rows()
    kwargs = (
        {"public_url": PUBLIC_URL, "replace_confirmed": True}
        if method == "generate"
        else {"confirmed": True}
    )
    with pytest.raises(HTTPException) as raised:
        getattr(store, method)(**kwargs)
    assert raised.value.status_code == 409
    assert "no-store" in raised.value.headers["Cache-Control"]
    assert ENV_TOKEN not in raised.value.detail
    assert connections.rows() == before


@pytest.mark.parametrize("auth_enabled,authenticated", [(False, True), (True, False)])
def test_authorization_precedes_database_access(
    connections, auth_enabled, authenticated
):
    blocked = Mock(
        side_effect=AssertionError("Unauthorized requests must not access settings")
    )
    store = mcp_connections.ConnectionStore(blocked)
    with connections.client(
        store=store, auth_enabled=auth_enabled, authenticated=authenticated
    ) as client:
        get = client.get(API)
        assert get.status_code == (401 if auth_enabled else 200)
        for operation in ("/token", "/reveal", "/revoke"):
            response = client.post(
                API + operation,
                json={
                    "public_url": PUBLIC_URL,
                    "replace_confirmed": True,
                    "confirmed": True,
                },
                headers=AJAX_HEADERS,
            )
            assert response.status_code == (401 if auth_enabled else 403)
            assert_no_store(response)
    blocked.assert_not_called()


def test_concurrent_confirmed_rotations_preserve_one_consistent_singleton(connections):
    initial = connections.store.generate(PUBLIC_URL)
    created_at = connections.rows()[0]["created_at"]
    responses = concurrent_generations(connections, confirmed=True)
    assert all(response.status_code == 200 for response in responses), [
        response.text for response in responses
    ]
    results = [response.json() for response in responses]
    assert len({result["token"] for result in results}) == len(results)
    assert all(result["token"] != initial["token"] for result in results)
    active = connections.store.active_configuration()
    assert (
        sum(
            active
            == {
                "public_url": result["public_url"],
                "token_digest": digest(result["token"]),
            }
            for result in results
        )
        == 1
    )
    rows = connections.rows()
    assert len(rows) == 1
    assert rows[0]["created_at"] == created_at
    for response in responses:
        assert_no_store(response)
