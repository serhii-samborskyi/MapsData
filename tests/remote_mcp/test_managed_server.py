import builtins
import hashlib
import sys
import threading
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import Mock

import anyio
import httpx
import pytest
from fastapi import APIRouter, FastAPI
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend
from mcp.server.transport_security import TransportSecurityMiddleware
from psycopg2.extras import Json
from starlette.responses import JSONResponse

import mcp_integration
from remote_mcp import CampaignService, MCPSettings, create_mcp_server

TOKEN = "managed-token-original-0123456789abcdef"
NEXT_TOKEN = "managed-token-rotated-0123456789abcdef"
URL = "https://managed.example.test/"
NEXT_URL = "https://rotated.example.test:8443/"


def configuration(token=TOKEN, url=URL):
    return {
        "public_url": url,
        "token_digest": hashlib.sha256(token.encode()).hexdigest(),
    }


async def probe(http, token=TOKEN, url=URL, *, method="POST", headers=None):
    return await http.request(
        method,
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            **(headers or {}),
        },
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
    )


@pytest.mark.anyio
async def test_managed_sdk_starts_unconfigured_and_can_be_enabled_without_restart():
    state = {"configuration": None}
    loader = Mock(side_effect=lambda: state["configuration"])
    service = Mock()
    service.list_templates.return_value = {"templates": []}
    server = create_mcp_server(service, configuration_loader=loader)
    loader.assert_not_called()
    manager = server.sdk.session_manager
    async with (
        server.lifespan(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http,
    ):
        assert (await probe(http)).status_code == 503
        state["configuration"] = configuration()
        http.headers["Authorization"] = f"Bearer {TOKEN}"
        async with (
            streamable_http_client(URL, http_client=http) as (read, write, _),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            assert len((await session.list_tools()).tools) == 8
            result = await session.call_tool("list_templates", {"kind": "source"})
            assert result.structuredContent == {"templates": []}
        assert server.sdk.session_manager is manager
    service.list_templates.assert_called_once_with("source")


@pytest.mark.anyio
async def test_rotation_revocation_and_url_updates_reach_independent_server_instances():
    state = {"configuration": configuration()}
    servers = [
        create_mcp_server(Mock(), configuration_loader=lambda: state["configuration"])
        for _ in range(2)
    ]
    async with AsyncExitStack() as stack:
        clients = []
        for server in servers:
            await stack.enter_async_context(server.lifespan())
            clients.append(
                await stack.enter_async_context(
                    httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app))
                )
            )
        for http in clients:
            assert (await probe(http)).status_code == 200
        state["configuration"] = configuration(NEXT_TOKEN)
        for http in clients:
            assert (await probe(http)).status_code == 401
            assert (await probe(http, NEXT_TOKEN)).status_code == 200
        state["configuration"] = configuration(NEXT_TOKEN, NEXT_URL)
        for http in clients:
            assert (await probe(http, NEXT_TOKEN)).status_code == 421
            assert (await probe(http, TOKEN, NEXT_URL)).status_code == 401
            assert (await probe(http, NEXT_TOKEN, NEXT_URL)).status_code == 200
            assert (
                await probe(
                    http, NEXT_TOKEN, NEXT_URL, headers={"Origin": URL.rstrip("/")}
                )
            ).status_code == 403
            assert (
                await probe(
                    http, NEXT_TOKEN, NEXT_URL, headers={"Origin": NEXT_URL.rstrip("/")}
                )
            ).status_code == 200
        state["configuration"] = None
        for http in clients:
            assert (await probe(http, NEXT_TOKEN, NEXT_URL)).status_code == 503
        state["configuration"] = configuration()
        for http in clients:
            assert (await probe(http)).status_code == 200


@pytest.mark.anyio
async def test_snapshot_loaded_once_off_event_loop_and_not_shared_between_requests(
    monkeypatch,
):
    event_loop_thread = threading.get_ident()
    state = {"configuration": configuration()}
    loads = []

    def load():
        assert threading.get_ident() != event_loop_thread
        loads.append(True)
        return state["configuration"]

    entered = anyio.Event()
    release = anyio.Event()
    original = TransportSecurityMiddleware.validate_request

    async def delayed_security(self, request, is_post=False):
        if self.settings.allowed_hosts == ["managed.example.test"]:
            entered.set()
            await release.wait()
        return await original(self, request, is_post=is_post)

    monkeypatch.setattr(
        TransportSecurityMiddleware, "validate_request", delayed_security
    )
    server = create_mcp_server(Mock(), configuration_loader=load)
    results = []
    async with (
        server.lifespan(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http,
    ):

        async def first_request():
            results.append(await probe(http))

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(first_request)
                await entered.wait()
                state["configuration"] = configuration(NEXT_TOKEN, NEXT_URL)
                results.append(await probe(http, NEXT_TOKEN, NEXT_URL))
                release.set()
    assert [response.status_code for response in results] == [200, 200]
    assert len(loads) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
@pytest.mark.parametrize(
    "headers,status",
    [
        ({"Host": "wrong.example.test"}, 421),
        ({"Origin": "https://wrong.example.test"}, 403),
    ],
)
async def test_official_dynamic_security_runs_before_bearer_auth(
    monkeypatch, method, headers, status
):
    authenticate = Mock(side_effect=AssertionError("Auth must follow security checks"))
    monkeypatch.setattr(BearerAuthBackend, "authenticate", authenticate)
    server = create_mcp_server(Mock(), configuration_loader=configuration)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http:
        response = await probe(http, method=method, headers=headers)
    assert response.status_code == status
    authenticate.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
@pytest.mark.parametrize(
    "headers,query",
    [
        ({}, ""),
        ({"Authorization": "Bearer wrong"}, ""),
        ({"Authorization": f"Basic {TOKEN}"}, ""),
        ({"Cookie": f"scrapiq_auth={TOKEN}"}, ""),
        ({}, f"?token={TOKEN}"),
    ],
)
async def test_managed_auth_requires_current_bearer_on_every_request(
    method, headers, query
):
    server = create_mcp_server(Mock(), configuration_loader=configuration)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http:
        response = await http.request(method, URL + query, headers=headers, json={})
    assert response.status_code == 401
    assert response.headers["www-authenticate"].startswith("Bearer")
    assert TOKEN not in response.text


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invalid",
    [
        None,
        {},
        {"public_url": URL, "token_digest": TOKEN},
        {"public_url": URL, "token_digest": "a" * 64 + "\n"},
        {"public_url": "http://unsafe.test/", "token_digest": "a" * 64},
        {"public_url": "https://unsafe.test:*/", "token_digest": "a" * 64},
        {"public_url": "https://unsafe.test:99999/", "token_digest": "a" * 64},
        {"public_url": "https://unsafe.test/\n", "token_digest": "a" * 64},
        {"public_url": "https://user:PRIVATE@unsafe.test/", "token_digest": "a" * 64},
        {"public_url": URL, "token_digest": "a" * 64, "token": TOKEN},
    ],
)
async def test_invalid_configuration_never_falls_back_to_static_credentials(
    invalid, caplog
):
    server = create_mcp_server(
        Mock(),
        MCPSettings(token=TOKEN, public_url=URL),
        configuration_loader=lambda: invalid,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http:
        response = await probe(http)
    assert response.status_code == 503
    assert response.json() == {"error": "MCP connection is unavailable"}
    assert TOKEN not in caplog.text + response.text
    assert "PRIVATE" not in caplog.text + response.text


@pytest.mark.anyio
async def test_store_outage_fails_closed_without_stale_auth_or_secret_logs(caplog):
    loader = Mock(side_effect=[configuration(), RuntimeError(f"DB secret {TOKEN}")])
    server = create_mcp_server(Mock(), configuration_loader=loader)
    async with (
        server.lifespan(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http,
    ):
        assert (await probe(http)).status_code == 200
        response = await probe(http)
    assert response.status_code == 503
    assert TOKEN not in caplog.text + response.text


@pytest.mark.anyio
@pytest.mark.parametrize("ui_auth", [False, True])
@pytest.mark.parametrize("environment", [False, True])
async def test_install_always_includes_setup_router_and_gates_mcp_mount(
    monkeypatch, ui_auth, environment
):
    if environment:
        monkeypatch.setenv("MAPSDATA_MCP_TOKEN", TOKEN)
        monkeypatch.setenv("MAPSDATA_MCP_PUBLIC_URL", URL)
    else:
        monkeypatch.delenv("MAPSDATA_MCP_TOKEN", raising=False)
        monkeypatch.setenv("MAPSDATA_MCP_PUBLIC_URL", "ignored-without-env-token")
    store = SimpleNamespace(active_configuration=Mock(return_value=None))
    constructor = Mock(return_value=store)
    host = SimpleNamespace(app=FastAPI(), UI_AUTH_ENABLED=ui_auth, get_db=Mock())
    setup = APIRouter()

    @setup.get("/api/mcp/connection")
    async def setup_status():
        return JSONResponse(
            {"locked": not ui_auth}, status_code=200 if ui_auth else 403
        )

    router = Mock(return_value=setup)
    monkeypatch.setitem(
        sys.modules,
        "mcp_connections",
        SimpleNamespace(
            ConnectionStore=constructor,
            router=router,
        ),
    )
    events = []
    host.app.add_event_handler("startup", lambda: events.append("startup"))
    host.app.add_event_handler("shutdown", lambda: events.append("shutdown"))
    mcp_integration.install(host)
    constructor.assert_called_once()
    assert constructor.call_args.args[0] is host.get_db
    settings = constructor.call_args.args[1]
    if environment:
        assert settings.token.get_secret_value() == TOKEN
    else:
        assert settings is None
    router.assert_called_once_with(host, store)
    store.active_configuration.assert_not_called()
    assert any(route.path == "/mcp" for route in host.app.routes) == (
        environment or ui_auth
    )
    async with (
        host.app.router.lifespan_context(host.app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=host.app)) as http,
    ):
        assert events == ["startup"]
        response = await http.get(URL + "api/mcp/connection")
        assert response.status_code == (200 if ui_auth else 403)
        if environment or ui_auth:
            assert (await probe(http, url=URL + "mcp/")).status_code == 503
            store.active_configuration.return_value = configuration()
            assert (await probe(http, url=URL + "mcp/")).status_code == 200
        else:
            assert (await probe(http, url=URL + "mcp/")).status_code == 404
    assert events == ["startup", "shutdown"]


def test_server_requires_real_settings_or_configuration_loader():
    with pytest.raises(ValueError, match="settings or a configuration loader"):
        create_mcp_server(Mock())


@pytest.mark.anyio
async def test_disabled_install_never_imports_sdk_but_keeps_real_setup_routes(
    monkeypatch,
):
    original_import = builtins.__import__

    def without_sdk(name, *args, **kwargs):
        if name == "remote_mcp" or name.startswith(("remote_mcp.", "mcp.")):
            raise AssertionError("Disabled host must not import MCP dependencies")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_sdk)
    monkeypatch.delenv("MAPSDATA_MCP_TOKEN", raising=False)
    host = SimpleNamespace(app=FastAPI(), UI_AUTH_ENABLED=False, get_db=Mock())
    mcp_integration.install(host)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=host.app)) as http:
        state = await http.get(URL + "api/mcp/connection")
        assert state.status_code == 200
        assert state.json()["can_manage"] is False
        for action in ("token", "reveal", "revoke"):
            response = await http.post(URL + f"api/mcp/connection/{action}", json={})
            assert response.status_code == 403
        assert (await probe(http, url=URL + "mcp/")).status_code == 404
    host.get_db.assert_not_called()


@pytest.mark.anyio
async def test_managed_transport_preserves_sdk_content_type_and_body_limits():
    server = create_mcp_server(Mock(), configuration_loader=configuration)
    async with (
        server.lifespan(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http,
    ):
        assert (await http.post(URL)).status_code == 401
        headers = {"Authorization": f"Bearer {TOKEN}"}
        assert (await http.post(URL, headers=headers)).status_code == 400
        assert (
            await http.post(
                URL, headers={**headers, "Content-Type": "text/plain"}, content="{}"
            )
        ).status_code == 400
        assert (
            await http.post(
                URL,
                headers={**headers, "Content-Type": "application/json"},
                content=b"x" * (4 * 1024 * 1024 + 1),
            )
        ).status_code == 413


@pytest.mark.anyio
async def test_real_connection_store_rotates_across_servers_and_environment_wins(
    get_db,
):
    from mcp_connections import ConnectionStore, init_schema

    with get_db() as conn:
        cursor = conn.cursor()
        init_schema(cursor)
        cursor.execute(
            "CREATE TABLE source_templates (id INTEGER PRIMARY KEY, name TEXT, "
            "source_type TEXT, enabled BOOLEAN, config JSONB)"
        )
        cursor.execute(
            "INSERT INTO source_templates VALUES "
            "(7, 'Managed source', 'http_api', TRUE, %s)",
            (Json({"api_key": "PRIVATE_SOURCE_CONFIG"}),),
        )
        conn.commit()
    managed_stores = [ConnectionStore(get_db), ConnectionStore(get_db)]
    public_url = URL + "mcp/"
    next_url = NEXT_URL + "mcp/"
    env_store = ConnectionStore(get_db, MCPSettings(token=TOKEN, public_url=public_url))
    service = CampaignService(
        get_db, mcp_integration.hooks(SimpleNamespace(get_db=get_db))
    )
    servers = [
        create_mcp_server(service, configuration_loader=store.active_configuration)
        for store in [*managed_stores, env_store]
    ]
    async with AsyncExitStack() as stack:
        clients = []
        for server in servers:
            await stack.enter_async_context(server.lifespan())
            app = FastAPI()
            app.mount("/mcp", server.app)
            clients.append(
                await stack.enter_async_context(
                    httpx.AsyncClient(transport=httpx.ASGITransport(app=app))
                )
            )
        assert (await probe(clients[0], url=public_url)).status_code == 503
        generated = managed_stores[0].generate(public_url)
        original_token = generated["token"]
        for http in clients[:2]:
            assert (await probe(http, original_token, public_url)).status_code == 200
            http.headers["Authorization"] = f"Bearer {original_token}"
            async with (
                streamable_http_client(public_url, http_client=http) as (
                    read,
                    write,
                    _,
                ),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                result = await session.call_tool("list_templates", {"kind": "source"})
                assert not result.isError
                assert result.structuredContent["templates"][1] == {
                    "id": 7,
                    "name": "Managed source",
                    "source_type": "http_api",
                    "enabled": True,
                    "kind": "source",
                    "configuration_redacted": True,
                }
                assert "PRIVATE_SOURCE_CONFIG" not in result.model_dump_json()
        assert (await probe(clients[2], url=public_url)).status_code == 200
        assert (await probe(clients[2], original_token, public_url)).status_code == 401
        generated = managed_stores[1].generate(next_url, replace_confirmed=True)
        rotated_token = generated["token"]
        assert rotated_token != original_token
        for http in clients[:2]:
            assert (await probe(http, original_token, next_url)).status_code == 401
            assert (await probe(http, rotated_token, public_url)).status_code == 421
            assert (await probe(http, rotated_token, next_url)).status_code == 200
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mcp_connections")
            saved = str(cursor.fetchall())
        assert original_token not in saved and rotated_token not in saved
        assert hashlib.sha256(rotated_token.encode()).hexdigest() in saved
        managed_stores[0].revoke(confirmed=True)
        for http in clients[:2]:
            assert (await probe(http, rotated_token, next_url)).status_code == 503
        assert (await probe(clients[2], url=public_url)).status_code == 200


def test_invalid_environment_settings_do_not_include_secret_values_in_errors():
    with pytest.raises(ValueError) as error:
        MCPSettings(token=TOKEN, public_url="https://user:PRIVATE_VALUE@example.test/")
    assert "PRIVATE_VALUE" not in str(error.value)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["static", "environment", "loader"])
@pytest.mark.parametrize(
    "configured,canonical",
    [
        ("https://MAPS.example.test/mcp/", "https://maps.example.test/mcp/"),
        ("https://maps.example.test:443/mcp/", "https://maps.example.test/mcp/"),
        ("https://MAPS.example.test:443/mcp/", "https://maps.example.test/mcp/"),
        ("https://MAPS.example.test:8443/mcp/", "https://maps.example.test:8443/mcp/"),
        ("https://[2001:DB8::1]:443/mcp/", "https://[2001:db8::1]/mcp/"),
        ("https://[2001:DB8::1]:8443/mcp/", "https://[2001:db8::1]:8443/mcp/"),
    ],
)
async def test_canonical_public_url_authenticates_normalized_clients(
    monkeypatch, mode, configured, canonical
):
    from mcp_connections import ConnectionStore

    monkeypatch.setenv("MAPSDATA_MCP_TOKEN", TOKEN)
    monkeypatch.setenv("MAPSDATA_MCP_PUBLIC_URL", configured)
    settings = MCPSettings.from_env()
    get_db = Mock(side_effect=AssertionError("Environment tokens must not query DB"))
    store = ConnectionStore(get_db, settings)
    if mode == "static":
        server = create_mcp_server(Mock(), settings)
    elif mode == "environment":
        server = create_mcp_server(
            Mock(), settings, configuration_loader=store.active_configuration
        )
    else:
        server = create_mcp_server(
            Mock(), configuration_loader=lambda: configuration(url=configured)
        )
    app = FastAPI()
    app.mount("/mcp", server.app)
    async with (
        server.lifespan(),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http,
    ):
        response = await probe(
            http,
            url=canonical,
            headers={"Origin": str(httpx.URL(canonical).copy_with(path=""))},
        )
        assert response.status_code == 200
        assert (await probe(http, "wrong", canonical)).status_code == 401
        assert (
            await probe(
                http, url=canonical, headers={"Origin": "https://wrong.example.test"}
            )
        ).status_code == 403
    assert settings.public_url == canonical
    assert store.active_configuration()["public_url"] == canonical
    assert store.metadata()["public_url"] == canonical
    assert canonical in store.reveal()["blocks"]["codex"]
    get_db.assert_not_called()
