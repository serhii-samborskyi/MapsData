import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from pydantic import ValidationError

from remote_mcp import MCPSettings, create_mcp_server

TOKEN = "test-only-mcp-token-0123456789abcdef"
URL = "https://maps.example.test/"


def remote(service):
    return create_mcp_server(service, MCPSettings(token=TOKEN, public_url=URL))


@asynccontextmanager
async def client(server):
    async with server.lifespan():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.app),
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as http:
            async with streamable_http_client(URL, http_client=http) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    yield session


@pytest.mark.anyio
async def test_sdk_handshake_discovery_and_tools():
    service = Mock()
    service.list_templates.return_value = {"templates": []}
    async with client(remote(service)) as session:
        listing = await session.list_tools()
        tools = {tool.name: tool for tool in listing.tools}
        assert set(tools) == {
            "generate_city_requests",
            "list_templates",
            "prepare_campaign",
            "get_prepared_campaign",
            "launch_prepared_campaign",
            "get_campaign_status",
            "get_run_status",
            "stop_campaign",
        }
        launch = tools["launch_prepared_campaign"]
        assert set(launch.inputSchema["required"]) == {
            "preview_id",
            "preview_hash",
            "confirmed",
        }
        assert launch.annotations.destructiveHint is True
        assert tools["list_templates"].annotations.readOnlyHint is True
        result = await session.call_tool("list_templates", {"kind": "source"})
        assert not result.isError
        assert result.structuredContent == {"templates": []}
    service.list_templates.assert_called_once_with("source")


@pytest.mark.anyio
async def test_geography_delegates_without_campaign_or_database_calls(monkeypatch):
    generate = Mock(
        return_value={
            "requests": ["niche Austin, TX"],
            "request_count": 1,
            "coverage": {"source": "US Census", "exhaustive_ads": False},
        }
    )
    monkeypatch.setitem(
        sys.modules,
        "geographic_requests",
        SimpleNamespace(generate_city_requests=generate),
    )
    service = Mock()
    async with client(remote(service)) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert tools["generate_city_requests"].annotations.readOnlyHint is True
        assert tools["generate_city_requests"].annotations.openWorldHint is True
        result = await session.call_tool(
            "generate_city_requests",
            {
                "state": "TX",
                "request_template": "niche {city}, {state}",
            },
        )
    assert result.structuredContent["requests"] == ["niche Austin, TX"]
    generate.assert_called_once_with("TX", "niche {city}, {state}")
    assert service.mock_calls == []


@pytest.mark.anyio
async def test_mounted_lifespan_preserves_existing_startup_handlers():
    service = Mock()
    service.list_templates.return_value = {"templates": []}
    server = remote(service)
    app = FastAPI()
    events = []
    app.add_event_handler("startup", lambda: events.append("startup"))
    app.add_event_handler("shutdown", lambda: events.append("shutdown"))
    previous = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app):
        async with previous(app):
            async with server.lifespan():
                yield

    app.router.lifespan_context = lifespan
    app.mount("/mcp", server.app)
    async with app.router.lifespan_context(app):
        assert events == ["startup"]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as http:
            async with streamable_http_client(URL + "mcp/", http_client=http) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        "list_templates", {"kind": "source"}
                    )
                    assert result.structuredContent == {"templates": []}
    assert events == ["startup", "shutdown"]


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
@pytest.mark.parametrize(
    "headers,query",
    [
        ({}, ""),
        ({"Authorization": "Bearer wrong"}, ""),
        ({}, f"?token={TOKEN}"),
        ({"Cookie": f"scrapiq_auth={TOKEN}"}, ""),
        ({"Authorization": f"Basic {TOKEN}"}, ""),
    ],
)
async def test_auth_required_on_every_http_request(method, headers, query):
    service = Mock()
    server = remote(service)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app)) as http:
        response = await http.request(method, URL + query, headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"].startswith("Bearer")
    assert TOKEN not in response.text
    assert service.mock_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "headers,status",
    [
        ({"Host": "evil.example"}, 421),
        ({"Origin": "https://evil.example"}, 403),
    ],
)
async def test_host_and_origin_protection(headers, status):
    server = remote(Mock())
    async with server.lifespan():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server.app)
        ) as http:
            response = await http.post(
                URL,
                json={},
                headers={
                    "Authorization": f"Bearer {TOKEN}",
                    "Accept": "application/json, text/event-stream",
                    **headers,
                },
            )
    assert response.status_code == status


@pytest.mark.anyio
@pytest.mark.parametrize("confirmed", [None, "true", 1])
async def test_missing_or_non_boolean_confirmation_never_reaches_service(confirmed):
    service = Mock()
    async with client(remote(service)) as session:
        args = {
            "preview_id": "b94e1077-0e65-47fe-87b2-2033b967bfc1",
            "preview_hash": "a" * 64,
        }
        if confirmed is not None:
            args["confirmed"] = confirmed
        result = await session.call_tool("launch_prepared_campaign", args)
        assert result.isError
    service.launch_prepared_campaign.assert_not_called()


@pytest.mark.anyio
async def test_hook_exceptions_do_not_expose_secrets():
    service = Mock()
    service.get_campaign_status.side_effect = RuntimeError(
        "https://provider.invalid/?token=SECRET"
    )
    async with client(remote(service)) as session:
        result = await session.call_tool("get_campaign_status", {"campaign_id": 1})
    assert result.isError
    assert "SECRET" not in result.model_dump_json()
    assert "provider.invalid" not in result.model_dump_json()


@pytest.mark.anyio
async def test_actual_draft_workflow_over_sdk_transport(integration, payload):
    service, hooks = integration
    async with client(remote(service)) as session:
        prepared = await session.call_tool(
            "prepare_campaign", {"payload": payload.model_dump()}
        )
        assert not prepared.isError
        preview = prepared.structuredContent
        assert hooks.launch_calls == 0
        args = {
            "preview_id": preview["preview_id"],
            "preview_hash": preview["preview_hash"],
            "confirmed": False,
        }
        denied = await session.call_tool("launch_prepared_campaign", args)
        assert denied.isError and hooks.launch_calls == 0
        args["confirmed"] = True
        launched = await session.call_tool("launch_prepared_campaign", args)
        assert not launched.isError
        retry = await session.call_tool("launch_prepared_campaign", args)
        assert retry.structuredContent["idempotent"] is True
        assert (
            retry.structuredContent["campaign_id"]
            == launched.structuredContent["campaign_id"]
        )
        stopped = await session.call_tool(
            "stop_campaign", {"campaign_id": launched.structuredContent["campaign_id"]}
        )
        assert stopped.structuredContent["stop_requested"] is True
        assert "SECRET" not in prepared.model_dump_json() + launched.model_dump_json()


@pytest.mark.parametrize(
    "token,url",
    [
        ("", URL),
        ("short", URL),
        ("x" * 31 + " ", URL),
        (TOKEN, "http://maps.example.test/"),
        (TOKEN, "https://maps.example.test/mcp"),
        (TOKEN, "https://maps.example.test/?key=secret"),
        (TOKEN, "https://user:pass@maps.example.test/"),
    ],
)
def test_configuration_fails_closed(token, url):
    with pytest.raises(ValidationError):
        MCPSettings(token=token, public_url=url)
