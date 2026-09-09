"""Run separately with MAPSDATA_MCP_TEST_DSN pointing at disposable PostgreSQL.

Each test imports the real host against a unique schema. No public-schema rows
are touched, and worker startup plus outbound HTTP are intercepted.
"""

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import anyio
import httpx
import psycopg2
import pytest
import requests
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn
from psycopg2.extras import Json

TOKEN = "host-integration-test-token-0123456789abcdef"
BASE_URL = "https://maps-host.example.test"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def host(monkeypatch):
    dsn = os.environ.get("MAPSDATA_MCP_TEST_DSN")
    if not dsn:
        pytest.skip(
            "Set MAPSDATA_MCP_TEST_DSN; run this file separately from legacy module stubs"
        )
    if parse_dsn(dsn).get("dbname") in (None, "postgres", "template0", "template1"):
        pytest.fail(
            "Host tests require a dedicated test database, such as mapsdata_mcp_test"
        )
    schema = "mcp_host_test_" + uuid4().hex
    module_name = "mapsdata_mcp_host_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    network_attempts = []

    def block_network(*args, **kwargs):
        network_attempts.append(True)
        raise AssertionError("Host integration tests must not make external HTTP calls")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", block_network)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", block_network)
    monkeypatch.setenv(
        "DATABASE_URL", make_dsn(dsn, options=f"-c search_path={schema}")
    )
    monkeypatch.setenv("MAPSDATA_MCP_TOKEN", TOKEN)
    monkeypatch.setenv("MAPSDATA_MCP_PUBLIC_URL", BASE_URL + "/mcp/")
    monkeypatch.setenv("LOGIN", "test-ui-user")
    monkeypatch.setenv("PASSWORD", "test-ui-password")
    monkeypatch.chdir(ROOT)

    try:
        spec = importlib.util.spec_from_file_location(module_name, ROOT / "main.py")
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, module_name, module)
        spec.loader.exec_module(module)
        assert module.app.__class__.__module__ == "fastapi.applications"
        assert any(route.path == "/mcp" for route in module.app.routes)

        def execute(query, parameters=(), fetch=False):
            with module.get_db() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, parameters)
                    result = cursor.fetchall() if fetch else None
                conn.commit()
            return result

        def wake_after_commit(run_id):
            # A different connection must see both the new run and draft result.
            rows = execute(
                "SELECT status FROM automation_runs WHERE id = %s", (run_id,), True
            )
            assert rows
            rows = execute(
                "SELECT id FROM mcp_campaign_drafts WHERE launch_result->>'run_id' = %s",
                (str(run_id),),
                True,
            )
            assert rows

        wake = Mock(side_effect=wake_after_commit)
        scheduler = Mock()
        source = Mock(side_effect=AssertionError("Unexpected source worker startup"))
        enrichment = Mock(
            side_effect=AssertionError("Unexpected enrichment worker startup")
        )
        monkeypatch.setattr(module, "_ensure_automation_scheduler", scheduler)
        monkeypatch.setattr(module, "_ensure_automation_run_worker", wake)
        monkeypatch.setattr(module, "_start_http_source_campaign_job", source)
        monkeypatch.setattr(module, "_ensure_enrichment_worker", enrichment)
        yield SimpleNamespace(
            app=module.app,
            module=module,
            execute=execute,
            wake=wake,
            scheduler=scheduler,
            source=source,
            enrichment=enrichment,
        )
        assert not network_attempts
        source.assert_not_called()
        enrichment.assert_not_called()
        assert not module.automation_active_runs
    finally:
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
        admin.close()


@pytest.fixture
async def session(host):
    async with host.app.router.lifespan_context(host.app):
        host.scheduler.assert_called_once()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=host.app),
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as http:
            async with streamable_http_client(BASE_URL + "/mcp/", http_client=http) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write) as client:
                    await client.initialize()
                    yield client


def seed(host, mode="batch", *, export=True):
    source_config = {
        "base_url": "https://source.invalid/search?q={query}&api_key=HOST_SOURCE_SECRET",
        "query_params": {"token": "HOST_QUERY_SECRET"},
        "request_template": "{{query}}",
        "field_mapping": [
            {"source_field": "name", "target_fields": ["business_name"]},
        ],
    }
    # Use the host's source normalizer to construct the same records as the UI.
    source_config = host.module._normalize_http_source_template_config(source_config)
    source_id = host.execute(
        """
        INSERT INTO source_templates (name, source_type, config)
        VALUES ('HTTP source', 'http_api', %s) RETURNING id
    """,
        (Json(source_config),),
        True,
    )[0]["id"]
    enrichment_id = host.execute(
        """
        INSERT INTO enrichment_templates (name, service, api_config, input_mapping, output_mapping)
        VALUES ('Host enrichment', 'http_enrichment', %s, %s, %s) RETURNING id
    """,
        (
            Json(
                {
                    "api_url": "https://enrichment.invalid/api",
                    "api_key": "HOST_ENRICH_SECRET",
                }
            ),
            Json({"company": "business_name", "city": "source_data.city"}),
            Json({"result": "custom_1"}),
        ),
        True,
    )[0]["id"]

    def export_template(name, target, mapping):
        return host.execute(
            """
            INSERT INTO export_templates (name, service, api_config, field_mappings)
            VALUES (%s, 'sendread_list', %s, %s) RETURNING id
        """,
            (
                name,
                Json(
                    {
                        "api_key": "HOST_EXPORT_SECRET",
                        "api_base_url": "https://export.invalid",
                        "sendread_target_id": target,
                        "sendread_target_type": "ab_test_list",
                    }
                ),
                Json(mapping),
            ),
            True,
        )[0]["id"]

    export_id = export_template(
        "Host export", "default-list", {"email": "email", "city": "source_data.city"}
    )
    override_id = export_template(
        "Override export",
        "override-default-list",
        {"email": "email", "firstName": "firstname"},
    )
    steps = [
        {"type": "pipeline", "enabled": True, "config": {}},
        {
            "type": "enrichment",
            "enabled": True,
            "config": {"template_id": enrichment_id},
        },
    ]
    if export:
        steps.append(
            {
                "type": "export",
                "enabled": True,
                "config": {
                    "template_id": export_id,
                    "require_confirmation": True,
                    "export_valid_only": True,
                    "exclude_public_emails": True,
                },
            }
        )
    funnel_id = host.execute(
        """
        INSERT INTO automation_funnel_templates (name, steps, default_retry_count, execution_mode)
        VALUES ('Host funnel', %s, 3, %s) RETURNING id
    """,
        (Json(steps), mode),
        True,
    )[0]["id"]
    return {
        "source_id": source_id,
        "funnel_id": funnel_id,
        "enrichment_id": enrichment_id,
        "export_id": export_id,
        "override_id": override_id,
        "source_config": source_config,
    }


async def call(session, name, arguments):
    response = await session.call_tool(name, arguments)
    assert not response.isError, response.model_dump_json()
    assert response.structuredContent is not None
    return response.structuredContent


async def prepare(session, records, **overrides):
    payload = {
        "name": "Host MCP campaign",
        "source_template_id": records["source_id"],
        "funnel_template_id": records["funnel_id"],
        "requests": [" niche Austin TX ", "niche Austin TX", "niche Round Rock TX"],
        **overrides,
    }
    return await call(session, "prepare_campaign", {"payload": payload})


def confirmation(preview, confirmed=True):
    return {
        "preview_id": preview["preview_id"],
        "preview_hash": preview["preview_hash"],
        "confirmed": confirmed,
    }


def count(host, table):
    return host.execute(
        sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table)),
        fetch=True,
    )[0]["n"]


@pytest.mark.anyio
async def test_host_lifespan_initializes_defaults_and_authenticates_mount(
    host, session
):
    assert count(host, "mcp_campaign_drafts") == 0
    defaults = host.execute("SELECT service FROM export_templates", fetch=True)
    assert {row["service"] for row in defaults} >= {
        "manyreach",
        "smartlead",
        "sendread_campaign",
        "sendread_list",
    }
    result = await call(session, "list_templates", {"kind": "source"})
    assert result["templates"][0] == {
        "id": None,
        "name": "Google Maps (built-in)",
        "kind": "source",
        "source_type": "builtin_google_maps",
        "enabled": True,
        "configuration_redacted": True,
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=host.app)) as http:
        for method in ("GET", "POST", "DELETE"):
            for headers in (
                {},
                {"Authorization": "Bearer wrong"},
                {"Cookie": "scrapiq_auth=wrong"},
            ):
                response = await http.request(
                    method, BASE_URL + "/mcp/", headers=headers
                )
                assert response.status_code == 401
                assert response.headers["www-authenticate"].startswith("Bearer")
    host.wake.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_real_prepare_confirmation_and_idempotent_launch(host, session, mode):
    records = seed(host, mode)
    preview = await prepare(session, records)
    assert preview["campaign"]["execution_mode"] == mode
    assert preview["submitted_requests"][0] == " niche Austin TX "
    assert preview["campaign"]["requests"] == ["niche Austin TX", "niche Round Rock TX"]
    assert preview["export"]["destination"]["target_id"] == "default-list"
    assert preview["export"]["field_mappings"] == {
        "email": "email",
        "city": "source_data.city",
    }
    assert preview["export"]["filters"]["export_valid_only"] is True
    assert preview["export"]["require_confirmation"] is True
    assert "HOST_" not in json.dumps(preview)
    assert ".invalid" not in json.dumps(preview)
    assert all(
        count(host, table) == 0
        for table in (
            "search_campaigns",
            "requests",
            "automation_runs",
            "pipeline_runs",
        )
    )
    host.wake.assert_not_called()

    denied = await session.call_tool(
        "launch_prepared_campaign", confirmation(preview, False)
    )
    assert denied.isError
    wrong_hash = confirmation(preview)
    wrong_hash["preview_hash"] = "0" * 64
    assert (await session.call_tool("launch_prepared_campaign", wrong_hash)).isError
    assert count(host, "search_campaigns") == 0
    first = await call(session, "launch_prepared_campaign", confirmation(preview))
    retry = await call(session, "launch_prepared_campaign", confirmation(preview))
    assert first["campaign_id"] == retry["campaign_id"]
    assert first["run_id"] == retry["run_id"]
    assert first["idempotent"] is False and retry["idempotent"] is True
    assert not first["worker_wake_pending"] and not retry["worker_wake_pending"]
    assert count(host, "search_campaigns") == count(host, "automation_runs") == 1
    assert count(host, "requests") == 2
    assert count(host, "automation_run_steps") == 3
    assert host.wake.call_count == 2
    assert (
        await call(
            session, "get_prepared_campaign", {"preview_id": preview["preview_id"]}
        )
    )["status"] == "launched"


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
@pytest.mark.parametrize("pipeline", ["missing", "disabled"])
@pytest.mark.parametrize("builtin_source", [False, True])
async def test_prepare_requires_enabled_pipeline_for_new_campaign(
    host, session, mode, pipeline, builtin_source
):
    records = seed(host, mode)
    steps = host.execute(
        "SELECT steps FROM automation_funnel_templates WHERE id=%s",
        (records["funnel_id"],),
        True,
    )[0]["steps"]
    if pipeline == "missing":
        steps = [step for step in steps if step["type"] != "pipeline"]
    else:
        steps[0]["enabled"] = False
    host.execute(
        "UPDATE automation_funnel_templates SET steps=%s WHERE id=%s",
        (Json(steps), records["funnel_id"]),
    )
    result = await session.call_tool(
        "prepare_campaign",
        {
            "payload": {
                "name": "New sourced campaign",
                "requests": ["literal niche Austin TX"],
                "source_template_id": None if builtin_source else records["source_id"],
                "funnel_template_id": records["funnel_id"],
            }
        },
    )
    assert result.isError
    assert "New campaigns require a funnel with an enabled pipeline step" in (
        result.model_dump_json()
    )
    assert all(
        count(host, table) == 0
        for table in (
            "mcp_campaign_drafts",
            "search_campaigns",
            "requests",
            "automation_runs",
            "automation_run_steps",
            "pipeline_runs",
        )
    )
    host.wake.assert_not_called()


@pytest.mark.parametrize("mode", ["batch", "streaming"])
def test_existing_contact_funnel_can_still_omit_pipeline(host, mode):
    records = seed(host, mode)
    host.execute(
        "UPDATE automation_funnel_templates SET steps=steps - 0 WHERE id=%s",
        (records["funnel_id"],),
    )
    campaign_id = host.execute(
        "INSERT INTO search_campaigns (name, status) "
        "VALUES ('Existing contacts', 'completed') RETURNING id",
        fetch=True,
    )[0]["id"]
    host.execute(
        "INSERT INTO contacts (campaign_id, business_name, email, status) "
        "VALUES (%s, %s, %s, 'pending')",
        (campaign_id, "Existing contact", "existing@example.test"),
    )
    with host.module.get_db() as conn:
        run_id = host.module._create_automation_run(
            conn.cursor(), campaign_id, records["funnel_id"]
        )
        conn.commit()
    steps = host.execute(
        "SELECT step_type FROM automation_run_steps WHERE run_id=%s ORDER BY step_order",
        (run_id,),
        True,
    )
    assert [step["step_type"] for step in steps] == ["enrichment", "export"]
    host.wake.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
@pytest.mark.parametrize("enabled", [True, False])
async def test_template_edits_cannot_change_prepared_source_or_steps(
    host, session, mode, enabled
):
    records = seed(host, mode)
    preview = await prepare(session, records)
    original_plan = host.execute(
        "SELECT frozen_plan FROM mcp_campaign_drafts WHERE id=%s",
        (preview["preview_id"],),
        True,
    )[0]["frozen_plan"]
    host.execute(
        "UPDATE source_templates SET config=%s WHERE id=%s",
        (
            Json(
                {
                    **records["source_config"],
                    "base_url": "https://changed.invalid/?q={query}",
                }
            ),
            records["source_id"],
        ),
    )
    host.execute(
        "UPDATE enrichment_templates SET api_config=%s, input_mapping=%s, output_mapping=%s WHERE id=%s",
        (
            Json({"api_url": "https://changed.invalid", "api_key": "CHANGED_SECRET"}),
            Json({"company": "company"}),
            Json({"result": "custom_2"}),
            records["enrichment_id"],
        ),
    )
    host.execute(
        "UPDATE export_templates SET api_config=%s, field_mappings=%s WHERE id=%s",
        (
            Json({"sendread_target_id": "changed-list", "api_key": "CHANGED_SECRET"}),
            Json({"email": "custom_2"}),
            records["export_id"],
        ),
    )
    host.execute(
        """UPDATE automation_funnel_templates
           SET name='Changed funnel', steps='[]', default_retry_count=8,
               execution_mode=%s, enabled=%s WHERE id=%s""",
        ("streaming" if mode == "batch" else "batch", enabled, records["funnel_id"]),
    )
    restored = await call(
        session, "get_prepared_campaign", {"preview_id": preview["preview_id"]}
    )
    assert restored == preview
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    campaign = host.execute(
        "SELECT * FROM search_campaigns WHERE id=%s", (launched["campaign_id"],), True
    )[0]
    run = host.execute(
        "SELECT * FROM automation_runs WHERE id=%s", (launched["run_id"],), True
    )[0]
    steps = host.execute(
        "SELECT * FROM automation_run_steps WHERE run_id=%s ORDER BY step_order",
        (launched["run_id"],),
        True,
    )
    assert campaign["source_snapshot"] == original_plan["source_snapshot"]
    assert run["execution_mode"] == mode and run["max_retries"] == 3
    assert [step["config"] for step in steps] == [
        step["config"] for step in original_plan["steps"]
    ]
    assert [step["step_type"] for step in steps] == [
        step["type"] for step in original_plan["steps"]
    ]
    assert all(
        step["max_retries"] == original_plan["default_retry_count"] for step in steps
    )
    assert (
        host.execute(
            "SELECT message FROM automation_run_logs WHERE run_id=%s ORDER BY id",
            (launched["run_id"],),
            True,
        )[0]["message"]
        == f"Funnel run created from template: {original_plan['funnel_name']}"
    )
    with host.module.get_db() as conn:
        assert (
            host.module._get_campaign_source_type(
                conn.cursor(), launched["campaign_id"]
            )
            == "http_api"
        )
    assert (
        steps[-1]["config"]["template_snapshot"]["api_config"]["sendread_target_id"]
        == "default-list"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_builtin_source_can_be_prepared_and_launched(host, session, mode):
    records = seed(host, mode)
    preview = await prepare(session, records, source_template_id=None)
    assert preview["templates"][0]["id"] is None
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    source = host.execute(
        "SELECT source_template_id,source_snapshot FROM search_campaigns WHERE id=%s",
        (launched["campaign_id"],),
        True,
    )[0]
    assert source["source_template_id"] is None
    assert source["source_snapshot"]["source_type"] == "builtin_google_maps"
    with host.module.get_db() as conn:
        assert (
            host.module._get_campaign_source_type(
                conn.cursor(), launched["campaign_id"]
            )
            == "builtin_google_maps"
        )


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
@pytest.mark.parametrize("with_overrides", [False, True])
async def test_frozen_execution_mode_does_not_depend_on_launch_overrides(
    host, session, monkeypatch, mode, with_overrides
):
    records = seed(host, mode)
    preview = await prepare(session, records)
    other_mode = "streaming" if mode == "batch" else "batch"
    host.execute(
        "UPDATE automation_funnel_templates SET execution_mode=%s WHERE id=%s",
        (other_mode, records["funnel_id"]),
    )
    create_run = host.module._create_automation_run

    def create_with_overrides(*args, **kwargs):
        kwargs["overrides"] = {"execution_mode": other_mode} if with_overrides else None
        return create_run(*args, **kwargs)

    monkeypatch.setattr(host.module, "_create_automation_run", create_with_overrides)
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    assert (
        host.execute(
            "SELECT execution_mode FROM automation_runs WHERE id=%s",
            (launched["run_id"],),
            True,
        )[0]["execution_mode"]
        == mode
    )


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_disabled_funnel_rejects_new_previews_and_nonfrozen_starts(
    host, session, mode
):
    records = seed(host, mode)
    host.execute(
        "UPDATE automation_funnel_templates SET enabled=FALSE WHERE id=%s",
        (records["funnel_id"],),
    )
    result = await session.call_tool(
        "prepare_campaign",
        {
            "payload": {
                "name": "Disabled funnel",
                "requests": ["literal request"],
                "funnel_template_id": records["funnel_id"],
            }
        },
    )
    assert result.isError
    assert count(host, "mcp_campaign_drafts") == 0
    campaign_id = host.execute(
        "INSERT INTO search_campaigns (name, status) VALUES ('Existing campaign', 'inactive') RETURNING id",
        fetch=True,
    )[0]["id"]
    with host.module.get_db() as conn:
        with pytest.raises(host.module.HTTPException) as error:
            host.module._create_automation_run(
                conn.cursor(), campaign_id, records["funnel_id"]
            )
        assert error.value.status_code == 404
    assert count(host, "automation_runs") == 0
    host.wake.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
@pytest.mark.parametrize("list_override", [None, "chosen-ab-list"])
async def test_export_override_uses_selected_template_and_resolved_destination(
    host, session, mode, list_override
):
    records = seed(host, mode)
    preview = await prepare(
        session,
        records,
        export_template_id=records["override_id"],
        sendread_ab_list_id=list_override,
    )
    assert preview["export"]["template_id"] == records["override_id"]
    assert preview["export"]["destination"]["target_id"] == (
        list_override or "override-default-list"
    )
    assert preview["export"]["field_mappings"] == {
        "email": "email",
        "firstName": "firstname",
    }
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    step = host.execute(
        "SELECT config FROM automation_run_steps WHERE run_id=%s AND step_type='export'",
        (launched["run_id"],),
        True,
    )[0]["config"]
    assert step["template_id"] == records["override_id"]
    assert step["field_mappings"] == preview["export"]["field_mappings"]


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_stop_cancels_work_and_launch_retry_cannot_restart(host, session, mode):
    records = seed(host, mode)
    preview = await prepare(session, records)
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    result = await call(
        session, "stop_campaign", {"campaign_id": launched["campaign_id"]}
    )
    assert result["stop_requested"] is True
    assert (await call(session, "get_run_status", {"run_id": launched["run_id"]}))[
        "status"
    ] == "cancelled"
    await call(session, "stop_campaign", {"campaign_id": launched["campaign_id"]})
    await call(session, "launch_prepared_campaign", confirmation(preview))
    # Exercise the actual worker entry point for a retried, stopped run. It must
    # return without sourcing/enriching/exporting or changing the terminal state.
    host.module._run_automation_run(launched["run_id"])
    campaign = host.execute(
        "SELECT status,daemon_ignore FROM search_campaigns WHERE id=%s",
        (launched["campaign_id"],),
        True,
    )[0]
    assert campaign == {"status": "inactive", "daemon_ignore": True}
    run = host.execute(
        "SELECT status FROM automation_runs WHERE id=%s", (launched["run_id"],), True
    )[0]
    assert run["status"] == "cancelled"
    assert all(
        row["status"] == "cancelled"
        for row in host.execute(
            "SELECT status FROM automation_run_steps WHERE run_id=%s",
            (launched["run_id"],),
            True,
        )
    )
    assert count(host, "search_campaigns") == count(host, "automation_runs") == 1


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_stop_cancels_multiple_active_runs_in_id_order(
    host, session, monkeypatch, mode
):
    records = seed(host, mode)
    preview = await prepare(session, records)
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    active_ids = [launched["run_id"]]
    for status in ("running", "waiting_confirmation"):
        active_ids.append(
            host.execute(
                "INSERT INTO automation_runs "
                "(campaign_id, template_id, status, execution_mode) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (launched["campaign_id"], records["funnel_id"], status, mode),
                True,
            )[0]["id"]
        )
    # Move the first row to the end of the heap so scan order is not ID order.
    host.execute(
        "UPDATE automation_runs SET updated_at=CURRENT_TIMESTAMP WHERE id=%s",
        (active_ids[0],),
    )
    cancel = Mock(wraps=host.module._cancel_automation_run)
    monkeypatch.setattr(host.module, "_cancel_automation_run", cancel)
    await call(session, "stop_campaign", {"campaign_id": launched["campaign_id"]})
    assert [
        invocation.args[1]["id"] for invocation in cancel.call_args_list
    ] == active_ids
    assert all(
        row["status"] == "cancelled"
        for row in host.execute(
            "SELECT status FROM automation_runs WHERE campaign_id=%s",
            (launched["campaign_id"],),
            True,
        )
    )


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["batch", "streaming"])
async def test_deleted_funnel_rejects_launch_and_rolls_back_all_inserts(
    host, session, mode
):
    records = seed(host, mode)
    preview = await prepare(session, records)
    host.execute(
        "DELETE FROM automation_funnel_templates WHERE id=%s", (records["funnel_id"],)
    )
    result = await session.call_tool("launch_prepared_campaign", confirmation(preview))
    assert result.isError
    assert "Prepared funnel no longer exists" in result.model_dump_json()
    assert (
        count(host, "search_campaigns")
        == count(host, "requests")
        == count(host, "automation_runs")
        == 0
    )
    assert (
        await call(
            session, "get_prepared_campaign", {"preview_id": preview["preview_id"]}
        )
    )["status"] == "prepared"
    host.wake.assert_not_called()


@pytest.mark.anyio
async def test_concurrent_host_launches_create_one_campaign_and_run(host, session):
    records = seed(host, "streaming")
    preview = await prepare(session, records)
    results = await asyncio.gather(
        *(
            call(session, "launch_prepared_campaign", confirmation(preview))
            for _ in range(4)
        )
    )
    assert len({result["campaign_id"] for result in results}) == 1
    assert len({result["run_id"] for result in results}) == 1
    assert sum(not result["idempotent"] for result in results) == 1
    assert not any(result["worker_wake_pending"] for result in results)
    assert count(host, "search_campaigns") == count(host, "automation_runs") == 1


@pytest.mark.anyio
async def test_host_stream_progress_and_stop_retain_delivery_outcomes(host, session):
    records = seed(host, "streaming")
    preview = await prepare(session, records)
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    steps = host.execute(
        "SELECT id,step_type FROM automation_run_steps WHERE run_id=%s",
        (launched["run_id"],),
        True,
    )
    export_step = next(step for step in steps if step["step_type"] == "export")
    for state in ("pending", "retry", "blocked", "running", "completed", "uncertain"):
        contact_id = host.execute(
            "INSERT INTO contacts (campaign_id,business_name,email,status) VALUES (%s,%s,%s,'pending') RETURNING id",
            (launched["campaign_id"], "Private contact", "private@example.test"),
            True,
        )[0]["id"]
        host.execute(
            """
            INSERT INTO automation_stream_tasks (run_id,step_id,contact_id,step_type,status,error,result)
            VALUES (%s,%s,%s,'export',%s,'HOST_PRIVATE_ERROR',%s)
        """,
            (
                launched["run_id"],
                export_step["id"],
                contact_id,
                state,
                Json({"provider_response": "HOST_PRIVATE_RESPONSE"}),
            ),
        )
    status = await call(
        session, "get_campaign_status", {"campaign_id": launched["campaign_id"]}
    )
    assert status["execution_mode"] == "streaming" and status["total_contacts"] == 6
    counts = next(
        row for row in status["stream_progress"] if row["step_type"] == "export"
    )
    assert all(
        counts[state] == 1
        for state in (
            "pending",
            "retry",
            "blocked",
            "running",
            "completed",
            "uncertain",
        )
    )
    assert "HOST_PRIVATE" not in json.dumps(status)
    assert "private@example.test" not in json.dumps(status)
    await call(session, "stop_campaign", {"campaign_id": launched["campaign_id"]})
    stopped = await call(session, "get_run_status", {"run_id": launched["run_id"]})
    counts = next(
        row for row in stopped["stream_progress"] if row["step_type"] == "export"
    )
    assert counts["cancelled"] == 3
    assert counts["running"] == counts["completed"] == counts["uncertain"] == 1
    assert stopped["status"] == "cancelled"


@pytest.mark.anyio
async def test_batch_export_consumes_the_same_destination_and_filters_as_preview(
    host, session, monkeypatch
):
    records = seed(host)
    steps = host.execute(
        "SELECT steps FROM automation_funnel_templates WHERE id=%s",
        (records["funnel_id"],),
        True,
    )[0]["steps"]
    steps[-1]["config"].update(
        {
            "export_valid_only": False,
            "filters": {"export_valid_only": True},
            "destination": {
                "target_id": "step-selected-list",
                "target_type": "ab_test_list",
            },
        }
    )
    host.execute(
        "UPDATE automation_funnel_templates SET steps=%s WHERE id=%s",
        (Json(steps), records["funnel_id"]),
    )
    preview = await prepare(session, records)
    assert preview["export"]["filters"]["export_valid_only"] is True
    assert preview["export"]["destination"]["target_id"] == "step-selected-list"
    launched = await call(session, "launch_prepared_campaign", confirmation(preview))
    config = host.execute(
        "SELECT config FROM automation_run_steps WHERE run_id=%s AND step_type='export'",
        (launched["run_id"],),
        True,
    )[0]["config"]
    calls = []

    async def capture(campaign_id, request):
        if calls:
            raise host.module.HTTPException(404, "No more test contacts")
        calls.append((await request.json(), request._template_snapshot))
        return {"contacts_exported": 1}

    monkeypatch.setattr(host.module, "export_campaign", AsyncMock(side_effect=capture))
    result = await anyio.to_thread.run_sync(
        host.module._automation_step_export, launched["campaign_id"], config
    )
    assert result[0] is True
    payload, snapshot = calls[0]
    assert (
        payload["export_valid_only"]
        == preview["export"]["filters"]["export_valid_only"]
    )
    assert (
        snapshot["api_config"]["sendread_target_id"]
        == preview["export"]["destination"]["target_id"]
    )
    assert payload["field_mappings"] == preview["export"]["field_mappings"]
