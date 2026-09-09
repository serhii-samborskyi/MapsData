import copy
import os
from contextlib import contextmanager
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from remote_mcp import CampaignInput, CampaignService, ServiceHooks, init_schema


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def get_db():
    dsn = os.environ.get("MAPSDATA_MCP_TEST_DSN")
    if not dsn:
        pytest.skip("Set MAPSDATA_MCP_TEST_DSN to a disposable PostgreSQL database")
    schema = "mcp_test_" + uuid4().hex
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.close()

    @contextmanager
    def connect():
        connection = psycopg2.connect(dsn, options=f"-c search_path={schema}")
        connection.cursor_factory = RealDictCursor
        try:
            yield connection
        finally:
            connection.close()

    try:
        with connect() as connection:
            with connection.cursor() as cursor:
                init_schema(cursor)
                init_schema(cursor)
                cursor.execute("""
                    CREATE TABLE test_campaigns (
                        id SERIAL PRIMARY KEY, plan JSONB NOT NULL, stopped BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
            connection.commit()
        yield connect
    finally:
        with psycopg2.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )
        connection.close()


def frozen_plan(payload):
    source = {
        "id": 1,
        "name": "Source",
        "config": {
            "url": "https://user:SECRET_PASSWORD@provider.invalid/api?key=SECRET_QUERY",
            "headers": {"Authorization": "Bearer SECRET_HEADER"},
            "api_key": "SECRET_KEY",
        },
    }
    export = {
        "id": 3,
        "name": "Export",
        "service": "sendread_list",
        "api_config": {"secret": "SECRET_EXPORT", "sendread_target_id": "list-1"},
        "field_mappings": {"email": "email", "city": "source_data.city"},
    }
    return {
        "name": payload["name"].strip(),
        "requests": list(dict.fromkeys(p.strip() for p in payload["requests"])),
        "source_template_id": 1,
        "source_snapshot": source,
        "funnel_template_id": 2,
        "funnel_name": "Funnel",
        "default_retry_count": 2,
        "execution_mode": payload.get("execution_mode") or "batch",
        "maps_scrape_mode": "slow",
        "scrape_maps_only": False,
        "steps": [
            {"type": "pipeline", "enabled": True, "config": {}},
            {
                "type": "export",
                "enabled": True,
                "config": {"template_id": 3, "template_snapshot": export},
            },
        ],
        "export": {
            "template_id": 3,
            "template_name": "Export",
            "sendread_ab_list_id": "list-1",
        },
    }


class HookFixture:
    def __init__(self, get_db):
        self.get_db = get_db
        self.launch_calls = 0
        self.wake_calls = 0
        self.fail_launch = False
        self.fail_wake = False
        self.seen_plan = None
        self.hooks = ServiceHooks(
            prepare=self.prepare,
            launch=self.launch,
            after_commit=self.after_commit,
            list_templates=self.list_templates,
            get_campaign_status=self.campaign_status,
            get_run_status=self.run_status,
            stop_campaign=self.stop,
        )

    def prepare(self, cursor, payload):
        cursor.execute("SHOW transaction_read_only")
        assert cursor.fetchone()["transaction_read_only"] == "on"
        cursor.execute("SHOW transaction_isolation")
        assert cursor.fetchone()["transaction_isolation"] == "repeatable read"
        return frozen_plan(payload)

    def launch(self, cursor, plan):
        self.launch_calls += 1
        self.seen_plan = copy.deepcopy(plan)
        cursor.execute(
            "INSERT INTO test_campaigns (plan) VALUES (%s) RETURNING id", (Json(plan),)
        )
        campaign_id = cursor.fetchone()["id"]
        if self.fail_launch:
            raise RuntimeError("SECRET_LAUNCH_ERROR")
        return {
            "campaign_id": campaign_id,
            "run_id": campaign_id,
            "execution_mode": plan["execution_mode"],
            "private_worker_data": "SECRET_WORKER",
        }

    def after_commit(self, result):
        self.wake_calls += 1
        with self.get_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM test_campaigns WHERE id = %s",
                    (result["campaign_id"],),
                )
                assert cursor.fetchone() is not None
                cursor.execute(
                    "SELECT count(*) AS n FROM mcp_campaign_drafts WHERE launch_result IS NOT NULL"
                )
                assert cursor.fetchone()["n"] == 1
        assert result["private_worker_data"] == "SECRET_WORKER"
        if self.fail_wake:
            raise RuntimeError("SECRET_WAKE_ERROR")

    def list_templates(self, cursor, kind):
        return [
            {
                "id": 1,
                "name": "Template",
                "enabled": True,
                "api_key": "SECRET_KEY",
                "config": '{"url":"https://provider.invalid/?token=SECRET_TOKEN"}',
            }
        ]

    def campaign_status(self, cursor, campaign_id):
        cursor.execute(
            "SELECT stopped FROM test_campaigns WHERE id = %s", (campaign_id,)
        )
        row = cursor.fetchone()
        return {
            "campaign_id": campaign_id,
            "status": "stopped" if row["stopped"] else "running",
            "total_requests": 1,
            "api_url": "https://provider.invalid/?SECRET",
            "contacts": ["SECRET"],
        }

    def run_status(self, cursor, run_id):
        return {
            "run_id": run_id,
            "status": "running",
            "completed_steps": 0,
            "last_error": "SECRET_ERROR",
            "logs": ["SECRET_LOG"],
        }

    def stop(self, cursor, campaign_id):
        cursor.execute(
            "UPDATE test_campaigns SET stopped = TRUE WHERE id = %s", (campaign_id,)
        )
        return {
            "campaign_id": campaign_id,
            "status": "stopping",
            "stop_requested": True,
            "private": "SECRET_STOP",
        }


@pytest.fixture
def payload():
    return CampaignInput(
        name="Search",
        requests=["  niche service Austin TX  ", "niche service Austin TX"],
        source_template_id=1,
        funnel_template_id=2,
        export_template_id=3,
    )


@pytest.fixture
def integration(get_db):
    hooks = HookFixture(get_db)
    return CampaignService(get_db, hooks.hooks), hooks
