"""PostgreSQL endpoint tests, isolated from main, workers and preview databases.

Run with STREAM_TEST_DATABASE_URL pointing specifically at mapsdata_stream_test.
Each test creates and removes its own schema; no public-schema rows are touched.
"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import psycopg2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn
from psycopg2.extras import Json, RealDictCursor

import streaming_api


class RecoveryHarness:
    def __init__(self, dsn):
        self.dsn = dsn
        self.statements = []
        self.before_statement = None
        self.authenticated = True
        harness = self

        class RecordingCursor(RealDictCursor):
            def execute(self, query, parameters=None):
                normalized = " ".join(query.split())
                harness.statements.append((normalized, parameters))
                if harness.before_statement:
                    harness.before_statement(normalized, parameters)
                return super().execute(query, parameters)

        @contextmanager
        def get_db():
            connection = psycopg2.connect(dsn, cursor_factory=RecordingCursor)
            try:
                yield connection
            finally:
                connection.close()

        def append_log(cursor, run_id, campaign_id, message):
            cursor.execute(
                "INSERT INTO automation_run_logs(run_id, campaign_id, message) VALUES (%s, %s, %s)",
                (run_id, campaign_id, message),
            )

        self.adapter = SimpleNamespace(
            get_db=get_db,
            _is_ui_authenticated=lambda _request: self.authenticated,
            _append_automation_log=append_log,
            _ensure_automation_run_worker=Mock(
                side_effect=AssertionError("Recovery must not start workers")
            ),
        )
        self.app = FastAPI()
        self.app.include_router(streaming_api.router(self.adapter))
        self.client = TestClient(self.app)

    @contextmanager
    def connection(self):
        connection = psycopg2.connect(self.dsn, cursor_factory=RealDictCursor)
        try:
            yield connection
        finally:
            connection.close()

    def execute(self, query, parameters=()):
        with self.connection() as conn, conn.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = (
                [dict(row) for row in cursor.fetchall()] if cursor.description else []
            )
            conn.commit()
            return rows

    def row(self, table, row_id):
        return self.execute(
            sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(table)),
            (row_id,),
        )[0]

    def run(self, status="waiting_confirmation"):
        return self.execute(
            "INSERT INTO automation_runs(status, campaign_id) VALUES (%s, 1) RETURNING id",
            (status,),
        )[0]["id"]

    def contact(self):
        return self.execute(
            "INSERT INTO contacts(business_name, email) VALUES ('Fixture', 'fixture@example.test') RETURNING id"
        )[0]["id"]

    def task(
        self, run_id, contact_id, status="uncertain", delivery_id=None, kind="export"
    ):
        return self.execute(
            """INSERT INTO automation_stream_tasks(run_id, contact_id, status, step_type, result,
                       dispatched, lease_token, finished_token, lease_until)
               VALUES (%s, %s, %s, %s, %s, TRUE, 'old-worker', 'previous-worker',
                       clock_timestamp() + INTERVAL '1 minute') RETURNING id""",
            (
                run_id,
                contact_id,
                status,
                kind,
                Json({"delivery_id": delivery_id} if delivery_id else {}),
            ),
        )[0]["id"]

    def delivery(
        self, owner, contact, status="uncertain", destination="fixture-destination"
    ):
        return self.execute(
            """INSERT INTO automation_export_deliveries(task_id, contact_id, destination, status, attempt_token, receipt)
               VALUES (%s, %s, %s, %s, 'old-worker', '{"diagnostic":"retained"}') RETURNING id""",
            (owner, contact, destination, status),
        )[0]["id"]

    def case(
        self,
        owner_run_status="waiting_confirmation",
        owner_status="uncertain",
        receipt_status="uncertain",
    ):
        contact = self.contact()
        owner_run, reference_run = self.run(owner_run_status), self.run()
        owner = self.task(owner_run, contact, owner_status)
        delivery = self.delivery(owner, contact, receipt_status)
        reference = self.task(reference_run, contact, delivery_id=delivery)
        return SimpleNamespace(
            contact=contact,
            owner_run=owner_run,
            reference_run=reference_run,
            owner=owner,
            delivery=delivery,
            reference=reference,
        )

    def resolve(self, case, resolution="delivered", *, owner=False, **extra):
        run_id, task_id = (
            (case.owner_run, case.owner)
            if owner
            else (case.reference_run, case.reference)
        )
        return self.client.post(
            f"/api/funnel-runs/{run_id}/deliveries/{task_id}/resolve",
            json={"confirmed": True, "resolution": resolution, **extra},
        )


@pytest.fixture
def recovery():
    dsn = os.environ.get("STREAM_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip(
            "Set STREAM_TEST_DATABASE_URL to the dedicated mapsdata_stream_test database"
        )
    if parse_dsn(dsn).get("dbname") != "mapsdata_stream_test":
        pytest.fail(
            "Recovery tests only run against mapsdata_stream_test, never the preview database"
        )
    schema = "stream_api_recovery_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        assert cursor.fetchone()[0] == "mapsdata_stream_test"
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped_dsn = make_dsn(
        dsn,
        options=f"-c search_path={schema} -c lock_timeout=4000 -c statement_timeout=6000",
    )
    harness = RecoveryHarness(scoped_dsn)
    try:
        harness.execute("""
            CREATE TABLE automation_runs (
                id BIGSERIAL PRIMARY KEY, campaign_id INTEGER NOT NULL,
                execution_mode TEXT NOT NULL DEFAULT 'streaming', status TEXT NOT NULL,
                latest_error TEXT
            );
            CREATE TABLE contacts (id BIGSERIAL PRIMARY KEY, business_name TEXT, email TEXT);
            CREATE TABLE automation_stream_tasks (
                id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL REFERENCES automation_runs(id),
                contact_id BIGINT NOT NULL REFERENCES contacts(id), step_type TEXT NOT NULL,
                status TEXT NOT NULL, result JSONB NOT NULL DEFAULT '{}', error TEXT,
                dispatched BOOLEAN NOT NULL DEFAULT FALSE, lease_token TEXT, finished_token TEXT,
                lease_until TIMESTAMPTZ, available_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE automation_export_deliveries (
                id BIGSERIAL PRIMARY KEY, task_id BIGINT NOT NULL REFERENCES automation_stream_tasks(id),
                contact_id BIGINT NOT NULL REFERENCES contacts(id), destination TEXT NOT NULL,
                status TEXT NOT NULL, attempt_token TEXT, receipt JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, UNIQUE(contact_id, destination)
            );
            CREATE TABLE automation_run_logs (
                id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL REFERENCES automation_runs(id),
                campaign_id INTEGER NOT NULL, message TEXT NOT NULL
            );
        """)
        yield harness
        harness.adapter._ensure_automation_run_worker.assert_not_called()
    finally:
        harness.client.close()
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
        admin.close()


def test_review_uses_canonical_id_and_exposes_owner(recovery):
    case = recovery.case()
    response = recovery.client.get(
        f"/api/funnel-runs/{case.reference_run}/deliveries/uncertain"
    )
    assert response.status_code == 200
    item = response.json()["deliveries"][0]
    assert item["task_id"] == case.reference
    assert item["delivery_id"] == case.delivery
    assert item["owner_task_id"] == case.owner
    assert item["owner_run_id"] == case.owner_run
    assert item["delivery_status"] == "uncertain"
    assert item["receipt"] == {"diagnostic": "retained"}


def test_legacy_owner_fallback_and_resolution(recovery):
    case = recovery.case()
    item = recovery.client.get(
        f"/api/funnel-runs/{case.owner_run}/deliveries/uncertain"
    ).json()["deliveries"][0]
    assert item["delivery_id"] == case.delivery
    response = recovery.resolve(case, owner=True)
    assert response.status_code == 200, response.text
    assert (
        recovery.row("automation_stream_tasks", case.reference)["status"] == "skipped"
    )


@pytest.mark.parametrize(
    "resolution,receipt_status,owner_status,reference_status",
    [
        ("delivered", "exported", "completed", "skipped"),
        ("not_delivered", "failed", "retry", "retry"),
    ],
)
def test_resolution_settles_owner_and_references_across_runs(
    recovery, resolution, receipt_status, owner_status, reference_status
):
    case = recovery.case()
    third_run = recovery.run()
    third = recovery.task(third_run, case.contact, delivery_id=case.delivery)
    unrelated = recovery.task(third_run, recovery.contact())
    response = recovery.resolve(case, resolution, delivery_id=case.delivery)
    assert response.status_code == 200, response.text
    assert response.json()["task_ids"] == [case.owner, case.reference, third]
    receipt = recovery.row("automation_export_deliveries", case.delivery)
    assert receipt["status"] == receipt_status
    assert receipt["attempt_token"] is None
    assert receipt["receipt"]["diagnostic"] == "retained"
    assert receipt["receipt"]["manual_resolution"] == resolution
    for task_id, expected in (
        (case.owner, owner_status),
        (case.reference, reference_status),
        (third, reference_status),
    ):
        task = recovery.row("automation_stream_tasks", task_id)
        assert task["status"] == expected
        assert (
            task["lease_token"] is task["lease_until"] is task["finished_token"] is None
        )
        assert not task["dispatched"]
        assert task["result"]["delivery_id"] == case.delivery
        assert task["result"]["delivery_status"] == receipt_status
    assert recovery.row("automation_stream_tasks", unrelated)["status"] == "uncertain"
    assert {
        row["run_id"]
        for row in recovery.execute("SELECT run_id FROM automation_run_logs")
    } == {case.owner_run, case.reference_run, third_run}
    assert {
        row["status"] for row in recovery.execute("SELECT status FROM automation_runs")
    } == {"waiting_confirmation"}


@pytest.mark.parametrize("resolution", ["delivered", "not_delivered"])
@pytest.mark.parametrize("owner_status", ["cancelled", "uncertain", "running"])
def test_stop_remains_terminal_during_resolution(recovery, resolution, owner_status):
    case = recovery.case(owner_run_status="cancelled", owner_status=owner_status)
    response = recovery.resolve(case, resolution)
    assert response.status_code == 200, response.text
    assert recovery.row("automation_runs", case.owner_run)["status"] == "cancelled"
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "cancelled"
    assert recovery.row("automation_export_deliveries", case.delivery)["status"] == (
        "exported" if resolution == "delivered" else "failed"
    )
    resumed = recovery.client.post(f"/api/funnel-runs/{case.owner_run}/resume")
    assert resumed.status_code == 409


def test_cancelled_owner_can_be_reviewed_directly(recovery):
    case = recovery.case(owner_run_status="cancelled", owner_status="cancelled")
    items = recovery.client.get(
        f"/api/funnel-runs/{case.owner_run}/deliveries/uncertain"
    ).json()["deliveries"]
    assert items[0]["delivery_id"] == case.delivery
    assert recovery.resolve(case, "not_delivered", owner=True).status_code == 200
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "cancelled"


def test_cancelled_reference_and_closed_owner_are_not_requeued(recovery):
    case = recovery.case(owner_run_status="completed", owner_status="failed")
    recovery.execute(
        "UPDATE automation_runs SET status='cancelled' WHERE id=%s",
        (case.reference_run,),
    )
    assert recovery.resolve(case, "not_delivered").status_code == 200
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "failed"
    assert (
        recovery.row("automation_stream_tasks", case.reference)["status"] == "cancelled"
    )
    assert recovery.row("automation_runs", case.owner_run)["status"] == "completed"


def test_explicit_resolution_fences_late_provider_receipt(recovery):
    case = recovery.case(
        owner_run_status="running", owner_status="running", receipt_status="sending"
    )
    assert recovery.resolve(case, "not_delivered").status_code == 200
    assert (
        recovery.execute(
            "UPDATE automation_export_deliveries SET status='exported' WHERE id=%s AND attempt_token='old-worker' RETURNING id",
            (case.delivery,),
        )
        == []
    )
    assert (
        recovery.execute(
            "UPDATE automation_stream_tasks SET status='completed' WHERE id=%s AND lease_token='old-worker' RETURNING id",
            (case.owner,),
        )
        == []
    )
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["status"]
        == "failed"
    )


@pytest.mark.parametrize(
    "receipt_status,resolution",
    [
        ("exported", "delivered"),
        ("failed", "not_delivered"),
        ("filtered", "not_delivered"),
    ],
)
def test_reference_can_settle_after_owner_receipt_finishes(
    recovery, receipt_status, resolution
):
    case = recovery.case(
        receipt_status=receipt_status,
        owner_status="completed" if receipt_status == "exported" else "failed",
    )
    assert recovery.resolve(case, resolution).status_code == 200


@pytest.mark.parametrize(
    "receipt_status,resolution",
    [("exported", "not_delivered"), ("failed", "delivered"), ("reserved", "delivered")],
)
def test_resolution_cannot_contradict_terminal_or_undispatched_receipt(
    recovery, receipt_status, resolution
):
    case = recovery.case(receipt_status=receipt_status)
    assert recovery.resolve(case, resolution).status_code == 409
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["status"]
        == receipt_status
    )


@pytest.mark.parametrize("reference", [None, "not-an-id", 999999999, {"id": 1}])
def test_missing_or_malformed_canonical_receipt_is_not_silently_resolved(
    recovery, reference
):
    case = recovery.case()
    recovery.execute(
        "UPDATE automation_stream_tasks SET result=%s WHERE id=%s",
        (
            Json({"delivery_id": reference} if reference is not None else {}),
            case.reference,
        ),
    )
    assert recovery.resolve(case).status_code == 409
    assert (
        recovery.row("automation_stream_tasks", case.reference)["status"] == "uncertain"
    )
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["status"]
        == "uncertain"
    )


def test_explicit_reference_never_falls_back_to_own_receipt(recovery):
    case = recovery.case()
    recovery.execute(
        "UPDATE automation_stream_tasks SET result=%s WHERE id=%s",
        (Json({"delivery_id": 999999999}), case.owner),
    )
    assert recovery.resolve(case, owner=True).status_code == 409


def test_mismatched_contact_or_preview_receipt_is_rejected(recovery):
    case = recovery.case()
    assert recovery.resolve(case, delivery_id=case.delivery + 1).status_code == 409
    other = recovery.contact()
    recovery.execute(
        "UPDATE automation_stream_tasks SET contact_id=%s WHERE id=%s",
        (other, case.reference),
    )
    assert recovery.resolve(case).status_code == 409


def test_ambiguous_legacy_receipts_are_rejected(recovery):
    case = recovery.case()
    recovery.delivery(case.owner, case.contact, destination="second-destination")
    assert recovery.resolve(case, owner=True).status_code == 409
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "uncertain"


def test_authorization_confirmation_and_run_membership_are_required(recovery):
    case = recovery.case()
    recovery.authenticated = False
    assert recovery.resolve(case).status_code == 401
    assert (
        recovery.client.get(
            f"/api/funnel-runs/{case.reference_run}/deliveries/uncertain"
        ).status_code
        == 401
    )
    recovery.authenticated = True
    assert recovery.resolve(case, confirmed="true").status_code == 400
    assert (
        recovery.client.post(
            f"/api/funnel-runs/{case.owner_run}/deliveries/{case.reference}/resolve",
            json={"confirmed": True, "resolution": "delivered"},
        ).status_code
        == 409
    )


def test_review_locks_sorted_runs_then_tasks_then_receipt(recovery):
    case = recovery.case()
    assert recovery.resolve(case).status_code == 200
    locks = [
        (query, parameters)
        for query, parameters in recovery.statements
        if "FOR UPDATE" in query
    ]
    assert len(locks) == 3
    assert "FROM automation_runs" in locks[0][0]
    assert locks[0][1] == ([case.owner_run, case.reference_run],)
    assert "FROM automation_stream_tasks" in locks[1][0]
    assert locks[1][1] == ([case.owner, case.reference],)
    assert "FROM automation_export_deliveries" in locks[2][0]


@pytest.mark.parametrize("when", ["runs", "receipt"])
def test_new_cross_run_reference_during_locking_requires_refresh(recovery, when):
    case = recovery.case()
    new_run = recovery.run()
    inserted = []

    def interleave(query, _parameters):
        target = (
            "FROM automation_runs"
            if when == "runs"
            else "FROM automation_export_deliveries"
        )
        if not inserted and target in query and "FOR UPDATE" in query:
            inserted.append(
                recovery.task(new_run, case.contact, delivery_id=case.delivery)
            )

    recovery.before_statement = interleave
    assert recovery.resolve(case).status_code == 409
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["status"]
        == "uncertain"
    )
    assert recovery.row("automation_stream_tasks", inserted[0])["status"] == "uncertain"


def test_canonical_ownership_change_is_revalidated(recovery):
    case = recovery.case()
    replacement = recovery.task(recovery.run(), case.contact, status="running")
    changed = []

    def interleave(query, _parameters):
        if not changed and "FROM automation_runs" in query and "FOR UPDATE" in query:
            recovery.execute(
                "UPDATE automation_export_deliveries SET task_id=%s, attempt_token='new-worker' WHERE id=%s",
                (replacement, case.delivery),
            )
            changed.append(True)

    recovery.before_statement = interleave
    assert recovery.resolve(case).status_code == 409
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["attempt_token"]
        == "new-worker"
    )


def test_task_reference_change_is_revalidated(recovery):
    case = recovery.case()
    replacement_owner = recovery.task(recovery.run(), case.contact)
    replacement = recovery.delivery(
        replacement_owner, case.contact, destination="another-destination"
    )
    changed = []

    def interleave(query, _parameters):
        if not changed and "FROM automation_runs" in query and "FOR UPDATE" in query:
            recovery.execute(
                "UPDATE automation_stream_tasks SET result=%s WHERE id=%s",
                (Json({"delivery_id": replacement}), case.reference),
            )
            changed.append(True)

    recovery.before_statement = interleave
    assert recovery.resolve(case).status_code == 409
    assert (
        recovery.row("automation_export_deliveries", case.delivery)["status"]
        == "uncertain"
    )
    assert (
        recovery.row("automation_export_deliveries", replacement)["status"]
        == "uncertain"
    )


@pytest.mark.parametrize(
    "table", ["automation_export_deliveries", "automation_stream_tasks"]
)
def test_zero_row_update_rolls_back_entire_resolution(recovery, table):
    case = recovery.case()
    condition = (
        "TRUE"
        if table == "automation_export_deliveries"
        else f"NEW.id = {case.reference}"
    )
    recovery.execute(f"""
        CREATE FUNCTION suppress_resolution() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF {condition} THEN RETURN NULL; END IF; RETURN NEW; END $$;
        CREATE TRIGGER suppress_resolution BEFORE UPDATE ON {table}
        FOR EACH ROW EXECUTE FUNCTION suppress_resolution();
    """)
    assert recovery.resolve(case).status_code == 409
    receipt = recovery.row("automation_export_deliveries", case.delivery)
    assert receipt["status"] == "uncertain"
    assert receipt["attempt_token"] == "old-worker"
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "uncertain"
    assert recovery.execute("SELECT * FROM automation_run_logs") == []


def test_concurrent_reviewers_do_not_deadlock_or_resolve_twice(recovery):
    case = recovery.case()
    barrier = threading.Barrier(2, timeout=3)

    def interleave(query, _parameters):
        if "FROM automation_runs" in query and "FOR UPDATE" in query:
            barrier.wait()

    recovery.before_statement = interleave
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(recovery.resolve, case, "delivered", owner=True)
        second = workers.submit(recovery.resolve, case, "not_delivered")
        statuses = [
            first.result(timeout=8).status_code,
            second.result(timeout=8).status_code,
        ]
    assert sorted(statuses) == [200, 409]


def test_stop_holding_run_lock_does_not_deadlock_with_review(recovery):
    case = recovery.case()
    reached_run_lock = threading.Event()

    def interleave(query, _parameters):
        if "FROM automation_runs" in query and "FOR UPDATE" in query:
            reached_run_lock.set()

    recovery.before_statement = interleave
    with ThreadPoolExecutor(max_workers=1) as workers, recovery.connection() as stop:
        cursor = stop.cursor()
        cursor.execute(
            "SELECT id FROM automation_runs WHERE id=%s FOR UPDATE", (case.owner_run,)
        )
        pending = workers.submit(recovery.resolve, case, "not_delivered")
        try:
            assert reached_run_lock.wait(3)
            cursor.execute(
                "UPDATE automation_runs SET status='cancelled' WHERE id=%s",
                (case.owner_run,),
            )
            cursor.execute(
                "UPDATE automation_stream_tasks SET status='cancelled' WHERE id=%s",
                (case.owner,),
            )
            stop.commit()
            assert pending.result(timeout=6).status_code == 200
        finally:
            stop.rollback()
    assert recovery.row("automation_stream_tasks", case.owner)["status"] == "cancelled"


@pytest.mark.parametrize(
    "run_status,expired,expected",
    [
        ("running", False, True),
        ("waiting_confirmation", False, True),
        ("waiting_confirmation", True, False),
        ("running", True, False),
        ("cancelled", False, False),
    ],
)
def test_source_heartbeat_keeps_live_work_but_cannot_revive_expired_lease(
    recovery, run_status, expired, expected
):
    run_id = recovery.run(run_status)
    task_id = recovery.task(
        run_id, recovery.contact(), status="running", kind="source_email"
    )
    if expired:
        recovery.execute(
            "UPDATE automation_stream_tasks SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=%s",
            (task_id,),
        )
    before = recovery.row("automation_stream_tasks", task_id)["lease_until"]
    response = recovery.client.post(
        f"/api/streaming/source-tasks/{task_id}/heartbeat",
        json={"lease_token": "old-worker"},
    )
    assert response.status_code == 200
    assert response.json() == {"active": expected}
    after = recovery.row("automation_stream_tasks", task_id)["lease_until"]
    assert after > before if expected else after == before


@pytest.mark.parametrize(
    "change", ["missing_deadline", "wrong_token", "cancelled_task"]
)
def test_source_heartbeat_requires_current_live_task_lease(recovery, change):
    run_id = recovery.run()
    task_id = recovery.task(
        run_id, recovery.contact(), status="running", kind="source_email"
    )
    token = "old-worker"
    if change == "missing_deadline":
        recovery.execute(
            "UPDATE automation_stream_tasks SET lease_until=NULL WHERE id=%s",
            (task_id,),
        )
    elif change == "wrong_token":
        token = "stale-worker"
    else:
        recovery.execute(
            "UPDATE automation_stream_tasks SET status='cancelled' WHERE id=%s",
            (task_id,),
        )
    response = recovery.client.post(
        f"/api/streaming/source-tasks/{task_id}/heartbeat", json={"lease_token": token}
    )
    assert response.json() == {"active": False}
