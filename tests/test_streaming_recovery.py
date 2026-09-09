"""Opt-in SQL/race tests using only a self-created, temporary PostgreSQL cluster.

Run with RUN_ISOLATED_RECOVERY_TESTS=1. DATABASE_URL and all existing test DSNs
are deliberately ignored. Neither main nor any provider integration is loaded.
"""

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from queue import Queue

import psycopg2
from psycopg2.extras import Json, RealDictCursor

import streaming


@unittest.skipUnless(os.environ.get("RUN_ISOLATED_RECOVERY_TESTS") == "1", "Requires opt-in private PostgreSQL cluster")
class StreamingRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("pg_config") or os.geteuid() == 0:
            raise unittest.SkipTest("Requires local PostgreSQL binaries and a non-root user")
        bindir = Path(subprocess.check_output(["pg_config", "--bindir"], text=True).strip())
        cls.pg_ctl = bindir / "pg_ctl"
        cls.temporary = tempfile.TemporaryDirectory(prefix="stream-recovery-")
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        cls.data = cls.directory / "data"
        subprocess.run(
            [str(bindir / "initdb"), "-D", str(cls.data), "-A", "trust", "-U", "recovery_test", "--no-locale"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        # TCP is disabled; the socket directory is private and newly allocated.
        subprocess.run(
            [str(cls.pg_ctl), "-D", str(cls.data), "-l", str(cls.directory / "postgres.log"),
             "-o", f"-F -k {cls.directory} -h '' -p 55439", "-w", "start"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        cls.addClassCleanup(cls.stop_cluster)
        cls.connect_args = {"host": str(cls.directory), "port": 55439, "user": "recovery_test",
                            "dbname": "postgres", "cursor_factory": RealDictCursor}
        with cls.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE automation_funnel_templates (id INTEGER PRIMARY KEY);
                CREATE TABLE search_campaigns (id INTEGER PRIMARY KEY);
                CREATE TABLE automation_runs (id INTEGER PRIMARY KEY, campaign_id INTEGER,
                    status TEXT, latest_error TEXT, completed_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
                CREATE TABLE automation_run_steps (id INTEGER PRIMARY KEY, run_id INTEGER, campaign_id INTEGER,
                    step_order INTEGER, step_type TEXT, status TEXT, config JSONB DEFAULT '{}',
                    started_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
                CREATE TABLE contacts (id INTEGER PRIMARY KEY, email TEXT, email_status TEXT, full_name TEXT);
                CREATE TABLE enrichment_api_rate_limits (endpoint_key TEXT PRIMARY KEY,
                    next_request_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE automation_run_logs (run_id INTEGER, campaign_id INTEGER,
                    step_id INTEGER, level TEXT, message TEXT);
            """)
            streaming.init_schema(cursor)
            # Existing installations must get attempt_token through ALTER as well.
            cursor.execute("ALTER TABLE automation_export_deliveries DROP COLUMN attempt_token")
            streaming.init_schema(cursor)

    @classmethod
    def stop_cluster(cls):
        subprocess.run([str(cls.pg_ctl), "-D", str(cls.data), "-m", "immediate", "-w", "stop"],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    @classmethod
    @contextmanager
    def connection(cls):
        with closing(psycopg2.connect(**cls.connect_args)) as conn, conn:
            yield conn

    def execute(self, query, params=(), one=True):
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if cursor.description:
                return cursor.fetchone() if one else cursor.fetchall()

    def setUp(self):
        self.execute("""
            TRUNCATE automation_runs, automation_run_steps, contacts, search_campaigns,
                automation_funnel_templates, automation_run_logs, enrichment_api_rate_limits RESTART IDENTITY CASCADE;
            INSERT INTO search_campaigns(id) VALUES (1);
            INSERT INTO automation_runs(id, campaign_id, status) VALUES (1, 1, 'running');
            INSERT INTO automation_run_steps(id, run_id, campaign_id, step_order, step_type, status)
                VALUES (2, 1, 1, 1, 'export', 'running');
            INSERT INTO contacts(id, email, email_status) VALUES (3, 'lead@example.org', 'valid');
            INSERT INTO enrichment_api_rate_limits(endpoint_key) VALUES ('provider');
        """)
        contact = self.execute("SELECT * FROM contacts WHERE id = 3")
        self.task = self.execute("""
            INSERT INTO automation_stream_tasks (id, run_id, step_id, contact_id, step_type,
                status, attempts, max_retries, lease_token, lease_until, input_data, bucket)
            VALUES (4, 1, 2, 3, 'export', 'running', 1, 2, 'attempt-a',
                clock_timestamp() + INTERVAL '30 seconds', %s, 'provider') RETURNING *
        """, (Json(streaming.contact_input(contact)),))

    def row(self):
        return self.execute("SELECT * FROM automation_stream_tasks WHERE id = 4")

    def run_status(self):
        return self.execute("SELECT status FROM automation_runs WHERE id = 1")["status"]

    def finish(self, outcome, task=None):
        with self.connection() as conn:
            return streaming.finish(conn.cursor(), self.task if task is None else task, outcome)

    def delivery(self, status="reserved", receipt=None, token="attempt-a"):
        return self.execute("""
            INSERT INTO automation_export_deliveries(task_id, contact_id, destination, status, attempt_token, receipt)
            VALUES (4, 3, 'destination', %s, %s, %s) RETURNING *
        """, (status, token, Json(receipt or {})))

    def delivery_row(self):
        return self.execute("SELECT * FROM automation_export_deliveries WHERE task_id = 4")

    def expire(self, dispatched=False):
        self.execute("UPDATE automation_stream_tasks SET lease_until = clock_timestamp() - INTERVAL '1 second', dispatched = %s WHERE id = 4", (dispatched,))

    def recover(self, stopped=False):
        with self.connection() as conn:
            if stopped:
                streaming.recover_stopped(conn.cursor())
            else:
                streaming.recover(conn.cursor(), 1)

    def assert_waiting_for_lock(self, pid):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            state = self.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,))
            if state and state["wait_event_type"] == "Lock":
                return
            time.sleep(0.01)
        self.fail("Worker did not wait for the held run lock")

    def test_schema_migrates_existing_delivery_table(self):
        row = self.delivery()
        self.assertEqual(row["attempt_token"], "attempt-a")

    def test_schema_adds_nullable_dispatch_timestamp_and_preserves_existing_value(self):
        row = self.execute("SELECT last_dispatch_at FROM enrichment_api_rate_limits WHERE endpoint_key = 'provider'")
        self.assertIsNone(row["last_dispatch_at"])
        before = self.execute("""UPDATE enrichment_api_rate_limits SET last_dispatch_at = clock_timestamp()
                                 WHERE endpoint_key = 'provider' RETURNING *""")
        with self.connection() as conn:
            streaming.init_schema(conn.cursor())
        self.assertEqual(self.execute("SELECT * FROM enrichment_api_rate_limits WHERE endpoint_key = 'provider'"), before)

    def test_heartbeat_renews_valid_active_and_paused_leases(self):
        for status in ("queued", "running", "waiting_confirmation"):
            with self.subTest(status=status):
                self.setUp()
                self.execute("UPDATE automation_runs SET status = %s WHERE id = 1", (status,))
                before = self.row()
                with self.connection() as conn:
                    streaming.heartbeat(conn.cursor(), [self.task])
                after = self.row()
                self.assertGreater(after["lease_until"], before["lease_until"])
                self.assertEqual(after["lease_token"], before["lease_token"])
                self.assertEqual(self.run_status(), status)

    def test_heartbeat_cannot_revive_expired_or_missing_lease(self):
        for status in ("running", "waiting_confirmation"):
            for missing in (False, True):
                with self.subTest(status=status, missing=missing):
                    self.setUp()
                    self.execute("UPDATE automation_runs SET status = %s WHERE id = 1", (status,))
                    self.expire()
                    if missing:
                        self.execute("UPDATE automation_stream_tasks SET lease_until = NULL WHERE id = 4")
                    before = self.row()
                    with self.connection() as conn:
                        streaming.heartbeat(conn.cursor(), [self.task])
                    self.assertEqual(self.row(), before)
                    self.assertFalse(self.finish({"status": "completed"}))

    def test_heartbeat_uses_wall_clock_in_older_transaction(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pg_sleep(0.03)")
            self.execute("""UPDATE automation_stream_tasks
                            SET lease_until = clock_timestamp() - INTERVAL '0.001 seconds' WHERE id = 4""")
            before = self.row()
            cursor.execute("SELECT CURRENT_TIMESTAMP < lease_until AS valid_at_start FROM automation_stream_tasks WHERE id = 4")
            self.assertTrue(cursor.fetchone()["valid_at_start"])
            streaming.heartbeat(cursor, [self.task])
        self.assertEqual(self.row(), before)

    def test_heartbeat_keeps_token_status_and_terminal_run_fences(self):
        for query in (
            "UPDATE automation_stream_tasks SET lease_token = 'attempt-b' WHERE id = 4",
            "UPDATE automation_stream_tasks SET status = 'completed' WHERE id = 4",
            "UPDATE automation_runs SET status = 'cancelled' WHERE id = 1",
            "UPDATE automation_runs SET status = 'completed' WHERE id = 1",
            "UPDATE automation_runs SET status = 'failed' WHERE id = 1",
        ):
            with self.subTest(query=query):
                self.setUp()
                self.execute(query)
                before = self.row()
                with self.connection() as conn:
                    streaming.heartbeat(conn.cursor(), [self.task])
                self.assertEqual(self.row(), before)

    def test_finish_waits_for_stop_run_lock_and_does_not_write_contacts_or_resurrect_run(self):
        self.execute("UPDATE automation_stream_tasks SET step_type = 'enrichment' WHERE id = 4")
        pids = Queue()

        def worker():
            with self.connection() as conn:
                pids.put(conn.get_backend_pid())
                return streaming.finish(conn.cursor(), self.task, {
                    "status": "blocked", "updates": {"full_name": "Must not be saved"}, "error": "Auth rejected",
                })

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.connection() as stopping:
                stopping.cursor().execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
                future = pool.submit(worker)
                self.assert_waiting_for_lock(pids.get(timeout=4))
            self.assertTrue(future.result(timeout=4))
        self.assertEqual(self.run_status(), "cancelled")
        self.assertEqual(self.row()["status"], "cancelled")
        self.assertIsNone(self.execute("SELECT full_name FROM contacts WHERE id = 3")["full_name"])

    def test_finish_rejects_expired_lease_and_writes_nothing(self):
        self.expire()
        before = self.row()
        self.assertFalse(self.finish({"status": "completed"}))
        self.assertEqual(self.row(), before)
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM automation_run_logs")["n"], 0)

    def test_lease_expiring_while_waiting_for_run_lock_is_rejected(self):
        pids = Queue()

        def worker():
            with self.connection() as conn:
                pids.put(conn.get_backend_pid())
                return streaming.finish(conn.cursor(), self.task, {"status": "completed"})

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.connection() as lock:
                cursor = lock.cursor()
                cursor.execute("SELECT id FROM automation_runs WHERE id = 1 FOR UPDATE")
                future = pool.submit(worker)
                self.assert_waiting_for_lock(pids.get(timeout=4))
                cursor.execute("UPDATE automation_stream_tasks SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = 4")
            self.assertFalse(future.result(timeout=4))
        self.assertEqual(self.row()["status"], "running")

    def test_concurrent_completion_replay_succeeds_once(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.finish, {"status": "completed", "result": {"delivery_id": 91}}) for _ in range(2)]
            self.assertEqual([future.result(timeout=4) for future in futures], [True, True])
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM automation_run_logs")["n"], 1)
        self.assertEqual(self.row()["finished_token"], "attempt-a")

    def test_old_token_cannot_finish_new_running_attempt(self):
        self.assertTrue(self.finish({"status": "retry"}))
        self.execute("UPDATE automation_stream_tasks SET status = 'running', lease_token = 'attempt-b' WHERE id = 4")
        self.assertFalse(self.finish({"status": "completed"}))
        self.assertEqual(self.row()["lease_token"], "attempt-b")

    def test_explicit_retry_and_deferred_refund_attempt(self):
        for state in ("retry", "deferred"):
            with self.subTest(state=state):
                self.setUp()
                self.assertTrue(self.finish({"status": state, "result": {"reason": "paused"}}))
                self.assertEqual((self.row()["status"], self.row()["attempts"]), ("retry", 0))

    def test_explicit_retry_can_opt_into_attempt_budget(self):
        self.execute("UPDATE automation_stream_tasks SET attempts = 3 WHERE id = 4")
        self.assertTrue(self.finish({"status": "retry", "count_attempt": True}))
        self.assertEqual((self.row()["status"], self.row()["attempts"]), ("failed", 3))

    def test_failed_attempt_retries_by_default(self):
        self.assertTrue(self.finish({"status": "failed", "error": "Temporary failure"}))
        self.assertEqual((self.row()["status"], self.row()["attempts"]), ("retry", 1))

    def test_retryable_false_at_either_level_prevents_retry(self):
        for outcome in ({"status": "failed", "retryable": False},
                        {"status": "failed", "result": {"retryable": False}},
                        {"status": "failed", "retryable": True, "result": {"retryable": False}}):
            with self.subTest(outcome=outcome):
                self.setUp()
                self.assertTrue(self.finish(outcome))
                self.assertEqual(self.row()["status"], "failed")

    def test_unknown_receipt_never_becomes_retry(self):
        self.assertTrue(self.finish({"status": "retry", "result": {"status": "unknown", "delivery_id": 90}}))
        self.assertEqual(self.row()["status"], "uncertain")
        self.assertEqual(self.run_status(), "waiting_confirmation")

    def test_terminal_run_keeps_uncertain_export_and_canonical_metadata(self):
        for state in streaming.TERMINAL_RUNS:
            with self.subTest(state=state):
                self.setUp()
                self.execute("UPDATE automation_runs SET status = %s WHERE id = 1", (state,))
                result = {"status": "unknown", "delivery_id": 89, "provider_response": {"created": 1}}
                self.assertTrue(self.finish({"status": "uncertain", "result": result}))
                self.assertEqual(self.run_status(), state)
                self.assertEqual((self.row()["status"], self.row()["result"]), ("uncertain", result))

    def test_stop_preserves_confirmed_export_and_cancels_deferred_work(self):
        for state, expected in (("completed", "completed"), ("retry", "cancelled"), ("blocked", "cancelled")):
            with self.subTest(state=state):
                self.setUp()
                self.execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
                self.assertTrue(self.finish({"status": state, "result": {"delivery_id": 81}}))
                self.assertEqual(self.row()["status"], expected)
                self.assertEqual(self.run_status(), "cancelled")

    def test_existing_delivery_id_survives_outcome_without_metadata(self):
        self.execute("UPDATE automation_stream_tasks SET result = %s WHERE id = 4", (Json({"delivery_id": 99}),))
        self.assertTrue(self.finish({"status": "retry"}))
        self.assertEqual(self.row()["result"]["delivery_id"], 99)

    def test_retry_after_updates_shared_provider_limiter(self):
        self.assertTrue(self.finish({"status": "failed", "retry_after": 60}))
        row = self.execute("SELECT next_request_at > clock_timestamp() + INTERVAL '55 seconds' AS cooling FROM enrichment_api_rate_limits WHERE endpoint_key = 'provider'")
        self.assertTrue(row["cooling"])

    def test_cancel_releases_reserved_but_not_sending_deliveries(self):
        for state in ("reserved", "sending"):
            with self.subTest(state=state):
                self.setUp()
                self.delivery(state)
                self.execute("UPDATE automation_stream_tasks SET dispatched = %s WHERE id = 4", (state == "sending",))
                with self.connection() as conn:
                    streaming.cancel(conn.cursor(), 1)
                self.assertEqual(self.delivery_row()["status"], "failed" if state == "reserved" else "sending")
                if state == "reserved":
                    self.assertIsNone(self.delivery_row()["attempt_token"])
                    self.assertFalse(self.delivery_row()["receipt"]["attempted"])

    def test_recovery_releases_expired_reserved_delivery(self):
        delivery = self.delivery()
        self.expire()
        self.recover()
        self.assertEqual(self.row()["status"], "retry")
        self.assertEqual(self.row()["result"]["delivery_id"], delivery["id"])
        self.assertEqual(self.delivery_row()["status"], "failed")
        self.assertIsNone(self.delivery_row()["attempt_token"])

    def test_recovery_releases_old_token_reservation_but_preserves_live_owner(self):
        self.delivery()
        self.recover()
        self.assertEqual(self.delivery_row()["status"], "reserved")
        self.execute("UPDATE automation_stream_tasks SET lease_token = 'attempt-b' WHERE id = 4")
        self.recover()
        self.assertEqual(self.delivery_row()["status"], "failed")
        self.assertEqual(self.row()["status"], "running")

    def test_deferred_completion_releases_its_reservation(self):
        self.delivery()
        self.assertTrue(self.finish({"status": "retry"}))
        self.assertEqual(self.delivery_row()["status"], "failed")

    def test_stopped_recovery_preserves_uncertainty_without_resuming(self):
        delivery = self.delivery("sending")
        self.execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
        self.expire(dispatched=True)
        self.recover(stopped=True)
        self.assertEqual(self.row()["status"], "uncertain")
        self.assertEqual(self.row()["result"]["delivery_id"], delivery["id"])
        self.assertEqual(self.delivery_row()["status"], "uncertain")
        self.assertEqual(self.run_status(), "cancelled")

    def test_stopped_recovery_waits_for_run_before_locking_task(self):
        self.delivery("sending")
        self.execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
        self.expire(dispatched=True)
        pids = Queue()

        def worker():
            with self.connection() as conn:
                pids.put(conn.get_backend_pid())
                streaming.recover_stopped(conn.cursor())

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.connection() as holding:
                holding.cursor().execute("SELECT id FROM automation_runs WHERE id = 1 FOR UPDATE")
                future = pool.submit(worker)
                self.assert_waiting_for_lock(pids.get(timeout=4))
                with self.connection() as observer:
                    observer.cursor().execute("SELECT id FROM automation_stream_tasks WHERE id = 4 FOR UPDATE NOWAIT")
            future.result(timeout=4)
        self.assertEqual(self.row()["status"], "uncertain")
        self.assertEqual(self.run_status(), "cancelled")

    def test_stopped_recovery_releases_unfinished_reservations(self):
        self.delivery()
        self.execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
        self.expire()
        self.recover(stopped=True)
        self.assertEqual((self.row()["status"], self.delivery_row()["status"]), ("cancelled", "failed"))

    def test_paused_sweep_recovers_expired_work_without_resuming(self):
        for state, task_status, delivery_status in (
            ("reserved", "retry", "failed"),
            ("sending", "uncertain", "uncertain"),
            ("exported", "completed", "exported"),
        ):
            with self.subTest(state=state):
                self.setUp()
                delivery = self.delivery(state, {"status": state})
                self.execute("UPDATE automation_runs SET status = 'waiting_confirmation', latest_error = 'Paused by user' WHERE id = 1")
                self.expire(dispatched=state != "reserved")
                self.recover(stopped=True)
                self.assertEqual(self.row()["status"], task_status)
                self.assertEqual(self.row()["result"]["delivery_id"], delivery["id"])
                self.assertEqual(self.delivery_row()["status"], delivery_status)
                self.assertEqual(self.run_status(), "waiting_confirmation")
                self.assertEqual(self.execute("SELECT latest_error FROM automation_runs WHERE id = 1")["latest_error"], "Paused by user")
                if state == "reserved":
                    self.assertIsNone(self.delivery_row()["attempt_token"])
                    self.assertFalse(self.delivery_row()["receipt"]["attempted"])

    def test_paused_sweep_leaves_live_reservations_untouched(self):
        self.delivery()
        self.execute("UPDATE automation_runs SET status = 'waiting_confirmation' WHERE id = 1")
        before = self.row(), self.delivery_row()
        self.recover(stopped=True)
        self.assertEqual((self.row(), self.delivery_row()), before)
        self.assertEqual(self.run_status(), "waiting_confirmation")

    def test_paused_sweep_releases_reservations_without_live_owner(self):
        for query in (
            "UPDATE automation_stream_tasks SET lease_token = 'attempt-b' WHERE id = 4",
            "UPDATE automation_stream_tasks SET status = 'retry', lease_token = NULL WHERE id = 4",
            "UPDATE automation_stream_tasks SET lease_until = NULL WHERE id = 4",
        ):
            with self.subTest(query=query):
                self.setUp()
                self.delivery()
                self.execute("UPDATE automation_runs SET status = 'waiting_confirmation' WHERE id = 1")
                self.execute(query)
                self.recover(stopped=True)
                self.assertEqual(self.delivery_row()["status"], "failed")
                self.assertIsNone(self.delivery_row()["attempt_token"])
                self.assertEqual(self.run_status(), "waiting_confirmation")

    def test_paused_sweep_waits_for_run_before_task_and_observes_new_expiry(self):
        self.delivery()
        self.execute("UPDATE automation_runs SET status = 'waiting_confirmation' WHERE id = 1")
        self.expire()
        pids = Queue()

        def worker():
            with self.connection() as conn:
                pids.put(conn.get_backend_pid())
                streaming.recover_stopped(conn.cursor())

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.connection() as holding:
                cursor = holding.cursor()
                cursor.execute("SELECT id FROM automation_runs WHERE id = 1 FOR UPDATE")
                future = pool.submit(worker)
                self.assert_waiting_for_lock(pids.get(timeout=4))
                with self.connection() as observer:
                    observer.cursor().execute("SELECT id FROM automation_stream_tasks WHERE id = 4 FOR UPDATE NOWAIT")
                cursor.execute("SELECT pg_sleep(0.03)")
                cursor.execute("""UPDATE automation_stream_tasks
                                  SET lease_until = clock_timestamp() - INTERVAL '0.001 seconds' WHERE id = 4""")
            future.result(timeout=4)
        self.assertEqual((self.row()["status"], self.delivery_row()["status"]), ("retry", "failed"))
        self.assertEqual(self.run_status(), "waiting_confirmation")

    def test_recovery_preserves_durable_acceptance_even_after_retry_budget_exhaustion(self):
        delivery = self.delivery("exported", {"status": "exported", "provider_response": {"created": 1}})
        self.execute("UPDATE automation_stream_tasks SET attempts = 3 WHERE id = 4")
        self.expire(dispatched=True)
        self.recover()
        self.assertEqual(self.row()["status"], "completed")
        self.assertEqual(self.row()["result"]["delivery_id"], delivery["id"])
        self.assertEqual(self.delivery_row()["status"], "exported")

    def test_recovery_honors_durable_permanent_rejection(self):
        self.delivery("failed", {"status": "failed", "retryable": False})
        self.expire()
        self.recover()
        self.assertEqual(self.row()["status"], "failed")

    def test_finalizer_does_not_reopen_stopped_steps(self):
        self.execute("UPDATE automation_runs SET status = 'cancelled' WHERE id = 1")
        self.execute("UPDATE automation_run_steps SET status = 'cancelled' WHERE id = 2")
        with self.connection() as conn:
            self.assertFalse(streaming.finalize(conn.cursor(), 1, True))
        self.assertEqual(self.execute("SELECT status FROM automation_run_steps WHERE id = 2")["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
