"""Run separately against STREAM_TEST_DATABASE_URL, a disposable PostgreSQL DB."""

import os
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4


@unittest.skipUnless(
    os.environ.get("STREAM_TEST_DATABASE_URL"), "Requires disposable PostgreSQL"
)
class StreamingIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DATABASE_URL"] = os.environ["STREAM_TEST_DATABASE_URL"]
        from fastapi.testclient import TestClient

        import main
        import streaming
        import streaming_runtime

        cls.main, cls.queue, cls.runtime = main, streaming, streaming_runtime
        cls.client = TestClient(main.app)

    def setUp(self):
        self.key = uuid4().hex
        self.campaigns, self.templates, self.exports, self.enrichments = [], [], [], []

    def tearDown(self):
        with self.main.get_db() as conn:
            c = conn.cursor()
            for cid in self.campaigns:
                c.execute("DELETE FROM export_logs WHERE campaign_id = %s", (cid,))
                c.execute("DELETE FROM contacts WHERE campaign_id = %s", (cid,))
                c.execute("DELETE FROM requests WHERE campaign_id = %s", (cid,))
                c.execute("DELETE FROM search_campaigns WHERE id = %s", (cid,))
            for table, ids in (
                ("automation_funnel_templates", self.templates),
                ("export_templates", self.exports),
                ("enrichment_templates", self.enrichments),
            ):
                for tid in ids:
                    c.execute(f"DELETE FROM {table} WHERE id = %s", (tid,))
            c.execute(
                "DELETE FROM enrichment_api_rate_limits WHERE endpoint_key LIKE %s",
                (f"%{self.key}%",),
            )
            conn.commit()

    def execute(self, query, params=(), one=False):
        with self.main.get_db() as conn:
            c = conn.cursor()
            c.execute(query, params)
            rows = c.fetchall() if c.description else []
            conn.commit()
        return dict(rows[0]) if one else [dict(row) for row in rows]

    def setup_run(
        self,
        kinds=("pipeline", "enrichment", "email_verification", "export"),
        contacts=1,
    ):
        cid = self.execute(
            "INSERT INTO search_campaigns(name,status,scrape_maps_only) VALUES (%s,'active',TRUE) RETURNING id",
            (self.key,),
            True,
        )["id"]
        self.campaigns.append(cid)
        tid = self.execute(
            "INSERT INTO automation_funnel_templates(name,execution_mode) VALUES (%s,'streaming') RETURNING id",
            (self.key,),
            True,
        )["id"]
        self.templates.append(tid)
        rid = self.execute(
            "INSERT INTO automation_runs(campaign_id,template_id,status,execution_mode) VALUES (%s,%s,'running','streaming') RETURNING id",
            (cid, tid),
            True,
        )["id"]
        for index, kind in enumerate(kinds):
            self.execute(
                "INSERT INTO automation_run_steps(run_id,campaign_id,step_order,step_type,status) VALUES (%s,%s,%s,%s,'pending')",
                (rid, cid, index + 1, kind),
            )
        self.run = self.execute(
            "SELECT * FROM automation_runs WHERE id=%s", (rid,), True
        )
        self.steps = self.execute(
            "SELECT * FROM automation_run_steps WHERE run_id=%s ORDER BY step_order",
            (rid,),
        )
        for _ in range(contacts):
            self.add_contact()
        self.enroll()
        return rid

    def add_contact(self, email="test@example.com"):
        return self.execute(
            "INSERT INTO contacts(campaign_id,business_name,status,email) VALUES (%s,'Company','new',%s) RETURNING id",
            (self.run["campaign_id"], email),
            True,
        )["id"]

    def enroll(self, needs_email=False):
        with self.main.get_db() as conn:
            self.queue.enroll(conn.cursor(), self.run, self.steps, needs_email)
            conn.commit()

    def claim(self, index, limit=10, interval=0, concurrency=10):
        with self.main.get_db() as conn:
            tasks = self.queue.claim(
                conn.cursor(),
                self.run["id"],
                self.steps[index],
                limit,
                self.key,
                concurrency,
                interval,
            )
            conn.commit()
        return tasks

    def finish(self, task, **outcome):
        with self.main.get_db() as conn:
            result = self.queue.finish(conn.cursor(), task, outcome)
            conn.commit()
        return result

    def export_step(self):
        from psycopg2.extras import Json

        template_id = self.execute(
            "INSERT INTO export_templates(name,service,api_config,field_mappings) VALUES (%s,'sendread_list','{}','{}') RETURNING id",
            (self.key,),
            True,
        )["id"]
        self.exports.append(template_id)
        step = next(step for step in self.steps if step["step_type"] == "export")
        step["config"] = {
            "template_id": template_id,
            "batch_size": 1,
            "template_snapshot": {
                "id": template_id,
                "service": "sendread_list",
                "api_config": {
                    "api_key": "fixture",
                    "base_url": f"https://{self.key}.example",
                    "sendread_target_id": "list",
                },
                "field_mappings": {"email": "email"},
            },
        }
        self.execute(
            "UPDATE automation_run_steps SET config=%s WHERE id=%s",
            (Json(step["config"]), step["id"]),
        )
        return step

    def test_queued_run_can_export(self):
        self.setup_run(("export",))
        step = self.export_step()
        task = self.claim(0, limit=1)[0]
        self.execute(
            "UPDATE automation_runs SET status='queued' WHERE id=%s", (self.run["id"],)
        )
        with patch.object(
            self.runtime.streaming_export,
            "send_batch",
            return_value=[{"contact_id": task["contact_id"], "status": "exported"}],
        ) as send:
            result = self.runtime._export_work(self.main, step, [task])[task["id"]]
        send.assert_called_once()
        self.assertEqual(result["status"], "completed")

    def test_pause_defers_export_and_keeps_inflight_contact_work(self):
        self.setup_run(("export",))
        step = self.export_step()
        task = self.claim(0, limit=1)[0]
        self.execute(
            "UPDATE automation_runs SET status='waiting_confirmation' WHERE id=%s",
            (self.run["id"],),
        )
        self.assertFalse(self.runtime.cancelled(self.main, self.run["id"]))
        with patch.object(self.runtime.streaming_export, "send_batch") as send:
            result = self.runtime._export_work(self.main, step, [task])[task["id"]]
        send.assert_not_called()
        self.finish(task, **result)
        self.assertEqual(
            self.execute(
                "SELECT status,attempts FROM automation_stream_tasks WHERE id=%s",
                (task["id"],),
                True,
            ),
            {"status": "retry", "attempts": 0},
        )

    def test_stale_export_worker_cannot_send_before_or_after_preparation(self):
        self.setup_run(("export",))
        step = self.export_step()
        old = self.claim(0, limit=1)[0]

        def expire(*_args):
            self.execute(
                "UPDATE automation_stream_tasks SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=%s",
                (old["id"],),
            )
            return {}

        with (
            patch.object(
                self.main, "_build_campaign_request_city_map", side_effect=expire
            ),
            patch.object(self.runtime.streaming_export, "send_batch") as send,
        ):
            self.runtime._export_work(self.main, step, [old])
        send.assert_not_called()
        with self.main.get_db() as conn:
            self.queue.recover(conn.cursor(), self.run["id"])
            conn.commit()
        new = self.claim(0, limit=1)[0]
        with patch.object(self.runtime.streaming_export, "send_batch") as send:
            self.runtime._export_work(self.main, step, [old])
        send.assert_not_called()
        with patch.object(
            self.runtime.streaming_export,
            "send_batch",
            return_value=[{"contact_id": new["contact_id"], "status": "exported"}],
        ) as send:
            result = self.runtime._export_work(self.main, step, [new])[new["id"]]
        send.assert_called_once()
        self.assertEqual(result["status"], "completed")

    def test_permanent_export_rejection_does_not_retry(self):
        self.setup_run(("export",))
        step = self.export_step()
        task = self.claim(0, limit=1)[0]
        receipt = {
            "contact_id": task["contact_id"],
            "status": "failed",
            "retryable": False,
            "error": {"http_status": 400, "message": "Invalid contact"},
        }
        with patch.object(
            self.runtime.streaming_export, "send_batch", return_value=[receipt]
        ):
            result = self.runtime._export_work(self.main, step, [task])[task["id"]]
        self.finish(task, **result)
        self.assertEqual(
            self.execute(
                "SELECT status FROM automation_stream_tasks WHERE id=%s",
                (task["id"],),
                True,
            )["status"],
            "failed",
        )

    def test_contact_advances_while_other_contact_still_enriching(self):
        self.setup_run(contacts=2)
        self.assertEqual(self.claim(2), [])
        first, second = self.claim(1)
        self.finish(first, status="completed", updates={"full_name": "Owner"})
        verification = self.claim(2)
        self.assertEqual([t["contact_id"] for t in verification], [first["contact_id"]])
        self.assertEqual(
            self.execute(
                "SELECT status FROM automation_stream_tasks WHERE id=%s",
                (second["id"],),
                True,
            )["status"],
            "running",
        )

    def test_builtin_maps_snapshot_remains_a_valid_source(self):
        self.setup_run(("pipeline",))
        self.execute(
            'UPDATE search_campaigns SET source_snapshot = \'{"source_type": "builtin_google_maps", "config": {}}\'::jsonb WHERE id=%s',
            (self.run["campaign_id"],),
        )
        with self.main.get_db() as conn:
            self.assertEqual(
                self.main._get_campaign_source_type(
                    conn.cursor(), self.run["campaign_id"]
                ),
                "builtin_google_maps",
            )
            self.assertEqual(
                self.runtime.source_state(
                    conn.cursor(), self.main, self.run, self.steps
                ),
                (True, False),
            )

    def test_streaming_source_persists_pipeline_mode(self):
        self.setup_run(("pipeline",))
        self.runtime.start_source(self.main, self.run, self.steps)
        self.assertEqual(
            self.execute(
                "SELECT execution_mode FROM pipeline_runs WHERE campaign_id=%s",
                (self.run["campaign_id"],),
                True,
            )["execution_mode"],
            "streaming",
        )

    def test_existing_batch_cleanup_blocks_streaming_intake(self):
        self.setup_run(("pipeline",))
        self.execute(
            "UPDATE search_campaigns SET status='completed' WHERE id=%s",
            (self.run["campaign_id"],),
        )
        self.execute(
            "INSERT INTO pipeline_runs(campaign_id,status,current_stage) VALUES (%s,'running','cleanup_contacts')",
            (self.run["campaign_id"],),
        )
        with self.assertRaisesRegex(ValueError, "batch scraping pipeline"):
            self.runtime.start_source(self.main, self.run, self.steps)

    def test_empty_open_queue_accepts_late_contacts(self):
        self.setup_run(("enrichment",))
        self.finish(self.claim(0)[0], status="completed")
        with self.main.get_db() as conn:
            self.assertFalse(self.queue.finalize(conn.cursor(), self.run["id"], False))
            conn.commit()
        late = self.add_contact()
        self.enroll()
        tasks = self.claim(0)
        self.assertEqual([t["contact_id"] for t in tasks], [late])
        self.finish(tasks[0], status="completed")
        with self.main.get_db() as conn:
            self.assertTrue(self.queue.finalize(conn.cursor(), self.run["id"], True))
            conn.commit()

    def test_retries_twice_then_failure_blocks_downstream_only(self):
        self.setup_run(contacts=2)
        tasks = self.claim(1)
        failed, healthy = tasks
        self.finish(healthy, status="completed")
        for attempt in range(3):
            self.finish(failed, status="failed", error="Provider error")
            self.execute(
                "UPDATE automation_stream_tasks SET available_at=CURRENT_TIMESTAMP WHERE id=%s",
                (failed["id"],),
            )
            if attempt < 2:
                failed = self.claim(1)[0]
        with self.main.get_db() as conn:
            self.queue.recover(conn.cursor(), self.run["id"])
            conn.commit()
        self.assertEqual(
            self.execute(
                "SELECT attempts,status FROM automation_stream_tasks WHERE id=%s",
                (failed["id"],),
                True,
            ),
            {"attempts": 3, "status": "failed"},
        )
        self.assertEqual(
            [t["contact_id"] for t in self.claim(2)], [healthy["contact_id"]]
        )

    def test_stale_lease_cannot_commit(self):
        self.setup_run(("enrichment",))
        old = self.claim(0)[0]
        self.execute(
            "UPDATE automation_stream_tasks SET lease_until=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE id=%s",
            (old["id"],),
        )
        with self.main.get_db() as conn:
            self.queue.recover(conn.cursor(), self.run["id"])
            conn.commit()
        new = self.claim(0)[0]
        self.assertNotEqual(old["lease_token"], new["lease_token"])
        self.assertFalse(
            self.finish(old, status="completed", updates={"full_name": "Stale"})
        )
        self.assertTrue(
            self.finish(new, status="completed", updates={"full_name": "Current"})
        )

    def test_cancel_prevents_new_claims_and_contact_writes(self):
        self.setup_run(("enrichment",), contacts=2)
        task = self.claim(0, limit=1)[0]
        self.execute(
            "UPDATE automation_runs SET status='cancelled' WHERE id=%s",
            (self.run["id"],),
        )
        self.assertEqual(self.claim(0), [])
        self.finish(task, status="completed", updates={"full_name": "Unwanted"})
        self.assertIsNone(
            self.execute(
                "SELECT full_name FROM contacts WHERE id=%s",
                (task["contact_id"],),
                True,
            )["full_name"]
        )

    def test_changed_email_invalidates_old_status(self):
        self.setup_run(("enrichment",))
        self.execute(
            "UPDATE contacts SET email_status='Valid' WHERE campaign_id=%s",
            (self.run["campaign_id"],),
        )
        task = self.claim(0)[0]
        self.finish(task, status="completed", updates={"email": "new@example.com"})
        row = self.execute(
            "SELECT email,email_status FROM contacts WHERE id=%s",
            (task["contact_id"],),
            True,
        )
        self.assertEqual(
            row, {"email": "new@example.com", "email_status": "unverified"}
        )

    def test_manual_edit_during_request_does_not_get_overwritten(self):
        self.setup_run(("enrichment",))
        task = self.claim(0)[0]
        self.execute(
            "UPDATE contacts SET full_name='Manual' WHERE id=%s", (task["contact_id"],)
        )
        self.finish(task, status="completed", updates={"full_name": "Old Response"})
        self.assertEqual(
            self.execute(
                "SELECT status FROM automation_stream_tasks WHERE id=%s",
                (task["id"],),
                True,
            )["status"],
            "retry",
        )
        self.assertEqual(
            self.execute(
                "SELECT full_name FROM contacts WHERE id=%s",
                (task["contact_id"],),
                True,
            )["full_name"],
            "Manual",
        )

    def test_batch_size_is_not_total_limit(self):
        self.setup_run(("export",), contacts=55)
        self.execute(
            "UPDATE automation_stream_tasks SET created_at=CURRENT_TIMESTAMP-INTERVAL '11 seconds' WHERE run_id=%s",
            (self.run["id"],),
        )
        first = self.claim(0, limit=50, concurrency=1)
        self.assertEqual(len(first), 50)
        self.assertEqual(self.claim(0, limit=50, concurrency=1), [])
        for task in first:
            self.finish(task, status="completed")
        self.assertEqual(len(self.claim(0, limit=50, concurrency=1)), 5)

    def test_export_lease_loss_requires_review(self):
        self.setup_run(("export",))
        task = self.claim(0, limit=1)[0]
        self.execute(
            "UPDATE automation_stream_tasks SET dispatched=TRUE,lease_until=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE id=%s",
            (task["id"],),
        )
        with self.main.get_db() as conn:
            self.queue.recover(conn.cursor(), self.run["id"])
            conn.commit()
        self.assertEqual(
            self.execute(
                "SELECT status FROM automation_stream_tasks WHERE id=%s",
                (task["id"],),
                True,
            )["status"],
            "uncertain",
        )
        self.assertEqual(self.claim(0, limit=1), [])

    def test_global_rate_reservation_uses_existing_prompt_limiter(self):
        self.setup_run(("enrichment",), contacts=2)
        self.assertEqual(len(self.claim(0, interval=2)), 1)
        self.assertEqual(self.claim(0, interval=2), [])
        self.assertTrue(
            self.execute(
                "SELECT next_request_at > clock_timestamp() AS future FROM enrichment_api_rate_limits WHERE endpoint_key=%s",
                (self.key,),
                True,
            )["future"]
        )

    def test_dispatch_spacing_survives_delayed_preparation(self):
        self.setup_run(("enrichment",), contacts=2)
        tasks = self.claim(0)
        starts = []

        def dispatch(task):
            self.assertTrue(
                self.runtime._wait_for_dispatch(self.main, [task], self.key, 0.1)
            )
            starts.append(time.monotonic())

        workers = [threading.Thread(target=dispatch, args=(task,)) for task in tasks]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual(len(starts), 2)
        self.assertGreaterEqual(max(starts) - min(starts), 0.085)

    def test_source_email_must_finish_before_enrichment(self):
        self.setup_run(contacts=0)
        self.add_contact(email=None)
        self.enroll(needs_email=True)
        self.assertEqual(self.claim(1), [])
        source = self.claim(0)[0]
        self.assertEqual(source["step_type"], "source_email")
        self.finish(source, status="completed", updates={"email": "found@example.com"})
        self.assertEqual(self.claim(1)[0]["contact"]["email"], "found@example.com")

    def test_runtime_exports_before_sourcing_closes_and_drains_late_lead(self):
        from psycopg2.extras import Json

        self.setup_run(("pipeline", "enrichment", "export"))
        rid, cid = self.run["id"], self.run["campaign_id"]
        self.execute(
            "INSERT INTO requests(campaign_id,req_text,status) VALUES (%s,'Still scraping','pending')",
            (cid,),
        )
        template = {
            "id": 1,
            "name": "Fixture",
            "service": "sendread_list",
            "api_config": {
                "api_key": "test",
                "base_url": f"https://{self.key}.example",
                "sendread_target_id": "list",
            },
            "field_mappings": {"email": "email"},
        }
        export_id = self.execute(
            "INSERT INTO export_templates(name,service,api_config,field_mappings) VALUES (%s,'sendread_list','{}','{}') RETURNING id",
            (self.key,),
            True,
        )["id"]
        self.exports.append(export_id)
        template["id"] = export_id
        self.execute(
            "UPDATE automation_run_steps SET config=%s WHERE id=%s",
            (
                Json(
                    {
                        "template_snapshot": template,
                        "template_id": export_id,
                        "batch_size": 1,
                        "export_valid_only": True,
                    }
                ),
                self.steps[2]["id"],
            ),
        )
        calls = []

        def process(_kind, _config, _contact, _context):
            return {"status": "completed", "updates": {"email_status": "Valid"}}

        def send(_config, contacts):
            calls.extend(c["id"] for c in contacts)
            return [{"contact_id": c["id"], "status": "exported"} for c in contacts]

        original_schedule = self.runtime.scheduling

        def schedule(step):
            if step["step_type"] == "enrichment":
                return self.key, 2, 0, 180, 2
            return original_schedule(step)

        with (
            patch.object(self.runtime, "start_source"),
            patch.object(self.runtime, "scheduling", side_effect=schedule),
            patch.object(
                self.runtime.streaming_services, "execute", side_effect=process
            ),
            patch.object(self.runtime.streaming_export, "send_batch", side_effect=send),
        ):
            worker = threading.Thread(target=self.runtime.run, args=(self.main, rid))
            worker.start()
            try:
                deadline = time.monotonic() + 12
                while not calls and time.monotonic() < deadline:
                    time.sleep(0.1)
                self.assertEqual(len(calls), 1)
                self.assertEqual(
                    self.execute(
                        "SELECT status FROM requests WHERE campaign_id=%s", (cid,), True
                    )["status"],
                    "pending",
                )
                late = self.add_contact("late@example.com")
                self.execute(
                    "UPDATE requests SET status='completed' WHERE campaign_id=%s",
                    (cid,),
                )
                worker.join(12)
                self.assertFalse(worker.is_alive())
                self.assertIn(late, calls)
                self.assertEqual(
                    self.execute(
                        "SELECT status FROM automation_runs WHERE id=%s", (rid,), True
                    )["status"],
                    "completed",
                )
            finally:
                if worker.is_alive():
                    self.execute(
                        "UPDATE automation_runs SET status='cancelled' WHERE id=%s",
                        (rid,),
                    )
                    worker.join(5)


if __name__ == "__main__":
    unittest.main()
