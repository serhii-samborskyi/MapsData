import copy
import logging
import os
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, patch

from Daemon import pipeline_runtime as pipeline
from Daemon import streaming_runtime as streaming


LOGGER = logging.getLogger("test_streaming_daemon")
LOGGER.addHandler(logging.NullHandler())
DAEMON_DIR = str(Path(__file__).resolve().parents[1] / "Daemon")


def context(**cfg):
    return pipeline.PipelineContext(
        "https://main.invalid/api", "https://main.invalid", {"batch_size": 20},
        {"batch": 10, "facebook_engine": "camoufox", "same_domain_only": True}, cfg, DAEMON_DIR,
    )


def task(contact_id=1, **contact):
    return {
        "id": contact_id + 100, "lease_token": f"lease-{contact_id}",
        "contact": {"id": contact_id, "campaign_id": 7, "domain": "business.test", "email": "", **contact},
    }


def normalized_task(**contact):
    item = task(**contact)
    streaming.SourceEmailWorker._validate_tasks([item], None, 1, [])
    return item


class FakeApi:
    def __init__(self, tasks=()):
        self.tasks = list(tasks)
        self.calls = []
        self.completed = []
        self.inflight = set()
        self.peak = 0
        self.lock = threading.Lock()
        self.on_complete = lambda *_: None
        self.heartbeat = lambda: {"active": True}

    def _request_json(self, method, path, payload=None, **_kwargs):
        with self.lock:
            self.calls.append((path, copy.deepcopy(payload)))
            if path.endswith("/claim"):
                claimed = self.tasks[:payload["limit"]]
                del self.tasks[:payload["limit"]]
                self.inflight.update(t["id"] for t in claimed)
                self.peak = max(self.peak, len(self.inflight))
                return {"tasks": claimed}
            if path.endswith("/heartbeat"):
                return self.heartbeat()
            if path.endswith("/complete"):
                task_id = int(path.split("/")[-2])
                self.completed.append((task_id, payload))
                self.inflight.discard(task_id)
                self.on_complete(task_id, payload)
                return {"accepted": True}
        raise AssertionError(f"Unexpected endpoint: {path}")


class ScopedExtractionTests(unittest.TestCase):
    def test_local_api_scopes_reads_and_buffers_writes(self):
        original = task()["contact"]
        with streaming.ScopedContactApi(7, original, lambda: False) as scope:
            api = pipeline.PipelineApiClient(scope.base_url, LOGGER)
            rows = api._request_json("GET", "/api/campaign/7/nomail?batch=1000")["contacts"]
            self.assertEqual(rows, [original])
            result = api._request_json("POST", "/api/campaign/7/email_update", {"id": "1", "email": "hello@business.test"})
            self.assertTrue(result["_ok"])
            self.assertEqual(scope.updates, [{"id": "1", "email": "hello@business.test"}])
            self.assertEqual(api.get_stats("7")["contacts_without_email"], 0)
            self.assertEqual(api._request_json("GET", "/api/campaign/7/nomail")["contacts"], [])
        self.assertEqual(original["email"], "")

    def test_out_of_scope_batch_is_rejected_without_partial_write(self):
        with streaming.ScopedContactApi(7, task()["contact"], lambda: False) as scope:
            api = pipeline.PipelineApiClient(scope.base_url, LOGGER)
            result = api._request_json("POST", "/api/campaign/7/email_update", {"contacts": [
                {"id": "1", "email": "valid@business.test"}, {"id": "99", "email": "wrong@business.test"},
            ]})
            self.assertEqual(result["_status"], 400)
            self.assertEqual(scope.updates, [])
            self.assertTrue(scope.error)
            self.assertEqual(api._request_json("GET", "/api/campaign/8/nomail")["_status"], 404)

    def test_lease_loss_blocks_local_reads_and_writes(self):
        stop = threading.Event()
        with streaming.ScopedContactApi(7, task()["contact"], stop.is_set) as scope:
            api = pipeline.PipelineApiClient(scope.base_url, LOGGER)
            stop.set()
            self.assertEqual(api._request_json("GET", "/api/campaign/7/nomail")["_status"], 409)
            result = api._request_json("POST", "/api/campaign/7/email_update", {"id": 1, "email": "hello@business.test"})
            self.assertEqual(result["_status"], 409)
            self.assertEqual(scope.updates, [])

    def _run_passes(self, email_in=None):
        calls = []
        original = context(fast_max_batches_cap=100)

        def scrape(args, cwd, logger, should_stop, process_group=False):
            self.assertTrue(process_group)
            self.assertEqual(cwd, DAEMON_DIR)
            self.assertFalse(should_stop())
            self.assertEqual(args[args.index("--max-batches") + 1], "1")
            self.assertEqual(args[args.index("--batch") + 1], "1")
            self.assertEqual(args[args.index("--concurrency") + 1], "1")
            base_url = args[args.index("--base-url") + 1]
            self.assertTrue(base_url.startswith("http://127.0.0.1:"))
            api = pipeline.PipelineApiClient(base_url, LOGGER)
            contacts = api._request_json("GET", "/api/campaign/7/nomail?batch=1")["contacts"]
            self.assertEqual([c["id"] for c in contacts], [1])
            stage = "fallback" if "--facebook" in args else "fast"
            if stage == "fallback":
                self.assertEqual(args[args.index("--max-batches-facebook") + 1], "1")
                self.assertEqual(args[args.index("--facebook-engine") + 1], "camoufox")
                self.assertTrue(args[1].endswith("email_scraper.py"))
            else:
                self.assertTrue(args[1].endswith("email_scraper_scrapy.py"))
            calls.append(stage)
            if stage == email_in:
                api._request_json("POST", "/api/campaign/7/email_update", {"id": 1, "email": "hello@business.test"})
            return 0

        with patch.object(pipeline, "_run_subprocess_with_stop", side_effect=scrape):
            updates = streaming.process_contact(normalized_task(), original, LOGGER, lambda: False)
        self.assertEqual(original.email_cfg["batch"], 10)
        self.assertEqual(original.pipeline_cfg["fast_max_batches_cap"], 100)
        return calls, updates

    def test_fast_and_fallback_run_before_no_email_completion(self):
        calls, updates = self._run_passes()
        self.assertEqual(calls, ["fast", "fallback"])
        self.assertEqual(updates, [])

    def test_fast_email_avoids_unnecessary_fallback(self):
        calls, updates = self._run_passes("fast")
        self.assertEqual(calls, ["fast"])
        self.assertEqual(updates[0]["email"], "hello@business.test")

    def test_fallback_email_is_returned(self):
        calls, updates = self._run_passes("fallback")
        self.assertEqual(calls, ["fast", "fallback"])
        self.assertEqual(updates[0]["email"], "hello@business.test")

    def test_zero_exit_without_contact_fetch_is_failure(self):
        with patch.object(pipeline, "_run_subprocess_with_stop", return_value=0):
            with self.assertRaisesRegex(RuntimeError, "without fetching"):
                streaming.process_contact(normalized_task(), context(), LOGGER, lambda: False)

    def test_existing_email_and_missing_website_need_no_subprocess(self):
        with patch.object(pipeline, "_run_subprocess_with_stop") as run:
            for contact in ({"email": "already@business.test"}, {"domain": ""}):
                self.assertEqual(streaming.process_contact(normalized_task(**contact), context(), LOGGER, lambda: False), [])
            run.assert_not_called()


class LeaseAndWorkerTests(unittest.TestCase):
    def worker(self, api, stop=lambda: False, **cfg):
        return streaming.SourceEmailWorker(api, context(**cfg), LOGGER, "worker-1", stop)

    def test_completion_contract_including_no_email(self):
        for updates in ([], [{"id": "1", "email": "hello@business.test"}]):
            with self.subTest(updates=updates):
                api = FakeApi()
                with patch.object(streaming, "process_contact", return_value=updates):
                    self.worker(api)._process_task(normalized_task(), lambda: False)
                self.assertEqual(api.calls[0], (streaming.SOURCE_TASKS_PATH + "/101/heartbeat", {"lease_token": "lease-1"}))
                payload = api.completed[0][1]
                self.assertEqual(payload["status"], "completed")
                self.assertEqual(payload["lease_token"], "lease-1")
                self.assertEqual(payload.get("email"), updates[0]["email"] if updates else None)

    def test_failed_extraction_uses_failed_completion(self):
        api = FakeApi()
        with patch.object(streaming, "process_contact", side_effect=RuntimeError("browser launch failed")):
            self.worker(api)._process_task(normalized_task(), lambda: False)
        self.assertEqual(api.completed[0][1]["status"], "failed")
        self.assertIn("browser launch failed", api.completed[0][1]["error"])

    def test_inactive_or_conflicting_lease_never_extracts_or_completes(self):
        for response in ({"active": False}, {"_ok": False, "_status": 409}):
            api = FakeApi()
            api.heartbeat = lambda: response
            with patch.object(streaming, "process_contact") as process:
                self.worker(api)._process_task(normalized_task(), lambda: False)
                process.assert_not_called()
            self.assertEqual(api.completed, [])

    def test_heartbeat_revocation_stops_in_progress_contact(self):
        api = FakeApi()
        answers = iter([{"active": True}, {"active": False}])
        api.heartbeat = lambda: next(answers)
        observed_stop = threading.Event()

        def process(_task, _ctx, _logger, should_stop):
            deadline = time.monotonic() + 2
            while not should_stop() and time.monotonic() < deadline:
                time.sleep(0.01)
            if should_stop():
                observed_stop.set()
            return [{"id": "1", "email": "late@business.test"}]

        with patch.object(streaming, "HEARTBEAT_SECONDS", 0.01), patch.object(streaming, "process_contact", side_effect=process):
            self.worker(api)._process_task(normalized_task(), lambda: False)
        self.assertTrue(observed_stop.is_set())
        self.assertEqual(api.completed, [])

    def test_renewal_latency_and_failed_renewal_do_not_extend_lease(self):
        api = FakeApi()
        with patch.object(streaming.time, "monotonic", return_value=10):
            lease = streaming.TaskLease(api, normalized_task(), lambda: False, LOGGER)
            self.assertTrue(lease.renew())
        self.assertEqual(lease.deadline, 190)
        api.heartbeat = lambda: {"_ok": False, "_status": 503}
        with patch.object(streaming.time, "monotonic", return_value=100):
            self.assertFalse(lease.renew())
            self.assertFalse(lease.stopped())
        with patch.object(streaming.time, "monotonic", return_value=190):
            self.assertTrue(lease.stopped())
        api.heartbeat = lambda: {"active": True}
        with patch.object(streaming.time, "monotonic", return_value=195):
            lease.renew()
            self.assertTrue(lease.stopped(), "A late heartbeat must not revive a stopped attempt")

    def test_rejected_json_acknowledgment_is_not_success(self):
        self.assertFalse(streaming._ok({"accepted": False, "_status": 200}))
        self.assertFalse(streaming._ok({"active": False, "_status": 200}))

    def test_shutdown_during_extraction_does_not_acknowledge(self):
        stop = threading.Event()
        api = FakeApi()

        def process(*_args):
            stop.set()
            return []

        with patch.object(streaming, "process_contact", side_effect=process):
            self.worker(api)._process_task(normalized_task(), stop.is_set)
        self.assertEqual(api.completed, [])

    def test_ambiguous_completion_is_not_converted_to_failed(self):
        api = FakeApi()
        real_request = api._request_json
        completions = []

        def request(method, path, payload=None, **kwargs):
            if path.endswith("/complete"):
                completions.append(payload)
                return {"_ok": False, "_status": 0}
            return real_request(method, path, payload, **kwargs)

        api._request_json = request
        with patch.object(streaming, "process_contact", return_value=[]):
            self.worker(api)._process_task(normalized_task(), lambda: False)
        self.assertEqual(len(completions), 3)
        self.assertTrue(all(p["status"] == "completed" and p["lease_token"] == "lease-1" for p in completions))

    def test_capacity_refills_without_waiting_for_slow_contact(self):
        api = FakeApi([task(1), task(2), task(3)])
        stop = threading.Event()
        slow_started, fast_done, third_done = threading.Event(), threading.Event(), threading.Event()

        def process(item, _ctx, _logger, should_stop):
            if item["contact"]["id"] == 1:
                slow_started.set()
                while not should_stop():
                    time.sleep(0.01)
            return []

        def completed(task_id, _payload):
            if task_id == 102:
                fast_done.set()
            if task_id == 103:
                third_done.set()
                stop.set()

        api.on_complete = completed
        thread = threading.Thread(target=self.worker(api, stop.is_set).run)
        with patch.object(streaming, "process_contact", side_effect=process):
            thread.start()
            try:
                self.assertTrue(slow_started.wait(2))
                self.assertTrue(fast_done.wait(2))
                self.assertTrue(third_done.wait(2))
            finally:
                stop.set()
                thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        self.assertEqual([item[0] for item in api.completed], [102, 103])
        self.assertLessEqual(api.peak, 2)
        self.assertTrue(all(p == {"worker_id": "worker-1", "limit": 1} for path, p in api.calls if path.endswith("/claim")))

    def test_404_is_backed_off_and_shutdown_is_responsive(self):
        stop = threading.Event()
        api = Mock()
        api._request_json.return_value = {"_ok": False, "_status": 404}
        worker = self.worker(api, stop.is_set)
        thread = threading.Thread(target=worker.run)
        thread.start()
        try:
            time.sleep(0.25)
        finally:
            stop.set()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(api._request_json.call_count, 1)

    def test_malformed_and_cross_campaign_claims_are_rejected(self):
        for item in (
            {**task(), "lease_token": ""}, task(id=""),
            {**task(), "campaign_id": 8}, {**task(), "step_type": "enrichment"},
        ):
            with self.subTest(task=item), self.assertRaises(RuntimeError):
                streaming.SourceEmailWorker._validate_tasks([item], "7", 1, [])
        with self.assertRaises(RuntimeError):
            streaming.SourceEmailWorker._validate_tasks([task()], None, 1, [("7", "1")])
        with self.assertRaises(RuntimeError):
            streaming.SourceEmailWorker._validate_tasks([task(1), task(2)], None, 1, [])


class PipelineIntegrationTests(unittest.TestCase):
    def test_metric_sampler_reports_daemon_metadata(self):
        sampler = pipeline.DaemonMetricSampler("maps")
        metadata = sampler.snapshot()["daemon"]
        self.assertEqual(metadata["role"], "maps")
        self.assertIsInstance(metadata["pid"], int)
        self.assertIn("cpu_count", metadata)

    def run_stage(self, stage, mode=None):
        stopped = threading.Event()
        api = Mock()
        api.claim.return_value = {"claimed": True, "run_id": 2, "campaign_id": 7, "stage": stage, "execution_mode": mode}
        api.heartbeat.return_value = {"_ok": True}
        api.stage_complete.side_effect = lambda **_kwargs: (stopped.set() or {"_ok": True})
        api.fail.side_effect = lambda **_kwargs: (stopped.set() or {"_ok": True})
        with patch.object(pipeline, "PipelineApiClient", return_value=api), \
             patch.object(pipeline, "_run_cleanup_stage") as cleanup, \
             patch.object(pipeline, "_run_email_stage") as email, \
             patch.object(pipeline, "_run_maps_stage") as maps:
            pipeline._run_pipeline_worker(LOGGER, context(), "worker", "daemon", 1, 180, 30, stopped.is_set)
        api.fail.assert_not_called()
        api.stage_complete.assert_called_once()
        return cleanup, email, maps

    def test_legacy_stages_skip_only_with_explicit_streaming_claim(self):
        for stage in ("cleanup_contacts", "email_fast", "email_fallback"):
            with self.subTest(stage=stage):
                cleanup, email, _ = self.run_stage(stage, "streaming")
                cleanup.assert_not_called()
                email.assert_not_called()
                cleanup, email, _ = self.run_stage(stage)
                (cleanup if stage == "cleanup_contacts" else email).assert_called_once()

    def test_streaming_maps_emits_single_rows_legacy_batch_is_preserved(self):
        for mode, batch_size in (("streaming", 1), (None, 20)):
            with self.subTest(mode=mode):
                _, _, maps = self.run_stage("maps_scrape", mode)
                self.assertEqual(maps.call_args.args[3].maps_cfg["batch_size"], batch_size)

    def test_manager_stop_releases_active_stage_without_marking_it_failed(self):
        stopped = threading.Event()
        api = Mock()
        api.claim.return_value = {
            "claimed": True,
            "run_id": 2,
            "campaign_id": 7,
            "stage": "maps_scrape",
        }
        api.heartbeat.return_value = {"_ok": True, "daemon_state": "stopped"}
        api.release.side_effect = lambda *_args, **_kwargs: (stopped.set() or {"status": "released"})
        with patch.object(pipeline, "PipelineApiClient", return_value=api), \
             patch.object(pipeline, "_run_maps_stage"):
            pipeline._run_pipeline_worker(LOGGER, context(), "worker", "daemon", 1, 180, 30, stopped.is_set)
        api.release.assert_called_once_with("2", "worker", ANY, "maps_scrape")
        api.stage_complete.assert_not_called()
        api.fail.assert_not_called()

    def test_background_worker_spans_maps_and_idle_and_is_joined(self):
        api = FakeApi([task(1)])
        first_done, second_done = threading.Event(), threading.Event()
        api.on_complete = lambda task_id, _payload: (first_done if task_id == 101 else second_done).set()

        def pipeline_loop(*_args):
            self.assertTrue(first_done.wait(2), "Email extraction must run while Maps is occupied")
            with api.lock:
                api.tasks.append(task(2))
            self.assertTrue(second_done.wait(2), "Email extraction must continue after Maps")

        with patch.object(pipeline, "PipelineApiClient", return_value=api), \
             patch.object(pipeline, "_run_pipeline_worker", side_effect=pipeline_loop), \
             patch.object(streaming, "process_contact", return_value=[]), \
             patch.object(streaming, "POLL_SECONDS", 0.05):
            pipeline.run_pipeline_worker(LOGGER, context(), "worker", "daemon", 1, 180, 30, lambda: False)
        self.assertFalse(any(t.name.startswith(("source-email", "source-task-heartbeat", "streaming-source-email")) for t in threading.enumerate()))

    def test_subprocess_stop_reaps_child(self):
        proc = Mock()
        proc.poll.side_effect = [None, None, 0]
        proc.wait.side_effect = [subprocess.TimeoutExpired("scraper", 10), 0]
        with patch.object(pipeline.subprocess, "Popen", return_value=proc):
            code = pipeline._run_subprocess_with_stop(["scraper"], DAEMON_DIR, LOGGER, lambda: True)
        self.assertEqual(code, -15)
        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(proc.wait.call_count, 2)

    @unittest.skipUnless(os.name == "posix", "Process groups require POSIX")
    def test_scoped_subprocess_group_is_stopped(self):
        code = pipeline._run_subprocess_with_stop(
            [sys.executable, "-c", "import time; time.sleep(30)"], DAEMON_DIR, LOGGER,
            lambda: True, process_group=True,
        )
        self.assertEqual(code, -15)


if __name__ == "__main__":
    unittest.main()
