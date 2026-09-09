"""Run separately with PROMPT_TEST_DATABASE_URL pointing at a disposable Postgres DB."""

import json
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from uuid import uuid4


@unittest.skipUnless(os.environ.get("PROMPT_TEST_DATABASE_URL"), "Requires a disposable Postgres database")
class PromptIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DATABASE_URL"] = os.environ["PROMPT_TEST_DATABASE_URL"]
        import main
        from fastapi.testclient import TestClient
        cls.main = main
        cls.client = TestClient(main.app)
        cls.calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.calls.append((time.monotonic(), self.path))
                fail = "/fail/" in self.path
                payload = {"ok": not fail, "result": {"ok": not fail, "ai_answer":
                    'json{"name":"Test Owner","phone":"555-0100","email":"new@example.com"} Copied to clipboard'}}
                if fail:
                    payload["error"] = "Provider unavailable"
                time.sleep(0.12)
                self.send_response(502 if fail else 200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

            def log_message(self, *_args):
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.token = uuid4().hex
        self.config = {"api_url": f"http://127.0.0.1:{self.server.server_port}/{self.token}?prompt={{prompt}}",
                       "prompt_template": "Find {{company}} in {{city}}. Return name, phone, email as JSON.",
                       "requests_per_minute": 600, "timeout_seconds": 15}
        self.template = {"name": self.token, "service": "prompt_http", "api_config": self.config,
                         "output_mapping": {"name": "full_name", "email": "email", "phone": "phone"}}
        self.calls.clear()
        self.campaign_ids = []
        self.template_ids = []

    def tearDown(self):
        with self.main.get_db() as conn:
            cursor = conn.cursor()
            for cid in self.campaign_ids:
                cursor.execute("DELETE FROM contacts WHERE campaign_id=%s", (cid,))
                cursor.execute("DELETE FROM search_campaigns WHERE id=%s", (cid,))
            for tid in self.template_ids:
                cursor.execute("DELETE FROM enrichment_templates WHERE id=%s", (tid,))
            cursor.execute("DELETE FROM enrichment_api_rate_limits WHERE endpoint_key LIKE %s", (f"%{self.token}%",))
            conn.commit()

    def create_template(self):
        response = self.client.post("/api/enrichment/templates", json=self.template)
        self.assertEqual(response.status_code, 200, response.text)
        tid = response.json()["template_id"]
        self.template_ids.append(tid)
        return tid

    def campaign(self, contacts):
        with self.main.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO search_campaigns (name,status) VALUES (%s,'completed') RETURNING id", (self.token,))
            cid = cursor.fetchone()["id"]
            self.campaign_ids.append(cid)
            for contact in contacts:
                cursor.execute("""INSERT INTO contacts (campaign_id,business_name,email,full_name,phone,status)
                    VALUES (%s,%s,%s,%s,%s,'new')""", (cid, contact.get("business_name", "Company & Sons"),
                    contact.get("email"), contact.get("full_name"), contact.get("phone")))
            conn.commit()
        return cid

    def start(self, cid, tid, **options):
        with patch.object(self.main, "_ensure_enrichment_worker"):
            response = self.client.post(f"/api/campaign/{cid}/enrichment/start", json={"template_id": tid, **options})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["run"]["run_id"]

    def row(self, query, params):
        with self.main.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return dict(cursor.fetchone())

    def test_save_edit_duplicate_and_run_snapshot(self):
        tid = self.create_template()
        saved = self.client.get(f"/api/enrichment/templates/{tid}").json()
        self.assertEqual(saved["api_config"]["requests_per_minute"], 600)
        self.assertEqual(saved["output_mapping"], self.template["output_mapping"])
        cid = self.campaign([{}])
        rid = self.start(cid, tid, api_url="https://ignored.example", prompt_template="ignored")
        edited = {**saved, "api_config": {**self.config, "prompt_template": "New {{company}}"},
                  "output_mapping": {"name": "custom_1"}}
        self.assertEqual(self.client.put(f"/api/enrichment/templates/{tid}", json=edited).status_code, 200)
        self.assertEqual(self.client.get(f"/api/enrichment/templates/{tid}").json()["output_mapping"], {"name": "custom_1"})
        duplicate = self.client.post("/api/enrichment/templates", json={**edited, "name": self.token + " Copy"})
        self.assertEqual(duplicate.status_code, 200, duplicate.text)
        self.template_ids.append(duplicate.json()["template_id"])
        run = self.row("SELECT * FROM enrichment_runs WHERE id=%s", (rid,))
        self.assertEqual(json.loads(run["prompt_config"])["prompt_template"], self.config["prompt_template"])
        self.assertEqual(run["api_url"], self.config["api_url"])
        self.assertEqual(json.loads(run["output_mapping"]), self.template["output_mapping"])

    def test_test_does_not_save_and_runner_fills_missing_fields(self):
        tid = self.create_template()
        cid = self.campaign([{"email": "existing@example.com"},
                             {"email": "all@example.com", "full_name": "Existing", "phone": "111"}])
        test = self.client.post(f"/api/campaign/{cid}/enrichment/test", json={"template_id": tid})
        self.assertEqual(test.status_code, 200, test.text)
        self.assertIn("prompt", test.json())
        fields = {field["api_field"]: field for field in test.json()["field_results"]}
        self.assertEqual(fields["name"]["value"], "Test Owner")
        self.assertTrue(fields["email"]["found"])
        self.assertEqual(self.row("SELECT COUNT(*) AS n FROM contacts WHERE campaign_id=%s AND full_name IS NULL", (cid,))["n"], 1)
        rid = self.start(cid, tid)
        self.main._run_enrichment_job(rid)
        run_response = self.client.get(f"/api/enrichment/runs/{rid}").json()
        run = run_response["run"]
        result_logs = [log for log in run_response["logs"] if "field_results" in log]
        self.assertEqual(len(result_logs), 1)
        self.assertEqual(result_logs[0]["display_message"], "Extracted results")
        self.assertTrue(all(field["found"] for field in result_logs[0]["field_results"]))
        self.assertNotIn("result_payload", result_logs[0])
        self.assertEqual((run["status"], run["enriched_contacts"], run["skipped_contacts"]), ("completed", 1, 1))
        contact = self.row("SELECT * FROM contacts WHERE campaign_id=%s AND email='existing@example.com'", (cid,))
        self.assertEqual(contact["full_name"], "Test Owner")
        self.assertEqual(contact["phone"], "555-0100")
        self.assertTrue(any(item["found_count"] == 1 for item in run["field_coverage"]))
        rid = self.start(cid, tid, overwrite_existing=True)
        self.main._run_enrichment_job(rid)
        self.assertEqual(self.row("SELECT COUNT(*) AS n FROM contacts WHERE campaign_id=%s AND email='new@example.com'", (cid,))["n"], 2)

    def test_two_campaigns_share_rate_limit_and_process_every_contact(self):
        tid = self.create_template()
        cids = [self.campaign([{}, {}, {}]), self.campaign([{}, {}, {}])]
        rids = [self.start(cid, tid) for cid in cids]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(self.main._run_enrichment_job, rids))
        self.assertEqual(len(self.calls), 6)
        timestamps = sorted(item[0] for item in self.calls)
        self.assertTrue(all(b - a >= 0.085 for a, b in zip(timestamps, timestamps[1:])), timestamps)
        for rid in rids:
            run = self.row("SELECT * FROM enrichment_runs WHERE id=%s", (rid,))
            self.assertEqual((run["status"], run["processed_contacts"], run["enriched_contacts"]), ("completed", 3, 3))

    def test_failure_retries_preserve_api_response_and_pause_prevents_calls(self):
        self.config["api_url"] = self.config["api_url"].replace(f"/{self.token}", f"/fail/{self.token}")
        tid = self.create_template()
        cid = self.campaign([{}])
        rid = self.start(cid, tid, max_retries=2)
        self.client.post(f"/api/enrichment/runs/{rid}/pause")
        run = self.row("SELECT * FROM enrichment_runs WHERE id=%s", (rid,))
        self.assertFalse(self.main._wait_for_prompt_slot(json.loads(run["prompt_config"]), rid))
        self.assertEqual(self.calls, [])
        with patch.object(self.main, "_ensure_enrichment_worker"):
            self.client.post(f"/api/enrichment/runs/{rid}/resume")
        self.main._run_enrichment_job(rid)
        self.assertEqual(len(self.calls), 2)
        result = self.row("SELECT * FROM enrichment_run_contacts WHERE run_id=%s", (rid,))
        self.assertEqual((result["status"], result["attempts"]), ("failed", 2))
        self.assertIn("Provider unavailable", result["response_payload"]["_prompt_http"]["response_text"])
        status = self.client.get(f"/api/enrichment/runs/{rid}").json()
        self.assertEqual(status["run"]["error_summary"], "API request failed (HTTP 502).")
        result_logs = [log for log in status["logs"] if "field_results" in log]
        self.assertEqual(len(result_logs), 1)
        self.assertTrue(all(not field["found"] for field in result_logs[0]["field_results"]))
        self.assertNotIn("Provider unavailable", result_logs[0]["display_message"])
        test = self.client.post(f"/api/campaign/{cid}/enrichment/test", json={"template_id": tid})
        self.assertEqual(test.status_code, 502)
        self.assertEqual(test.json()["detail"]["error_summary"], "API request failed (HTTP 502).")
        self.assertEqual(len(test.json()["detail"]["field_results"]), 3)

    def test_partial_results_and_legacy_logs_use_saved_run_mapping(self):
        self.template["output_mapping"]["website"] = "domain"
        tid = self.create_template()
        cid = self.campaign([{}])
        test = self.client.post(f"/api/campaign/{cid}/enrichment/test", json={"template_id": tid})
        fields = {field["api_field"]: field for field in test.json()["field_results"]}
        self.assertFalse(fields["website"]["found"])
        self.assertIsNone(fields["website"]["value"])
        rid = self.start(cid, tid)
        self.main._run_enrichment_job(rid)
        with self.main.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE enrichment_runs SET service='http_enrichment' WHERE id=%s", (rid,))
            cursor.execute("UPDATE enrichment_run_contacts SET response_payload=%s::jsonb WHERE run_id=%s",
                           (json.dumps({"name": "Legacy Owner", "email": "Unknown", "phone": "123", "other": "metadata"}), rid))
            conn.commit()
        self.client.put(f"/api/enrichment/templates/{tid}", json={**self.template, "output_mapping": {"different": "custom_1"}})
        logs = self.client.get(f"/api/enrichment/runs/{rid}").json()["logs"]
        result_log = next(log for log in logs if "field_results" in log)
        fields = {field["api_field"]: field for field in result_log["field_results"]}
        self.assertEqual(set(fields), {"name", "email", "phone", "website"})
        self.assertEqual(fields["name"]["value"], "Legacy Owner")
        self.assertFalse(fields["email"]["found"])
        self.assertFalse(fields["website"]["found"])

    def test_invalid_typed_output_finishes_instead_of_leaving_contact_processing(self):
        self.template["output_mapping"] = {"name": "rating"}
        tid = self.create_template()
        cid = self.campaign([{}])
        rid = self.start(cid, tid)
        self.main._run_enrichment_job(rid)
        run = self.row("SELECT * FROM enrichment_runs WHERE id=%s", (rid,))
        self.assertEqual((run["status"], run["processed_contacts"], run["failed_contacts"]), ("completed", 1, 1))
        result = self.row("SELECT * FROM enrichment_run_contacts WHERE run_id=%s", (rid,))
        self.assertEqual(result["status"], "failed")
        self.assertIn("Could not save mapped fields", result["last_error"])
        self.assertIn("Test Owner", result["response_payload"]["_prompt_http"]["response_text"])


if __name__ == "__main__":
    unittest.main()
