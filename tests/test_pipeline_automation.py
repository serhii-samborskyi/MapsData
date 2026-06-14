import asyncio
import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta


def _install_stub_modules():
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.DataError = type("DataError", (Exception,), {})
    psycopg2.IntegrityError = type("IntegrityError", (Exception,), {})
    sys.modules["psycopg2"] = psycopg2

    psycopg2_extras = types.ModuleType("psycopg2.extras")

    class Json:
        def __init__(self, obj):
            self.obj = obj

    psycopg2_extras.Json = Json
    sys.modules["psycopg2.extras"] = psycopg2_extras

    fastapi = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FastAPI:
        def mount(self, *args, **kwargs):
            return None

        def _decorator(self, *args, **kwargs):
            def wrapper(func):
                return func
            return wrapper

        get = post = put = delete = on_event = _decorator

    def Form(default=None, **kwargs):
        return default

    class Request:
        pass

    fastapi.FastAPI = FastAPI
    fastapi.Form = Form
    fastapi.Request = Request
    fastapi.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi

    fastapi_responses = types.ModuleType("fastapi.responses")
    fastapi_responses.HTMLResponse = type("HTMLResponse", (), {})
    fastapi_responses.JSONResponse = type("JSONResponse", (), {"__init__": lambda self, *a, **k: None})
    sys.modules["fastapi.responses"] = fastapi_responses

    fastapi_staticfiles = types.ModuleType("fastapi.staticfiles")
    fastapi_staticfiles.StaticFiles = type("StaticFiles", (), {"__init__": lambda self, *a, **k: None})
    sys.modules["fastapi.staticfiles"] = fastapi_staticfiles

    fastapi_templating = types.ModuleType("fastapi.templating")

    class Jinja2Templates:
        def __init__(self, *args, **kwargs):
            pass

        def TemplateResponse(self, template_name, context):
            return {"template": template_name, "context": context}

    fastapi_templating.Jinja2Templates = Jinja2Templates
    sys.modules["fastapi.templating"] = fastapi_templating

    database = types.ModuleType("database")
    database.init_db = lambda: None
    database.get_db = lambda: None
    sys.modules["database"] = database

    templates = types.ModuleType("templates")

    class TemplateManager:
        @staticmethod
        def get_all_templates():
            return []

        @staticmethod
        def get_template(_template_id):
            return None

        @staticmethod
        def create_template(*args, **kwargs):
            return 1

        @staticmethod
        def update_template(*args, **kwargs):
            return None

        @staticmethod
        def delete_template(*args, **kwargs):
            return None

    class ManyReachIntegration:
        def __init__(self, *_args, **_kwargs):
            self.rate_limit = 60

        def get_campaigns(self):
            return []

        def get_default_field_mapping(self):
            return {}

        def transform_contact(self, *_args, **_kwargs):
            return {}

        def validate_contact(self, *_args, **_kwargs):
            return True

        def export_to_manyreach_bulk(self, *_args, **_kwargs):
            return {"ok": True}

    class SmartLeadIntegration:
        def __init__(self, *_args, **_kwargs):
            self.rate_limit = 60
            self.max_leads_per_request = 1000

        def get_campaigns(self, **_kwargs):
            return []

        def get_default_field_mapping(self):
            return {}

        def transform_contact(self, *_args, **_kwargs):
            return {}

        def validate_contact(self, *_args, **_kwargs):
            return True

        def export_to_smartlead_bulk(self, *_args, **_kwargs):
            return {"ok": True}

    class SendReadIntegration:
        def __init__(self, *_args, **_kwargs):
            self.rate_limit = 120

        def get_default_field_mapping(self):
            return {}

        def get_campaigns(self):
            return []

        def get_ab_test_lists(self):
            return []

        def transform_contact(self, *_args, **_kwargs):
            return {}

        def validate_contact(self, *_args, **_kwargs):
            return True

        def export_to_campaign(self, *_args, **_kwargs):
            return {"ok": True}

        def export_to_ab_test_list(self, *_args, **_kwargs):
            return {"ok": True}

    templates.TemplateManager = TemplateManager
    templates.ManyReachIntegration = ManyReachIntegration
    templates.SmartLeadIntegration = SmartLeadIntegration
    templates.SendReadIntegration = SendReadIntegration
    templates.extract_city_from_address = lambda *_args, **_kwargs: ""
    sys.modules["templates"] = templates

    email_verification = types.ModuleType("email_verification")

    class EmailVerificationManager:
        @staticmethod
        def get_template(_template_id):
            return {"id": 1, "name": "stub"}

        @staticmethod
        def get_all_templates():
            return []

        @staticmethod
        def create_template(*args, **kwargs):
            return 1

        @staticmethod
        def delete_template(*args, **kwargs):
            return None

    class EmailVerificationService:
        def verify_batch(self, emails, *_args, **_kwargs):
            return [{"email": email, "success": True, "mapped_status": "Valid"} for email in emails]

    class MyEmailVerifierIntegration:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_default_status_mapping(self):
            return {}

    email_verification.EmailVerificationManager = EmailVerificationManager
    email_verification.EmailVerificationService = EmailVerificationService
    email_verification.MyEmailVerifierIntegration = MyEmailVerifierIntegration
    sys.modules["email_verification"] = email_verification

    requests_module = types.ModuleType("requests")

    class _DummyResponse:
        def __init__(self, status_code=200, payload=None):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = ""
            self.content = b"{}"

        def json(self):
            return self._payload

    def _dummy_post(*_args, **_kwargs):
        return _DummyResponse()

    def _dummy_get(*_args, **_kwargs):
        return _DummyResponse(payload={"input_fields": [], "required_input_fields": [], "enrichment_fields": []})

    def _dummy_request(*_args, **_kwargs):
        return _DummyResponse(payload={})

    requests_module.post = _dummy_post
    requests_module.get = _dummy_get
    requests_module.request = _dummy_request
    sys.modules["requests"] = requests_module


def load_main_module():
    for module_name in [
        "main",
        "psycopg2",
        "psycopg2.extras",
        "fastapi",
        "fastapi.responses",
        "fastapi.staticfiles",
        "fastapi.templating",
        "database",
        "templates",
        "email_verification",
    ]:
        sys.modules.pop(module_name, None)
    _install_stub_modules()
    return importlib.import_module("main")


class FakeRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class ScriptedCursor:
    def __init__(self, steps):
        self.steps = list(steps)
        self.index = 0
        self.rowcount = 0
        self._last_step = None

    def execute(self, query, params=None):
        if self.index >= len(self.steps):
            raise AssertionError(f"Unexpected query: {query}")
        step = self.steps[self.index]
        self.index += 1
        normalized_query = " ".join(query.lower().split())
        expected = step["match"].lower()
        if expected not in normalized_query:
            raise AssertionError(f"Expected query containing '{step['match']}', got '{query}'")
        self._last_step = step
        self.rowcount = step.get("rowcount", 0)

    def fetchone(self):
        if not self._last_step:
            return None
        return self._last_step.get("fetchone")

    def fetchall(self):
        if not self._last_step:
            return []
        return self._last_step.get("fetchall", [])


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeDBContext:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class PipelineEndpointTests(unittest.TestCase):
    def setUp(self):
        self.main = load_main_module()

    def _patch_db(self, steps):
        cursor = ScriptedCursor(steps)
        conn = FakeConnection(cursor)
        self.main.get_db = lambda: FakeDBContext(conn)
        return cursor, conn

    def test_start_is_idempotent_when_active_run_exists(self):
        cursor, _ = self._patch_db([
            {"match": "select id from search_campaigns", "fetchone": {"id": 1}},
            {"match": "from pipeline_runs", "fetchone": {"id": 15, "status": "running", "current_stage": "maps_scrape"}},
            {"match": "from pipeline_run_stages", "fetchone": {"status": "running"}},
        ])

        response = asyncio.run(self.main.start_campaign_pipeline(1, FakeRequest({"actor": "dashboard"})))
        self.assertTrue(response["idempotent"])
        self.assertEqual(response["run_id"], 15)
        self.assertEqual(response["current_stage"], "maps_scrape")
        self.assertEqual(cursor.index, 3)

    def test_claim_returns_none_when_lock_is_active_for_other_worker(self):
        now = datetime.utcnow()
        self._patch_db([
            {"match": "pg_advisory_xact_lock"},
            {
                "match": "from pipeline_run_locks prl",
                "fetchall": [{"run_id": 42, "worker_id": "daemon-a", "lease_expires_at": now + timedelta(seconds=120)}],
            },
            {
                "match": "from pipeline_runs",
                "fetchall": [{"id": 42, "campaign_id": 7, "status": "running", "current_stage": "cleanup_contacts"}],
            },
            {
                "match": "from pipeline_run_stages",
                "fetchall": [{"run_id": 42, "stage": "cleanup_contacts", "stage_order": 1, "status": "running"}],
            },
            {
                "match": "from pipeline_run_locks where run_id",
                "fetchone": {"worker_id": "daemon-a", "lease_expires_at": now + timedelta(seconds=120)},
            },
        ])

        response = asyncio.run(self.main.claim_pipeline_stage(FakeRequest({"worker_id": "daemon-b"})))
        self.assertFalse(response["claimed"])
        self.assertEqual(response["reason"], "all_leased")
        self.assertIsNone(response["run_id"])

    def test_claim_returns_reason_when_no_runs_exist(self):
        self._patch_db([
            {"match": "pg_advisory_xact_lock"},
            {"match": "from pipeline_run_locks prl", "fetchall": []},
            {"match": "from pipeline_runs", "fetchall": []},
        ])

        response = asyncio.run(self.main.claim_pipeline_stage(FakeRequest({"worker_id": "daemon-b"})))
        self.assertFalse(response["claimed"])
        self.assertEqual(response["reason"], "run_not_started")

    def test_claim_fairness_blocks_second_distinct_run_when_other_worker_active(self):
        now = datetime.utcnow()
        self._patch_db([
            {"match": "pg_advisory_xact_lock"},
            {
                "match": "from pipeline_run_locks prl",
                "fetchall": [
                    {"run_id": 10, "worker_id": "daemon-a", "lease_expires_at": now + timedelta(seconds=120)},
                    {"run_id": 11, "worker_id": "daemon-b", "lease_expires_at": now + timedelta(seconds=120)},
                ],
            },
            {
                "match": "from pipeline_runs",
                "fetchall": [{"id": 99, "campaign_id": 8, "status": "pending", "current_stage": "email_fast"}],
            },
            {
                "match": "from pipeline_run_stages",
                "fetchall": [{"run_id": 99, "stage": "email_fast", "stage_order": 2, "status": "pending"}],
            },
            {
                "match": "from pipeline_run_locks where run_id",
                "fetchone": None,
            },
        ])

        response = asyncio.run(self.main.claim_pipeline_stage(FakeRequest({"worker_id": "daemon-a"})))
        self.assertFalse(response["claimed"])
        self.assertEqual(response["reason"], "all_leased")
        self.assertIsNone(response["run_id"])

    def test_source_template_config_accepts_xpath_regex_dynamic_fields(self):
        config = self.main._normalize_source_template_config({
            "start_url_template": "https://example.com/search?q={query}",
            "navigation": {"type": "scroll", "max_scrolls": 5, "all_the_way_down_scrolls": True},
            "fast": {
                "block_xpath": "//div[@class='card']",
                "fields": [
                    {"label": "Name", "target_type": "core", "target_field": "business_name", "xpath": ".//h2/text()", "required": True},
                    {
                        "label": "License",
                        "target_type": "dynamic",
                        "target_field": "license_number",
                        "xpath": ".//span",
                        "regex": "License: (\\w+)",
                        "run_regex_within_xpath_content": True,
                        "strip_html_before_regex": True,
                    },
                ],
            },
            "slow": {"enabled": False, "detail_url_within_block": False},
        })

        self.assertEqual(config["navigation"]["type"], "scroll")
        self.assertTrue(config["navigation"]["all_the_way_down_scrolls"])
        self.assertEqual(config["fast"]["fields"][0]["target_field"], "business_name")
        self.assertEqual(config["fast"]["fields"][1]["target_type"], "dynamic")
        self.assertEqual(config["fast"]["fields"][1]["target_field"], "license_number")
        self.assertTrue(config["fast"]["fields"][1]["run_regex_within_xpath_content"])
        self.assertTrue(config["fast"]["fields"][1]["strip_html_before_regex"])
        self.assertEqual(config["slow"]["detail_scrolls"], 3)
        self.assertFalse(config["slow"]["detail_url_within_block"])

    def test_source_template_config_rejects_bad_regex(self):
        with self.assertRaises(Exception) as ctx:
            self.main._normalize_source_template_config({
                "start_url_template": "https://example.com/search?q={query}",
                "navigation": {"type": "scroll"},
                "fast": {
                    "block_xpath": "//div",
                    "fields": [
                        {"label": "Name", "target_type": "core", "target_field": "business_name", "xpath": ".//h2/text()", "regex": "("},
                    ],
                },
            })
        self.assertIn("invalid regex", str(getattr(ctx.exception, "detail", ctx.exception)).lower())

    def test_source_template_config_rejects_dynamic_core_collision(self):
        with self.assertRaises(Exception) as ctx:
            self.main._normalize_source_template_config({
                "start_url_template": "https://example.com/search?q={query}",
                "navigation": {"type": "scroll"},
                "fast": {
                    "block_xpath": "//div",
                    "fields": [
                        {"label": "Name", "target_type": "dynamic", "target_field": "business_name", "xpath": ".//h2/text()"},
                    ],
                },
            })
        self.assertIn("conflicts", str(getattr(ctx.exception, "detail", ctx.exception)).lower())

    def test_source_template_export_payload_uses_versioned_schema(self):
        payload = self.main._source_template_export_payload({
            "id": 5,
            "name": "Facebook Ads",
            "description": "FB library scraper",
            "source_type": "generic",
            "enabled": True,
            "config": {
                "start_url_template": "https://example.com/search?q={query}",
                "navigation": {"type": "scroll", "max_scrolls": 5},
                "fast": {
                    "block_xpath": "//div[@class='card']",
                    "fields": [
                        {"label": "Name", "target_type": "core", "target_field": "business_name", "xpath": ".//h2/text()"},
                    ],
                },
            },
        })

        self.assertEqual(payload["schema"], "scrapiq.source_template")
        self.assertEqual(payload["version"], 1)
        self.assertEqual(payload["name"], "Facebook Ads")
        self.assertEqual(payload["source_type"], "generic")
        self.assertEqual(payload["config"]["version"], 1)

    def test_source_template_import_payload_validates_and_normalizes(self):
        payload = self.main._normalize_source_template_import_payload({
            "schema": "scrapiq.source_template",
            "version": 1,
            "name": "Directory",
            "description": "Imported",
            "enabled": False,
            "source_type": "generic",
            "config": {
                "start_url_template": "https://example.com/search?q={query}",
                "navigation": {"type": "scroll", "all_the_way_down_scrolls": True},
                "fast": {
                    "block_xpath": "//article",
                    "fields": [
                        {"label": "Name", "target_type": "core", "target_field": "business_name", "xpath": ".//h2"},
                    ],
                },
            },
            "test": {"query": "plumber chicago"},
        })

        self.assertEqual(payload["name"], "Directory")
        self.assertFalse(payload["enabled"])
        self.assertEqual(payload["source_type"], "generic")
        self.assertTrue(payload["config"]["navigation"]["all_the_way_down_scrolls"])
        self.assertEqual(payload["test"]["query"], "plumber chicago")

    def test_http_source_template_config_accepts_mapping_timeout_and_concurrency(self):
        config = self.main._normalize_source_template_config_for_type({
            "base_url": "http://example.com/api/run-sync?scriptName=ai_overview.js&request={query}",
            "method": "GET",
            "response_path": "result.ai_answer",
            "timeout_seconds": 275,
            "concurrency": 25,
            "field_mapping": [
                {"source_field": "company", "target_fields": ["business_name", "company"]},
                {"source_field": "owner_fname", "target_field": "owner first name"},
                {"source_field": "domain", "target_field": "website/domain"},
            ],
        }, "http_api")

        self.assertEqual(config["method"], "GET")
        self.assertEqual(config["timeout_seconds"], 275)
        self.assertEqual(config["concurrency"], 25)
        self.assertEqual(config["request_template"], "{{query}}")
        self.assertEqual(config["field_mapping"][0]["target_fields"], ["business_name", "company"])
        self.assertEqual(config["field_mapping"][1]["target_fields"], ["firstname"])
        self.assertEqual(config["field_mapping"][2]["target_fields"], ["domain"])

    def test_http_source_template_config_requires_query_placeholder(self):
        with self.assertRaises(Exception) as ctx:
            self.main._normalize_source_template_config_for_type({
                "base_url": "http://example.com/api/run-sync",
                "method": "GET",
                "field_mapping": {"company": "business_name"},
            }, "http_api")

        self.assertIn("{query}", str(getattr(ctx.exception, "detail", ctx.exception)))

    def test_http_source_extracts_first_json_array_from_ai_answer_text(self):
        payload = {
            "ok": True,
            "result": {
                "ai_answer": "json[ { \"company\": \"Chicago Locksmiths\", \"domain\": \"chicagolocksmiths.net\" } ]"
            },
        }
        rows = self.main._extract_http_source_rows(payload, "", {"response_path": "result.ai_answer"})

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company"], "Chicago Locksmiths")
        self.assertEqual(rows[0]["domain"], "chicagolocksmiths.net")

    def test_http_source_extracts_markdown_json_array_from_full_response(self):
        rows = self.main._extract_http_source_rows(
            {"message": "```json\n[{\"company\":\"A\"}]\n```"},
            "",
            {"response_path": ""},
        )

        self.assertEqual(rows, [{"company": "A"}])

    def test_http_source_extracts_google_ai_array_with_code_warning_artifacts(self):
        payload = {
            "ok": True,
            "result": {
                "ai_answer": (
                    "json["
                    "{\"company\":\"MaxComfort HVAC\",\"domain\":\"Use code with caution.jsonmaxcomforthvac.comUse code with caution.json\"},"
                    "{\"company\":\"Precision Air Tech\",\"domain\":\"precisionairil.com\", Use code with caution. "
                    "\"address\":\"315 S Bothwell St\",\"email\":\"ryan@precisionairil.com\"}"
                    "] trailing Google UI text"
                )
            },
        }

        rows = self.main._extract_http_source_rows(payload, "", {"response_path": "result.ai_answer"})

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["domain"], "maxcomforthvac.com")
        self.assertEqual(rows[1]["address"], "315 S Bothwell St")

    def test_http_source_url_template_injects_encoded_query(self):
        rendered = self.main._render_http_source_url_template(
            "http://73.72.215.253:4865/api/run-sync?scriptName=ai_overview.js&request={query}",
            {},
            "top 10 locksmiths in chicago",
            "top 10 locksmiths in chicago, answer in json like: {company, domain}",
        )

        self.assertEqual(
            rendered,
            "http://73.72.215.253:4865/api/run-sync?scriptName=ai_overview.js&request=top%2010%20locksmiths%20in%20chicago%2C%20answer%20in%20json%20like%3A%20%7Bcompany%2C%20domain%7D",
        )

    def test_http_source_job_serializer_includes_failed_requests(self):
        idle = self.main._serialize_http_source_job(None)
        self.assertEqual(idle["failed_requests"], 0)

        payload = self.main._serialize_http_source_job({
            "status": "completed_with_errors",
            "campaign_id": 10,
            "processed_requests": 6,
            "failed_requests": 2,
            "saved_contacts": 30,
            "logs": [],
        })

        self.assertEqual(payload["status"], "completed_with_errors")
        self.assertEqual(payload["failed_requests"], 2)

    def test_http_source_error_details_preserve_server_answer_and_logs(self):
        exc = self.main.HTTPException(status_code=502, detail={
            "message": "HTTP source returned 400",
            "request_url": "http://example.com/api?request=locksmiths",
            "response_status": 400,
            "response_text": "{\"ok\":false,\"error\":\"bad query\"}",
        })
        details = self.main._http_source_error_details(
            exc,
            12,
            "top locksmiths",
            {"logs": ["started", "failed"]},
        )

        self.assertEqual(details["message"], "HTTP source returned 400")
        self.assertEqual(details["request_id"], 12)
        self.assertEqual(details["response_status"], 400)
        self.assertIn("bad query", details["response_text"])
        self.assertEqual(details["job_logs"], ["started", "failed"])

    def test_http_source_request_failure_marks_request_failed(self):
        self.main._fetch_http_source_rows = lambda *_args, **_kwargs: (_ for _ in ()).throw(Exception("bad response"))
        self._patch_db([
            {"match": "update requests set status = 'failed'"},
        ])

        with self.assertRaises(Exception):
            self.main._process_http_source_campaign_request(
                7,
                {"config": {"request_template": "{{query}}"}},
                {"id": 3, "name": "HTTP", "source_type": "http_api", "config": {}},
                {"id": 12, "req_text": "bad query"},
            )

    def test_stage_complete_advances_to_next_stage(self):
        self._patch_db([
            {
                "match": "select * from pipeline_runs where id = %s",
                "fetchone": {"id": 77, "campaign_id": 9, "status": "running", "current_stage": "cleanup_contacts", "worker_id": "daemon-1"},
            },
            {"match": "from pipeline_run_locks", "fetchone": None},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "select coalesce(scrape_maps_only, false)", "fetchone": {"scrape_maps_only": False}},
            {"match": "update pipeline_runs", "rowcount": 1},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_locks", "rowcount": 1},
        ])

        response = asyncio.run(self.main.complete_pipeline_stage(77, FakeRequest({"worker_id": "daemon-1", "stage": "cleanup_contacts"})))
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["completed_stage"], "cleanup_contacts")
        self.assertEqual(response["next_stage"], "email_fast")
        self.assertEqual(response["pipeline_status"], "running")
        self.assertEqual(response["skipped_stages"], [])

    def test_stage_complete_skips_email_stages_when_maps_only_enabled(self):
        self._patch_db([
            {
                "match": "select * from pipeline_runs where id = %s",
                "fetchone": {"id": 78, "campaign_id": 10, "status": "running", "current_stage": "cleanup_contacts", "worker_id": "daemon-1"},
            },
            {"match": "from pipeline_run_locks", "fetchone": None},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "select coalesce(scrape_maps_only, false)", "fetchone": {"scrape_maps_only": True}},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "update pipeline_runs", "rowcount": 1},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_locks", "rowcount": 1},
        ])

        response = asyncio.run(self.main.complete_pipeline_stage(78, FakeRequest({"worker_id": "daemon-1", "stage": "cleanup_contacts"})))
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["completed_stage"], "cleanup_contacts")
        self.assertEqual(response["next_stage"], "finalize")
        self.assertEqual(response["pipeline_status"], "running")
        self.assertEqual(response["skipped_stages"], ["email_fast", "email_fallback"])

    def test_claim_returns_maps_mode_and_machine_fields(self):
        self._patch_db([
            {"match": "pg_advisory_xact_lock"},
            {"match": "from pipeline_run_locks prl", "fetchall": []},
            {
                "match": "from pipeline_runs pr",
                "fetchall": [
                    {
                        "id": 42,
                        "campaign_id": 7,
                        "status": "pending",
                        "current_stage": "maps_scrape",
                        "worker_id": None,
                        "lease_expires_at": None,
                        "campaign_name": "HVAC Dallas",
                        "maps_scrape_mode": "fast",
                    }
                ],
            },
            {
                "match": "from pipeline_run_stages",
                "fetchall": [{"run_id": 42, "stage": "maps_scrape", "stage_order": 0, "status": "pending"}],
            },
            {"match": "from pipeline_run_locks where run_id", "fetchone": None},
            {"match": "insert into pipeline_run_locks", "rowcount": 1},
            {"match": "update pipeline_runs", "rowcount": 1},
            {"match": "update pipeline_run_stages", "rowcount": 1},
        ])

        response = asyncio.run(self.main.claim_pipeline_stage(FakeRequest({"worker_id": "daemon-a"})))
        self.assertTrue(response["claimed"])
        self.assertEqual(response["run_id"], 42)
        self.assertEqual(response["campaign_id"], 7)
        self.assertEqual(response["stage"], "maps_scrape")
        self.assertEqual(response["pipeline_status"], "running")
        self.assertEqual(response["maps_scrape_mode"], "fast")
        self.assertEqual(response["worker_id"], "daemon-a")
        self.assertEqual(response["machine_id"], "daemon-a")

    def test_claim_free_machine_policy_blocks_second_run_even_without_other_workers(self):
        now = datetime.utcnow()
        self._patch_db([
            {"match": "pg_advisory_xact_lock"},
            {
                "match": "from pipeline_run_locks prl",
                "fetchall": [
                    {"run_id": 10, "worker_id": "daemon-a", "lease_expires_at": now + timedelta(seconds=120)},
                ],
            },
            {
                "match": "from pipeline_runs pr",
                "fetchall": [{"id": 99, "campaign_id": 8, "status": "pending", "current_stage": "email_fast"}],
            },
            {
                "match": "from pipeline_run_stages",
                "fetchall": [{"run_id": 99, "stage": "email_fast", "stage_order": 2, "status": "pending"}],
            },
            {
                "match": "from pipeline_run_locks where run_id",
                "fetchone": None,
            },
        ])

        response = asyncio.run(self.main.claim_pipeline_stage(FakeRequest({"worker_id": "daemon-a"})))
        self.assertFalse(response["claimed"])
        self.assertEqual(response["reason"], "all_leased")

    def test_fail_and_retry_start_flow(self):
        self._patch_db([
            {
                "match": "select * from pipeline_runs where id = %s",
                "fetchone": {"id": 88, "campaign_id": 12, "status": "running", "current_stage": "email_fast", "worker_id": "daemon-1"},
            },
            {"match": "from pipeline_run_locks", "fetchone": None},
            {"match": "update pipeline_run_stages", "rowcount": 1},
            {"match": "update pipeline_runs", "rowcount": 1},
            {"match": "delete from pipeline_run_locks", "rowcount": 1},
        ])
        fail_response = asyncio.run(
            self.main.fail_pipeline_run(
                88,
                FakeRequest({"worker_id": "daemon-1", "stage": "email_fast", "error": "provider timeout"}),
            )
        )
        self.assertEqual(fail_response["status"], "failed")
        self.assertEqual(fail_response["stage"], "email_fast")

        self._patch_db([
            {"match": "select id from search_campaigns", "fetchone": {"id": 12}},
            {"match": "from pipeline_runs", "fetchone": None},
            {"match": "select retries", "fetchone": {"retries": 2}},
            {"match": "from requests", "fetchone": {"total_requests": 0, "completed_requests": 0, "pending_requests": 0, "inuse_requests": 0, "reserved_requests": 0}},
            {"match": "insert into pipeline_runs", "fetchone": {"id": 99, "status": "pending", "current_stage": "maps_scrape"}},
            {"match": "insert into pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_stages", "rowcount": 1},
            {"match": "insert into pipeline_run_stages", "rowcount": 1},
        ])
        retry_response = asyncio.run(self.main.start_campaign_pipeline(12, FakeRequest({"retry": True})))
        self.assertFalse(retry_response["idempotent"])
        self.assertEqual(retry_response["run_id"], 99)
        self.assertEqual(retry_response["current_stage"], "maps_scrape")

    def test_create_campaign_defaults_maps_mode_to_slow(self):
        self._patch_db([
            {"match": "insert into search_campaigns", "fetchone": {"id": 123}},
            {"match": "insert into requests", "rowcount": 1},
            {"match": "insert into requests", "rowcount": 1},
        ])

        response = asyncio.run(self.main.create_campaign(name="Demo", search_phrases="a\nb"))
        self.assertEqual(response["campaign_id"], 123)
        self.assertEqual(response["maps_scrape_mode"], "slow")
        self.assertFalse(response["scrape_maps_only"])

    def test_create_campaign_accepts_scrape_maps_only_toggle(self):
        self._patch_db([
            {"match": "insert into search_campaigns", "fetchone": {"id": 124}},
            {"match": "insert into requests", "rowcount": 1},
        ])

        response = asyncio.run(
            self.main.create_campaign(
                name="MapsOnly",
                search_phrases="a",
                maps_scrape_mode="fast",
                scrape_maps_only="1",
            )
        )
        self.assertEqual(response["campaign_id"], 124)
        self.assertEqual(response["maps_scrape_mode"], "fast")
        self.assertTrue(response["scrape_maps_only"])

    def test_create_campaign_auto_starts_http_source_runner(self):
        started = []
        self.main._start_http_source_campaign_job = lambda campaign_id: started.append(campaign_id) or True
        self._patch_db([
            {
                "match": "select * from source_templates",
                "fetchone": {
                    "id": 7,
                    "name": "HTTP",
                    "description": "",
                    "source_type": "http_api",
                    "enabled": True,
                    "config": {
                        "base_url": "http://example.com/api?request={query}",
                        "field_mapping": {"company": "business_name"},
                    },
                },
            },
            {"match": "insert into search_campaigns", "fetchone": {"id": 125}},
            {"match": "insert into requests", "rowcount": 1},
        ])

        response = asyncio.run(
            self.main.create_campaign(
                name="HTTP Campaign",
                search_phrases="top locksmiths in chicago",
                source_template_id="7",
            )
        )

        self.assertEqual(response["campaign_id"], 125)
        self.assertTrue(response["http_source_runner_started"])
        self.assertEqual(started, [125])

    def test_create_campaign_rejects_invalid_maps_mode(self):
        with self.assertRaises(self.main.HTTPException) as ctx:
            asyncio.run(
                self.main.create_campaign(
                    name="Demo",
                    search_phrases="a",
                    maps_scrape_mode="turbo",
                )
            )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_duplicate_campaign_from_finalized_pipeline_becomes_inactive(self):
        self._patch_db([
            {"match": "select * from search_campaigns where id = %s", "fetchone": {"id": 20, "name": "Demo", "status": "active", "maps_scrape_mode": "slow", "scrape_maps_only": False}},
            {"match": "select id from search_campaigns where name = %s", "fetchone": None},
            {"match": "select status, current_stage from pipeline_runs", "fetchone": {"status": "completed", "current_stage": "finalize"}},
            {"match": "insert into search_campaigns", "fetchone": {"id": 120}},
            {"match": "select req_text, status from requests where campaign_id = %s", "fetchall": [{"req_text": "a", "status": "completed"}, {"req_text": "b", "status": "pending"}]},
            {"match": "insert into requests", "rowcount": 1},
            {"match": "insert into requests", "rowcount": 1},
        ])

        response = asyncio.run(
            self.main.duplicate_campaign(
                20,
                FakeRequest({"name": "Demo Copy", "contactFilters": {}}),
            )
        )
        self.assertEqual(response["status"], "success")
        self.assertEqual(response["new_campaign_id"], 120)
        self.assertEqual(response["new_campaign_status"], "inactive")
        self.assertEqual(response["copied_requests"], 2)

    def test_set_campaign_daemon_ignore_toggle(self):
        self._patch_db([
            {"match": "select coalesce(daemon_ignore, false)", "fetchone": {"daemon_ignore": False}},
            {"match": "update search_campaigns set daemon_ignore", "rowcount": 1},
        ])

        response = asyncio.run(
            self.main.set_campaign_daemon_ignore(
                33,
                FakeRequest({}),
            )
        )
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["campaign_id"], 33)
        self.assertTrue(response["daemon_ignore"])

    def test_set_campaign_daemon_ignore_explicit_value(self):
        self._patch_db([
            {"match": "select coalesce(daemon_ignore, false)", "fetchone": {"daemon_ignore": True}},
            {"match": "update search_campaigns set daemon_ignore", "rowcount": 1},
        ])

        response = asyncio.run(
            self.main.set_campaign_daemon_ignore(
                34,
                FakeRequest({"daemon_ignore": False}),
            )
        )
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["campaign_id"], 34)
        self.assertFalse(response["daemon_ignore"])

    def test_cleanup_flags_invalid_domains_and_removes_duplicates(self):
        self._patch_db([
            {"match": "select id from search_campaigns", "fetchone": {"id": 3}},
            {
                "match": "select id, domain, email, phone, place_id, business_name, status from contacts",
                "fetchall": [
                    {"id": 1, "domain": "example.com", "email": "a@example.com", "phone": "", "place_id": "", "business_name": "A", "status": "pending"},
                    {"id": 2, "domain": "example.com", "email": "a@example.com", "phone": "", "place_id": "", "business_name": "A2", "status": "pending"},
                    {"id": 3, "domain": "", "email": "", "phone": "", "place_id": "", "business_name": "B", "status": "pending"},
                    {"id": 4, "domain": "bad_domain", "email": "", "phone": "", "place_id": "", "business_name": "C", "status": "pending"},
                ],
            },
            {"match": "update contacts", "rowcount": 2},
            {"match": "delete from contacts", "rowcount": 1},
            {"match": "select count(*) as count from contacts", "fetchone": {"count": 3}},
        ])

        response = asyncio.run(self.main.cleanup_campaign_contacts(3))
        self.assertEqual(response["before_count"], 4)
        self.assertEqual(response["after_count"], 3)
        self.assertEqual(response["duplicates_removed"], 1)
        self.assertEqual(response["flagged_invalid_domain_count"], 2)

    def test_save_contacts_allows_inflight_request_when_campaign_not_active(self):
        self._patch_db([
            {"match": "select status from search_campaigns", "fetchone": {"status": "completed"}},
            {"match": "select id, status from requests", "fetchone": {"id": 777, "status": "inuse"}},
            {"match": "from pipeline_runs", "fetchone": None},
            {"match": "insert into contacts", "fetchone": {"id": 501}},
        ])

        payload = [{
            "campaign_id": 12,
            "request_id": 777,
            "business_name": "Demo Biz",
            "address": "123 Main St",
        }]
        response = asyncio.run(self.main.save_contacts(FakeRequest(payload)))
        self.assertEqual(response["status"], "Contacts saved successfully")
        self.assertEqual(len(response["saved_contacts"]), 1)
        self.assertEqual(response["saved_contacts"][0]["contact_id"], 501)

    def test_save_contacts_rejects_inactive_campaign_without_inflight_context(self):
        self._patch_db([
            {"match": "select status from search_campaigns", "fetchone": {"status": "completed"}},
            {"match": "select id, status from requests", "fetchone": {"id": 777, "status": "completed"}},
            {"match": "from pipeline_runs", "fetchone": None},
        ])

        payload = [{
            "campaign_id": 12,
            "request_id": 777,
            "business_name": "Demo Biz",
        }]
        with self.assertRaises(self.main.HTTPException) as ctx:
            asyncio.run(self.main.save_contacts(FakeRequest(payload)))
        self.assertEqual(ctx.exception.status_code, 400)


class PipelineUnitLogicTests(unittest.TestCase):
    def setUp(self):
        self.main = load_main_module()

    def test_next_stage_mapping(self):
        self.assertEqual(self.main._next_pipeline_stage("maps_scrape"), "cleanup_contacts")
        self.assertEqual(self.main._next_pipeline_stage("cleanup_contacts"), "email_fast")
        self.assertEqual(self.main._next_pipeline_stage("email_fast"), "email_fallback")
        self.assertEqual(self.main._next_pipeline_stage("email_fallback"), "finalize")
        self.assertIsNone(self.main._next_pipeline_stage("finalize"))

    def test_domain_normalization_and_validation(self):
        normalized = self.main._normalize_domain("https://www.Example.com/path?x=1")
        self.assertEqual(normalized, "example.com")
        self.assertTrue(self.main._is_valid_domain(normalized))
        self.assertFalse(self.main._is_valid_domain("bad_domain"))
        self.assertFalse(self.main._is_valid_domain(""))

    def test_is_valid_email_lead_requires_email_and_valid_status(self):
        self.assertTrue(self.main._is_valid_email_lead({"email": "a@example.com", "email_status": "Valid"}))
        self.assertTrue(self.main._is_valid_email_lead({"email": "a@example.com", "email_status": "verified"}))
        self.assertFalse(self.main._is_valid_email_lead({"email": "a@example.com", "email_status": "Invalid"}))
        self.assertFalse(self.main._is_valid_email_lead({"email": "", "email_status": "Valid"}))

    def test_stats_computation(self):
        contacts = [
            {"id": 1, "domain": "example.com", "email": "a@example.com", "phone": "", "place_id": "", "business_name": "A"},
            {"id": 2, "domain": "example.com", "email": "a@example.com", "phone": "", "place_id": "", "business_name": "A2"},
            {"id": 3, "domain": "", "email": "", "phone": "", "place_id": "", "business_name": "B"},
        ]
        stats = self.main._compute_campaign_stats_from_contacts(contacts, last_updated_at="2026-03-19T00:00:00")
        self.assertEqual(stats["total_contacts"], 3)
        self.assertEqual(stats["unique_contacts"], 2)
        self.assertEqual(stats["duplicates_removed"], 1)
        self.assertEqual(stats["contacts_with_domain"], 2)
        self.assertEqual(stats["contacts_without_domain"], 1)

    def test_stage_recent_heartbeat_blocks_reclaim(self):
        now = datetime.utcnow()
        run = {"worker_id": "daemon-a", "last_heartbeat_at": now - timedelta(seconds=30)}
        stage_row = {"worker_id": "daemon-a", "last_heartbeat_at": now - timedelta(seconds=30)}
        self.assertTrue(self.main._stage_recently_heartbeated_by_other_worker(run, stage_row, "daemon-b", now))
        self.assertFalse(self.main._stage_recently_heartbeated_by_other_worker(run, stage_row, "daemon-a", now))

    def test_extract_city_from_request_text_uses_common_pattern(self):
        city = self.main._extract_city_from_request_text(
            "lawn care Colfax, WI",
            common_prefix="lawn care ",
            common_suffix=", WI",
        )
        self.assertEqual(city, "Colfax")

    def test_apply_city_fallback_for_export_uses_request_city_map(self):
        contacts = [
            {"id": 1, "city": "", "address": "", "request_id": 1001},
            {"id": 2, "city": "Elmwood", "address": "", "request_id": 1002},
        ]
        self.main._apply_city_fallback_for_export(contacts, {1001: "Mondovi", 1002: "Durand"})
        self.assertEqual(contacts[0]["city"], "Mondovi")
        self.assertEqual(contacts[1]["city"], "Elmwood")

    def test_resolve_enrichment_input_value_supports_literal_and_field(self):
        contact = {"city": "Chicago", "state": "IL", "notes": "one two three four"}
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "city"), "Chicago")
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "literal:Wisconsin"), "Wisconsin")
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "TX"), "TX")
        self.assertEqual(
            self.main._resolve_enrichment_input_value(
                contact,
                {"source": "notes", "crop": {"enabled": True, "word_limit": 2}},
            ),
            "one two",
        )
        self.assertEqual(
            self.main._resolve_enrichment_input_value(
                contact,
                {"source": "literal:alpha beta gamma", "crop": {"enabled": True, "word_limit": 2}},
            ),
            "alpha beta",
        )

    def test_build_enrichment_payload_tracks_missing_required(self):
        contact = {"business_name": "Acme", "city": "", "state": "WI"}
        payload, missing = self.main._build_enrichment_payload(
            contact,
            {"company": "business_name", "city": "city", "state": "state"},
            ["company", "city", "state"],
        )
        self.assertEqual(payload["company"], "Acme")
        self.assertEqual(payload["state"], "WI")
        self.assertIn("city", missing)

    def test_build_enrichment_payload_crops_configured_input_words(self):
        contact = {"notes": "first second third fourth", "state": "WI"}
        payload, missing = self.main._build_enrichment_payload(
            contact,
            {
                "post_text": {"source": "notes", "crop": {"enabled": True, "word_limit": 3}},
                "state": "state",
            },
            ["post_text", "state"],
        )
        self.assertEqual(payload["post_text"], "first second third")
        self.assertEqual(payload["state"], "WI")
        self.assertEqual(missing, [])

    def test_enrichment_post_sends_wrapped_input_then_flat_json_fallback(self):
        calls = []

        class Response:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload
                self.text = str(payload)

            def json(self):
                return self._payload

        def fake_post(_url, headers=None, json=None, timeout=None):
            calls.append({"headers": headers, "json": json, "timeout": timeout})
            if len(calls) == 1:
                return Response(400, {"error": "Missing required input fields: fb_post_text"})
            return Response(200, {"ok": True})

        original_post = self.main.requests.post
        self.main.requests.post = fake_post
        try:
            response, encoding, request_body = self.main._post_enrichment_request(
                "https://example.test/enrich",
                {"Content-Type": "application/json", "x-api-key": "key"},
                {"fb_post_text": "hello"},
                15,
            )
        finally:
            self.main.requests.post = original_post

        self.assertEqual(response.status_code, 200)
        self.assertEqual(encoding, "json_flat")
        self.assertEqual(request_body, {"fb_post_text": "hello"})
        self.assertEqual(calls[0]["json"], {"input": {"fb_post_text": "hello"}})
        self.assertEqual(calls[1]["json"], {"fb_post_text": "hello"})
        self.assertEqual(calls[1]["headers"]["Content-Type"], "application/json")

    def test_enrichment_post_sends_selected_output_fields(self):
        calls = []

        class Response:
            status_code = 200
            text = "{}"

            def json(self):
                return {"ok": True}

        def fake_post(_url, headers=None, json=None, timeout=None):
            calls.append({"headers": headers, "json": json, "timeout": timeout})
            return Response()

        original_post = self.main.requests.post
        self.main.requests.post = fake_post
        try:
            response, encoding, request_body = self.main._post_enrichment_request(
                "https://example.test/enrich",
                {"Content-Type": "application/json", "x-api-key": "key"},
                {"company": "Acme"},
                15,
                ["owner_name", "top_service"],
            )
        finally:
            self.main.requests.post = original_post

        self.assertEqual(response.status_code, 200)
        self.assertEqual(encoding, "json_input")
        self.assertEqual(request_body, {
            "input": {"company": "Acme"},
            "enrichment_fields": ["owner_name", "top_service"],
        })
        self.assertEqual(calls[0]["json"], request_body)

    def test_selected_enrichment_fields_use_output_mapping_keys(self):
        selected = self.main._selected_enrichment_fields({
            "owner_name": "firstname",
            "top_service": "custom_2",
            "": "custom_3",
            "owner_name ": "custom_4",
        })
        self.assertEqual(selected, ["owner_name", "top_service"])

    def test_enrichment_payload_uses_city_fallback_from_request_map(self):
        contacts = [{"id": 1, "business_name": "Acme", "city": "", "state": "WI", "request_id": 1001, "address": ""}]
        self.main._apply_city_fallback_for_export(contacts, {1001: "Mondovi"})
        payload, missing = self.main._build_enrichment_payload(
            contacts[0],
            {"company": "business_name", "city": "city", "state": "state"},
            ["company", "city", "state"],
        )
        self.assertEqual(payload["city"], "Mondovi")
        self.assertEqual(missing, [])

    def test_apply_enrichment_output_mapping_validates_local_fields(self):
        mapped = self.main._apply_enrichment_output_mapping(
            {"owner_firstname": "Shay", "top_service": "Locksmith"},
            {"owner_firstname": "firstname", "top_service": "custom_2", "bad": "not_real_field"},
        )
        self.assertEqual(mapped["firstname"], "Shay")
        self.assertEqual(mapped["custom_2"], "Locksmith")
        self.assertNotIn("not_real_field", mapped)

    def test_compute_enrichment_progress_payload_prefers_active_run(self):
        active = {
            "id": 9,
            "campaign_id": 3,
            "template_id": 2,
            "status": "running",
            "total_contacts": 100,
            "processed_contacts": 40,
            "enriched_contacts": 30,
            "failed_contacts": 5,
            "skipped_contacts": 5,
            "current_contact_name": "Acme",
            "pause_requested": False,
            "cancel_requested": False,
            "concurrency": 2,
            "max_retries": 1,
            "overwrite_existing": False,
            "skip_missing_input": True,
        }
        latest = {
            "id": 8,
            "campaign_id": 3,
            "template_id": 2,
            "status": "completed",
            "total_contacts": 20,
            "processed_contacts": 20,
            "enriched_contacts": 20,
            "failed_contacts": 0,
            "skipped_contacts": 0,
            "pause_requested": False,
            "cancel_requested": False,
            "concurrency": 1,
            "max_retries": 1,
            "overwrite_existing": False,
            "skip_missing_input": True,
        }
        payload = self.main._compute_enrichment_progress_payload(3, active, latest)
        self.assertEqual(payload["run_id"], 9)
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["processed_contacts"], 40)

    def test_domain_checker_template_detection(self):
        self.assertTrue(self.main._is_domain_checker_template({"service": "dns_ssl_checker"}))
        self.assertTrue(self.main._is_domain_checker_template({"service": " DNS_SSL_CHECKER "}))
        self.assertFalse(self.main._is_domain_checker_template({"service": "myemailverifier"}))
        self.assertFalse(self.main._is_domain_checker_template(None))

    def test_domain_checker_api_config_normalizes_concurrency(self):
        self.assertEqual(
            self.main._normalize_domain_checker_api_config({})["concurrency"],
            self.main.DOMAIN_CHECKER_DEFAULT_CONCURRENCY,
        )
        self.assertEqual(
            self.main._normalize_domain_checker_api_config({"concurrency": 0})["concurrency"],
            1,
        )
        self.assertEqual(
            self.main._normalize_domain_checker_api_config({"concurrency": 999})["concurrency"],
            self.main.DOMAIN_CHECKER_MAX_CONCURRENCY,
        )

    def test_verification_job_serialization_includes_template_service(self):
        payload = self.main._serialize_verification_job({
            "job_id": "job-1",
            "campaign_id": 10,
            "template_id": 20,
            "template_service": "dns_ssl_checker",
            "status": "running",
            "total_emails": 4,
            "processed_emails": 2,
            "verified_emails": 1,
            "invalid_emails": 1,
            "failed_emails": 0,
            "current_email": "example.com",
            "message": "Checking domains",
            "cancel_requested": False,
            "skip_public_providers": False,
            "started_at": "2026-06-14T12:00:00",
            "completed_at": None,
            "logs": ["Checking example.com"],
        })

        self.assertEqual(payload["template_service"], "dns_ssl_checker")
        self.assertEqual(payload["current_email"], "example.com")


if __name__ == "__main__":
    unittest.main()
