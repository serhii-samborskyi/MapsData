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

    requests_module.post = _dummy_post
    requests_module.get = _dummy_get
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
        contact = {"city": "Chicago", "state": "IL"}
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "city"), "Chicago")
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "literal:Wisconsin"), "Wisconsin")
        self.assertEqual(self.main._resolve_enrichment_input_value(contact, "TX"), "TX")

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


if __name__ == "__main__":
    unittest.main()
