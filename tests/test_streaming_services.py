"""Streaming adapter contracts, using real main helpers and no network or DB."""

import importlib.util
import json
import socket
import sys
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import streaming_services
try:
    from tests.test_pipeline_automation import load_main_module
except ImportError:
    from test_pipeline_automation import load_main_module


class StreamingServicesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reuse the repository's dependency stubs without leaking them to peers.
        with patch.dict(sys.modules, {"streaming": ModuleType("streaming")}):
            cls.app = load_main_module()
            spec = importlib.util.spec_from_file_location(
                "streaming_test_verification", Path(__file__).resolve().parents[1] / "email_verification.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            cls.verification = module

    def setUp(self):
        self.get = Mock(side_effect=AssertionError("Unexpected HTTP GET"))
        self.post = Mock(side_effect=AssertionError("Unexpected HTTP POST"))
        self.service = Mock()
        patches = [
            patch.object(self.app.requests, "get", self.get),
            patch.object(self.app.requests, "post", self.post),
            patch.object(self.app, "EmailVerificationService", return_value=self.service),
            patch.object(self.app, "get_db", side_effect=AssertionError("Database access forbidden")),
            patch.object(self.app, "_wait_for_prompt_slot", side_effect=AssertionError("Engine owns pacing")),
            patch.object(self.app, "_post_enrichment_request", side_effect=AssertionError("Nested POST forbidden")),
            patch.object(self.app.time, "sleep", side_effect=AssertionError("Nested delay forbidden")),
            patch.object(socket, "getaddrinfo", side_effect=AssertionError("Real DNS forbidden")),
            patch.object(socket, "create_connection", side_effect=AssertionError("Real network forbidden")),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        self.context = {"app": self.app, "run_id": 1, "task_id": 2, "campaign_id": 3,
                        "cancelled": lambda: False}
        self.contact = {"id": 4, "business_name": "Acme & Sons", "company": "", "city": "Austin",
                        "email": "old@example.test", "email_status": "Valid", "domain": "example.test",
                        "domain_status": "Domain OK"}

    def response(self, data, status=200, headers=None, text=None):
        return SimpleNamespace(status_code=status, json=Mock(return_value=data),
                               text=json.dumps(data) if text is None else text,
                               content=b"{}", headers=headers or {})

    def config(self, service="prompt_http", **options):
        if service == "prompt_http":
            template = {"id": 1, "service": service,
                        "api_config": {"api_url": "https://api.example.test/find?prompt={prompt}&mode=full",
                                       "prompt_template": "Find {{company}} in {{city}}; {{email}}",
                                       "requests_per_minute": 20, "timeout_seconds": 40},
                        "output_mapping": {"owner.name": "full_name", "email": "email", "site": "domain"}}
        elif service == "dns_ssl_checker":
            template = {"id": 2, "service": service, "api_config": {"timeout_seconds": 4}, "status_mapping": {}}
        elif service == "myemailverifier":
            template = {"id": 3, "service": service, "api_config": {"api_key": "test-only-key"},
                        "status_mapping": {"valid": "Valid"}}
        else:
            template = {"id": 4, "service": service,
                        "api_config": {"api_url": "https://api.example.test/enrich", "api_key": "test-only-key"},
                        "input_mapping": {"company": "business_name", "city": "city"},
                        "output_mapping": {"name": "full_name", "email": "email"},
                        "schema_cache": {"required_input_fields": ["company", "city"]}}
        return {"template_snapshot": template, **options}

    def run_step(self, step="enrichment", config=None, contact=None, context=None):
        return streaming_services.execute(step, config if config is not None else self.config(),
                                          contact if contact is not None else self.contact,
                                          context if context is not None else self.context)

    def prompt_response(self, values=None):
        answer = {"owner": {"name": "Ada Example"}, "email": "new@example.test", "site": "new.test"}
        self.get.side_effect = None
        self.get.return_value = self.response({"result": {"ai_answer": json.dumps(answer if values is None else values)}})

    def domain_result(self, **changes):
        return {"success": True, "dns_ok": True, "mapped_status": "Domain OK", "addresses": ["192.0.2.1"],
                "http": {"status_code": 301, "reachable": True},
                "https": {"status_code": 200, "reachable": True}, "ssl": {"valid": True}, **changes}

    def test_dispatch_guard_can_defer_without_calling_any_provider(self):
        for step, service in (("enrichment", "prompt_http"), ("enrichment", "http_enrichment"),
                              ("dns_check", "dns_ssl_checker"), ("email_verification", "myemailverifier")):
            with self.subTest(service=service):
                guard = Mock(return_value=False)
                outcome = self.run_step(step, self.config(service, overwrite_existing=True),
                                        context={**self.context, "before_request": guard})
                self.assertEqual(outcome["status"], "retry")
                guard.assert_called_once()
                self.get.assert_not_called()
                self.post.assert_not_called()
                self.service.verify_domain.assert_not_called()
                self.service.verify_batch.assert_not_called()

    def test_prompt_uses_frozen_template_company_fallback_and_single_get(self):
        config = self.config(overwrite_existing=True, max_retries=10)
        self.prompt_response()
        outcome = self.run_step(config=config)
        self.assertEqual(outcome["status"], "completed")
        self.assertEqual(outcome["error"], "")
        self.assertEqual(outcome["updates"], {"full_name": "Ada Example", "email": "new@example.test",
                                             "email_status": "unverified", "domain": "new.test"})
        self.assertNotIn("domain_status", outcome["updates"])
        self.assertEqual(outcome["result"]["values"]["owner.name"], "Ada Example")
        args, kwargs = self.get.call_args
        self.assertEqual(parse_qs(urlsplit(args[0]).query)["prompt"],
                         ["Find Acme & Sons in Austin; old@example.test"])
        self.assertEqual(kwargs, {"headers": {"Accept": "application/json"}, "timeout": 40})
        self.get.assert_called_once()
        self.post.assert_not_called()

    def test_partial_output_retains_existing_values_and_preserves_extraction(self):
        self.prompt_response()
        outcome = self.run_step()
        self.assertEqual(outcome["updates"], {"full_name": "Ada Example"})
        self.assertEqual(outcome["result"]["values"]["email"], "new@example.test")
        self.assertEqual(len(outcome["result"]["field_results"]), 3)

    def test_unchanged_normalized_email_keeps_status(self):
        self.prompt_response({"email": "OLD@example.test "})
        outcome = self.run_step(config=self.config(overwrite_existing=True))
        self.assertEqual(outcome["updates"]["email"], "OLD@example.test")
        self.assertNotIn("email_status", outcome["updates"])

    def test_only_missing_outputs_skip_and_placeholder_outputs_are_fillable(self):
        self.contact["full_name"] = "Already Present"
        self.assertEqual(self.run_step()["result"]["reason"], "output_already_present")
        self.get.assert_not_called()
        self.contact["full_name"] = "Unknown"
        self.prompt_response({"owner": {"name": "Ada"}})
        self.assertEqual(self.run_step()["updates"], {"full_name": "Ada"})

    def test_prompt_failure_diagnostics_are_separate_from_log_summary(self):
        self.get.side_effect = None
        self.get.return_value = self.response({}, 503, text="PRIVATE BODY " * 2000)
        outcome = self.run_step(config=self.config(max_retries=99))
        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["updates"], {})
        self.assertEqual(outcome["error"], "API request failed (HTTP 503).")
        self.assertIn("PRIVATE BODY", outcome["result"]["diagnostics"]["response_text"])
        self.get.assert_called_once()

    def test_prompt_auth_errors_block_shared_credentials(self):
        for code in (401, 403):
            with self.subTest(code=code):
                self.get.side_effect = None
                self.get.return_value = self.response({}, code, text="Private diagnostic")
                outcome = self.run_step()
                self.assertEqual(outcome["status"], "blocked")
                self.assertEqual(outcome["updates"], {})
                self.assertNotIn("Private", outcome["error"])

    def test_prompt_empty_and_invalid_extraction_fail_without_retry(self):
        for data in ({"result": {"ai_answer": "not json"}}, {"email": "unknown"},
                     {"ok": False, "error": "unavailable"}):
            with self.subTest(data=data):
                self.get.reset_mock()
                self.get.side_effect = None
                self.get.return_value = self.response(data)
                outcome = self.run_step()
                self.assertEqual(outcome["status"], "failed")
                self.get.assert_called_once()

    def test_legacy_mappings_cropping_selected_fields_and_overwrite(self):
        for service in ("http_enrichment", "legacy_service_label"):
            with self.subTest(service=service):
                config = self.config(service, overwrite_existing=True, max_retries=8)
                config["template_snapshot"]["input_mapping"]["excerpt"] = {
                    "source": "literal:one two three", "crop": {"enabled": True, "word_limit": 2}}
                self.post.side_effect = None
                self.post.return_value = self.response({"name": "Ada", "email": "new@example.test", "extra": "diagnostic"})
                self.post.reset_mock()
                outcome = self.run_step(config=config)
                self.assertEqual(outcome["status"], "completed")
                self.assertEqual(outcome["updates"]["email_status"], "unverified")
                self.assertEqual(outcome["result"]["values"], {"name": "Ada", "email": "new@example.test"})
                self.assertEqual(self.post.call_args.kwargs["json"], {
                    "input": {"company": "Acme & Sons", "city": "Austin", "excerpt": "one two"},
                    "enrichment_fields": ["name", "email"]})
                self.assertEqual(self.post.call_args.kwargs["headers"]["x-api-key"], "test-only-key")
                self.post.assert_called_once()

    def test_legacy_flat_fallback_is_a_separate_parent_attempt(self):
        config = self.config("http_enrichment")
        self.post.side_effect = None
        self.post.return_value = self.response({"error": "Missing required input fields: company"}, 400)
        first = self.run_step(config=config)
        self.assertEqual(first["status"], "failed")
        self.post.assert_called_once()
        self.assertEqual(first["result"]["retry_config"], {"request_encoding": "json_flat"})
        config.update(first["result"]["retry_config"])
        self.post.return_value = self.response({"name": "Ada"})
        second = self.run_step(config=config)
        self.assertEqual(second["status"], "completed")
        self.assertEqual(self.post.call_args.kwargs["json"],
                         {"company": "Acme & Sons", "city": "Austin", "enrichment_fields": ["name", "email"]})
        self.assertEqual(self.post.call_count, 2)

    def test_legacy_mapping_names_are_trimmed_and_disabled_targets_ignored(self):
        config = self.config("http_enrichment", output_mapping={" name ": " firstname ", "off": ""},
                             required_inputs=[" company ", " city "])
        self.post.side_effect = None
        self.post.return_value = self.response({"name": " Ada ", "off": "ignored"})
        outcome = self.run_step(config=config)
        self.assertEqual(outcome["status"], "completed")
        self.assertEqual(outcome["updates"], {"firstname": "Ada"})
        self.assertEqual(outcome["result"]["field_results"], [
            {"api_field": "name", "local_field": "firstname", "found": True, "value": "Ada"}])

    def test_legacy_single_attempt_auth_retry_after_and_other_failures(self):
        for code, expected in ((401, "blocked"), (403, "blocked"), (429, "failed"), (500, "failed")):
            with self.subTest(code=code):
                self.post.reset_mock()
                self.post.side_effect = None
                self.post.return_value = self.response({}, code, {"Retry-After": "12"})
                outcome = self.run_step(config=self.config("http_enrichment", max_retries=5))
                self.assertEqual(outcome["status"], expected)
                if expected == "failed":
                    self.assertEqual(outcome["retry_after"], 12)
                self.assertEqual(outcome["updates"], {})
                self.post.assert_called_once()

    def test_legacy_http_200_failure_and_no_values_are_retryable(self):
        for data in ({}, [], {"ok": False}, {"success": False}, {"error": "failed"}):
            with self.subTest(data=data):
                self.post.side_effect = None
                self.post.return_value = self.response(data)
                outcome = self.run_step(config=self.config("http_enrichment"))
                self.assertEqual(outcome["status"], "failed")
                self.assertNotIn("HTTP 200", outcome["error"])

    def test_legacy_required_inputs_respect_skip_option(self):
        self.contact["city"] = ""
        config = self.config("http_enrichment")
        outcome = self.run_step(config=config)
        self.assertEqual(outcome["result"]["missing_required"], ["city"])
        self.post.assert_not_called()
        self.post.side_effect = None
        self.post.return_value = self.response({"name": "Ada"})
        config["skip_missing_input"] = False
        self.assertEqual(self.run_step(config=config)["status"], "completed")
        self.post.assert_called_once()

    def test_enrichment_filters_use_main_boolean_and_missing_normalization(self):
        cases = [({"emails_only": "true"}, {"email": ""}, "missing_email"),
                 ({"valid_emails_only": True}, {"email_status": "Invalid"}, "email_not_valid"),
                 ({"missing_field_only": True, "missing_field_name": "city"}, {}, "filter_field_populated")]
        for options, contact, reason in cases:
            with self.subTest(reason=reason):
                outcome = self.run_step(config=self.config(**options), contact={**self.contact, **contact})
                self.assertEqual(outcome["result"]["reason"], reason)
        self.get.assert_not_called()

    def test_serialized_snapshot_mappings_and_run_overrides(self):
        config = self.config("http_enrichment", output_mapping={"name": "firstname"}, timeout_seconds=30)
        template = config["template_snapshot"]
        for key in ("api_config", "input_mapping", "output_mapping", "schema_cache"):
            template[key] = json.dumps(template[key])
        self.post.side_effect = None
        self.post.return_value = self.response({"name": "Ada"})
        outcome = self.run_step(config=config)
        self.assertEqual(outcome["updates"], {"firstname": "Ada"})
        self.assertEqual(self.post.call_args.kwargs["timeout"], 30)

    def test_email_reuses_real_verify_batch_and_never_sleeps(self):
        config = self.config("myemailverifier", max_retries=99)
        self.get.side_effect = None
        self.get.return_value = self.response({"Status": "catch-all", "Diagnosis": "test"})
        with patch.object(self.app, "EmailVerificationService", self.verification.EmailVerificationService):
            outcome = self.run_step("email_verification", config)
        self.assertEqual(outcome["status"], "completed")
        self.assertEqual(outcome["updates"], {"email_status": "Catch-All"})
        self.assertEqual(outcome["result"]["diagnostics"]["diagnosis"], "test")
        self.assertEqual(self.get.call_args.kwargs["timeout"], 30)
        self.get.assert_called_once()

    def test_email_real_service_auth_and_transient_errors(self):
        config = self.config("myemailverifier", max_retries=7)
        for code, status in ((401, "blocked"), (403, "blocked"), (429, "failed"), (503, "failed")):
            with self.subTest(code=code):
                self.get.reset_mock()
                self.get.side_effect = None
                self.get.return_value = self.response({}, code, text="private response")
                with patch.object(self.app, "EmailVerificationService", self.verification.EmailVerificationService):
                    outcome = self.run_step("email_verification", config)
                self.assertEqual(outcome["status"], status)
                self.assertEqual(outcome["updates"], {})
                self.assertNotIn("private response", outcome["error"])
                self.get.assert_called_once()

    def test_email_verified_status_without_matching_fingerprint_still_calls(self):
        config = self.config("myemailverifier")
        self.service.verify_batch.return_value = [{"success": True, "mapped_status": "Valid"}]
        first = self.run_step("email_verification", config)
        self.assertEqual(first["status"], "completed")
        self.service.verify_batch.assert_called_once_with(["old@example.test"],
                                                          unittest.mock.ANY, 0)
        known = first["result"]["input_fingerprint"]
        self.context["verification_fingerprints"] = {"email_verification": known}
        self.assertEqual(self.run_step("email_verification", config)["result"]["reason"], "already_verified")
        self.assertEqual(self.service.verify_batch.call_count, 1)
        self.contact["email"] = "new@example.test"
        self.assertEqual(self.run_step("email_verification", config)["status"], "completed")
        self.assertEqual(self.service.verify_batch.call_count, 2)

    def test_changed_verifier_config_and_unverified_status_force_recheck(self):
        config = self.config("myemailverifier")
        self.service.verify_batch.return_value = [{"success": True, "mapped_status": "Valid"}]
        first = self.run_step("email_verification", config)
        self.context["verification_fingerprints"] = {"email_verification": first["result"]["input_fingerprint"]}
        self.contact["email_status"] = "unverified"
        self.assertEqual(self.run_step("email_verification", config)["status"], "completed")
        self.contact["email_status"] = "Valid"
        config["template_snapshot"]["status_mapping"] = {"valid": "Verified"}
        self.assertEqual(self.run_step("email_verification", config)["status"], "completed")

    def test_email_public_providers_missing_email_and_missing_key(self):
        config = self.config("myemailverifier", skip_public_providers="true")
        self.contact["email"] = "User@GMAIL.com"
        self.assertEqual(self.run_step("email_verification", config)["result"]["reason"], "public_email_provider")
        self.contact["email"] = " "
        self.assertEqual(self.run_step("email_verification", config)["result"]["reason"], "missing_email")
        self.contact["email"] = "user@example.test"
        config["template_snapshot"]["api_config"] = {}
        self.assertEqual(self.run_step("email_verification", config)["status"], "blocked")
        self.service.verify_batch.assert_not_called()

    def test_malformed_verification_result_and_exception_fail_without_updates(self):
        for result in ([], [{}], [None], [{"success": False, "error": "timeout"}]):
            with self.subTest(result=result):
                self.service.verify_batch.return_value = result
                outcome = self.run_step("email_verification", self.config("myemailverifier"))
                self.assertEqual(outcome["status"], "failed")
                self.assertEqual(outcome["updates"], {})
        self.service.verify_batch.side_effect = TimeoutError("Private request URL")
        outcome = self.run_step("email_verification", self.config("myemailverifier"))
        self.assertEqual(outcome["status"], "failed")
        self.assertNotIn("Private", outcome["error"])

    def test_dns_uses_real_verify_domain_and_check_domain(self):
        config = self.config("dns_ssl_checker")
        self.contact["domain"] = "https://www.Example.test/path?q=a"
        with patch.object(self.app, "EmailVerificationService", self.verification.EmailVerificationService), patch.object(
                self.verification.DomainDnsSslIntegration, "check_domain", return_value=self.domain_result()) as check:
            outcome = self.run_step("dns_check", config)
        check.assert_called_once_with("example.test")
        self.assertEqual(outcome["status"], "completed")
        updates = outcome["updates"]
        self.assertEqual(updates["domain_status"], "Domain OK")
        self.assertEqual(updates["domain_dns_status"], "resolved")
        self.assertEqual(updates["domain_http_status"], "301")
        self.assertEqual(updates["domain_https_status"], "200")
        self.assertEqual(updates["domain_ssl_status"], "valid")
        datetime.fromisoformat(updates["domain_last_checked_at"])

    def test_dns_negative_and_website_403_are_findings_not_shared_auth_failure(self):
        for details, status in ((self.domain_result(success=False, dns_ok=False, mapped_status="Expired Domain",
                                                    error="No such host"), "Expired Domain"),
                                (self.domain_result(https={"status_code": 403, "reachable": True}), "Domain OK")):
            with self.subTest(status=status):
                self.service.verify_domain.return_value = details
                outcome = self.run_step("dns_check", self.config("dns_ssl_checker"))
                self.assertEqual(outcome["status"], "completed")
                self.assertEqual(outcome["updates"]["domain_status"], status)
                self.assertEqual(outcome["result"]["diagnostics"], details)

    def test_dns_fingerprint_tracks_normalized_input_and_configuration(self):
        config = self.config("dns_ssl_checker")
        self.service.verify_domain.return_value = self.domain_result()
        first = self.run_step("dns_check", config)
        self.context["verification_fingerprints"] = {"dns_check": first["result"]["input_fingerprint"]}
        self.contact["domain"] = "https://www.EXAMPLE.test/path"
        self.assertEqual(self.run_step("dns_check", config)["result"]["reason"], "already_verified")
        self.contact["domain"] = "changed.test"
        self.assertEqual(self.run_step("dns_check", config)["status"], "completed")
        self.assertEqual(self.service.verify_domain.call_count, 2)

    def test_dns_bad_input_and_service_failure(self):
        config = self.config("dns_ssl_checker")
        for domain in ("", "not_a_domain"):
            with self.subTest(domain=domain):
                self.assertEqual(self.run_step("dns_check", config, {"domain": domain})["status"], "skipped")
        self.service.verify_domain.assert_not_called()
        self.service.verify_domain.return_value = {"mapped_status": "Check Failed", "error": "private error"}
        outcome = self.run_step("dns_check", config)
        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["updates"], {})
        self.assertNotIn("private", outcome["error"])

    def test_dns_http_exception_is_not_a_shared_api_credential_block(self):
        self.service.verify_domain.side_effect = self.app.HTTPException(403, "private target URL")
        outcome = self.run_step("dns_check", self.config("dns_ssl_checker"))
        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["error"], "Domain check failed.")

    def test_enriched_email_cannot_reuse_previous_verification(self):
        verification = self.config("myemailverifier")
        self.service.verify_batch.return_value = [{"success": True, "mapped_status": "Valid"}]
        first = self.run_step("email_verification", verification)
        self.context["verification_fingerprints"] = {"email_verification": first["result"]["input_fingerprint"]}
        self.prompt_response()
        enriched = self.run_step(config=self.config(overwrite_existing=True))
        contact_after_commit = {**self.contact, **enriched["updates"]}
        self.assertEqual(contact_after_commit["email_status"], "unverified")
        second = self.run_step("email_verification", verification, contact_after_commit)
        self.assertEqual(second["status"], "completed")
        self.assertEqual(self.service.verify_batch.call_count, 2)
        self.assertEqual(self.service.verify_batch.call_args.args[0], ["new@example.test"])

    def test_pre_call_cancellation_needs_no_adapter_or_snapshot(self):
        outcome = streaming_services.execute("enrichment", {}, {}, {"cancelled": lambda: True})
        self.assertEqual(outcome["result"]["reason"], "cancelled")
        self.assertEqual(outcome["updates"], {})

    def test_cancellation_after_all_service_types_discards_updates_but_keeps_details(self):
        for step, service in (("enrichment", "prompt_http"), ("enrichment", "http_enrichment"),
                              ("email_verification", "myemailverifier"), ("dns_check", "dns_ssl_checker")):
            with self.subTest(service=service):
                state = {"cancelled": False}
                self.context["cancelled"] = lambda: state["cancelled"]

                def finish(*args, **kwargs):
                    state["cancelled"] = True
                    if service in {"prompt_http", "http_enrichment"}:
                        return self.response({"email": "new@example.test", "name": "Ada"})
                    if service == "myemailverifier":
                        return [{"success": True, "mapped_status": "Valid"}]
                    return self.domain_result()

                self.get.side_effect = self.post.side_effect = finish
                self.service.verify_batch.side_effect = self.service.verify_domain.side_effect = finish
                outcome = self.run_step(step, self.config(service, overwrite_existing=True))
                self.assertEqual(outcome["result"]["reason"], "cancelled")
                self.assertEqual(outcome["updates"], {})
                self.assertIn("diagnostics", outcome["result"])

    def test_input_objects_are_never_mutated(self):
        config = self.config(overwrite_existing=True)
        original_config, original_contact = deepcopy(config), deepcopy(self.contact)
        self.prompt_response()
        streaming_services.limits("enrichment", config)
        self.run_step(config=config)
        self.assertEqual(config, original_config)
        self.assertEqual(self.contact, original_contact)
        dns_config = self.config("dns_ssl_checker")
        original_dns = deepcopy(dns_config)
        self.service.verify_domain.return_value = self.domain_result()
        self.run_step("dns_check", dns_config)
        self.assertEqual(dns_config, original_dns)

    def test_limits_share_prompt_endpoint_across_templates_and_prompt_text(self):
        first, second = self.config(), self.config()
        second["template_snapshot"]["id"] = 99
        api = second["template_snapshot"]["api_config"]
        api["api_url"] = "https://API.example.test/find?mode=full&prompt=prefix{prompt}"
        api["prompt_template"] = "Other prompt {{business_name}}"
        api["requests_per_minute"] = 10
        one, two = (streaming_services.limits("enrichment", config) for config in (first, second))
        self.assertEqual(one["endpoint_key"], two["endpoint_key"])
        self.assertEqual(one["requests_per_minute"], 20)
        self.assertEqual(two["requests_per_minute"], 10)
        self.assertEqual(one["timeout_seconds"], 40)

    def test_limits_report_actual_timeouts_and_provider_rate(self):
        config = self.config("myemailverifier", delay=3, requests_per_minute=100, timeout_seconds=600)
        result = streaming_services.limits("email_verification", config)
        self.assertEqual(result["requests_per_minute"], 20)
        self.assertEqual(result["timeout_seconds"], 30)
        dns = streaming_services.limits("dns_check", self.config("dns_ssl_checker", timeout_seconds=500))
        self.assertEqual(dns["timeout_seconds"], 60)
        self.assertIsNone(dns["requests_per_minute"])
        legacy = streaming_services.limits("enrichment", self.config("http_enrichment", timeout_seconds=1))
        self.assertEqual(legacy["timeout_seconds"], 15)
        self.assertIsNone(legacy["requests_per_minute"])

    def test_output_fingerprints_ignore_dns_check_timestamp(self):
        self.service.verify_domain.return_value = self.domain_result()
        one = self.run_step("dns_check", self.config("dns_ssl_checker"))
        two = self.run_step("dns_check", self.config("dns_ssl_checker"))
        self.assertEqual(one["result"]["output_fingerprint"], two["result"]["output_fingerprint"])
        self.service.verify_domain.return_value = self.domain_result(mapped_status="Domain Down")
        three = self.run_step("dns_check", self.config("dns_ssl_checker"))
        self.assertNotEqual(one["result"]["output_fingerprint"], three["result"]["output_fingerprint"])

    def test_retry_after_date_and_invalid_values(self):
        self.post.side_effect = None
        headers = {"Retry-After": format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30))}
        self.post.return_value = self.response({}, 429, headers)
        outcome = self.run_step(config=self.config("http_enrichment"))
        self.assertGreater(outcome["retry_after"], 28)
        self.assertLessEqual(outcome["retry_after"], 30)
        for value in ("nan", "inf", "bad header"):
            headers["Retry-After"] = value
            self.assertNotIn("retry_after", self.run_step(config=self.config("http_enrichment")))

    def test_bad_configuration_returns_contract_without_network(self):
        for step, config in (("unknown", self.config()), ("enrichment", {}),
                             ("dns_check", self.config("myemailverifier"))):
            with self.subTest(step=step):
                outcome = self.run_step(step, config)
                self.assertEqual(outcome["status"], "failed")
                self.assertEqual(outcome["updates"], {})
                self.assertIsInstance(outcome["error"], str)
        self.get.assert_not_called()
        self.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
