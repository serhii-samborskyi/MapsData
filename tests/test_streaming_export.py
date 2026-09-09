"""Adapter contract tests: real transforms, mocked HTTP, no database/provider I/O."""

import ast
import asyncio
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import Mock, patch

import streaming_export as export


def legacy_helpers():
    # Importing main initializes the database. Compile only its pure predicates
    # to verify compatibility without booting the app or replacing its logic.
    tree = ast.parse((Path(__file__).resolve().parents[1] / "main.py").read_text())
    names = {
        "_normalize_status_value",
        "_resolve_contact_email_status",
        "_is_valid_email_status",
        "_is_catch_all_email_status",
        "_matches_export_status_filter",
    }
    nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {"Any": Any}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), namespace)
    return SimpleNamespace(**namespace)


def response(status=200, body=None, headers=None):
    value = Mock(status_code=status, headers=headers or {})
    value.json.return_value = (
        body if body is not None else {"total": 1, "created": 1, "updated": 0}
    )
    return value


class StreamingExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = legacy_helpers()

    def setUp(self):
        self.config = {
            "template": {
                "id": 71,
                "name": "Frozen AB export",
                "service": "sendread_list",
                "api_config": {
                    "api_key": "test-secret",
                    "sendread_target_id": "template-list",
                },
                "field_mappings": export.SendReadIntegration(
                    ""
                ).get_default_field_mapping(),
            },
            "filters": {},
        }
        self.contacts = [
            {"id": 11, "email": "FIRST@example.org"},
            {"id": 12, "email": "second@example.org"},
        ]
        self.post = self.enterContext(
            patch.object(export.requests, "post", return_value=response())
        )
        default_response = self.post.return_value

        def default_post(_url, **kwargs):
            if self.post.return_value is not default_response:
                return self.post.return_value
            payload = kwargs["json"]
            rows = (
                payload
                if isinstance(payload, list)
                else payload.get("leads", payload.get("lead_list"))
            )
            return response(
                body={"total": len(rows), "created": len(rows), "updated": 0}
            )

        self.post.side_effect = default_post
        self.enterContext(
            patch.object(
                export.requests.sessions.Session,
                "request",
                side_effect=AssertionError("Unexpected network access"),
            )
        )

    def send(self, contacts=None):
        return export.send_batch(
            self.config, self.contacts if contacts is None else contacts
        )

    def test_engine_facade_frozen_mapping_destination_and_sparse_defaults(self):
        self.config["destination"] = {"list_id": "run-list"}
        self.config["field_mappings"] = {
            "email": "email",
            "firstName": "firstname",
            "custom1": "custom_1",
            "custom2": "source_data.segment",
            "custom3": "Literal text",
            "website": "domain",
            "city": "city",
        }
        contact = {
            "id": 1,
            "email": "Owner <LEAD@EXAMPLE.ORG>",
            "domain": "example.org",
            "address": "100 Main St, Madison, WI 53703",
            "source_data": '{"segment": "  owner  "}',
        }
        before = deepcopy((self.config, contact))
        receipt = self.send([contact])[0]
        self.assertEqual(receipt["status"], "exported")
        self.assertEqual(receipt["contact_id"], 1)
        args, kwargs = self.post.call_args
        self.assertTrue(args[0].endswith("/ab-test-lists/run-list/leads"))
        self.assertEqual(
            kwargs["json"],
            {
                "leads": [
                    {
                        "email": "lead@example.org",
                        "custom2": "owner",
                        "custom3": "Literal text",
                        "website": "https://example.org",
                        "city": "Madison",
                    }
                ]
            },
        )
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-secret")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual((self.config, contact), before)
        self.assertNotIn("test-secret", json.dumps(receipt))

    def test_default_mapping_omits_all_missing_enrichment_fields(self):
        self.send(self.contacts[:1])
        self.assertEqual(
            self.post.call_args.kwargs["json"],
            {"leads": [{"email": "first@example.org"}]},
        )

    def test_request_city_map_survives_json_freezing_and_preserves_existing_city(self):
        self.config["request_city_map"] = {"42": "Chicago"}
        self.send(
            [
                dict(self.contacts[0], request_id=42),
                dict(self.contacts[1], request_id=42, city="Austin"),
            ]
        )
        self.assertEqual(
            [lead["city"] for lead in self.post.call_args.kwargs["json"]["leads"]],
            ["Chicago", "Austin"],
        )

    def test_same_missing_null_and_blank_fields_as_database_rows(self):
        for value in (None, "", "  "):
            with self.subTest(value=value):
                self.send([dict(self.contacts[0], firstname=value, custom_1=value)])
                self.assertEqual(
                    self.post.call_args.kwargs["json"]["leads"][0],
                    {"email": "first@example.org"},
                )

    def test_filter_status_combinations_match_legacy_predicate(self):
        for email_status, fallback, flags in itertools.product(
            [
                None,
                "",
                "  ",
                "unverified",
                "Valid",
                "VALID SMTP",
                "Verified",
                "catch-all",
                "Catch All",
                "invalid",
            ],
            ["Valid", "Catch All", "invalid"],
            itertools.product([False, True], repeat=3),
        ):
            contact = dict(self.contacts[0], email_status=email_status, status=fallback)
            self.config["filters"] = dict(
                zip(
                    ("export_valid_only", "export_catch_all", "export_catch_all_only"),
                    flags,
                )
            )
            expected = self.main._matches_export_status_filter(contact, *flags)
            self.assertEqual(
                export.matches_export_status_filter(contact, *flags), expected
            )
            self.assertEqual(
                export.eligibility(contact, self.config, self.main), expected
            )

    def test_all_export_toggles_applied_before_transport(self):
        self.config["filters"] = {
            "exclude_public_emails": True,
            "export_domain_ok": True,
            "export_valid_only": True,
        }
        contacts = [
            {"id": 1},
            {"id": 2, "email": "   "},
            {
                "id": 3,
                "email": "user@GMAIL.COM",
                "domain_status": "Domain OK",
                "email_status": "valid",
            },
            {"id": 4, "email": "user@example.org", "email_status": "valid"},
            {
                "id": 5,
                "email": "user@example.org",
                "domain_status": "Domain OK",
                "email_status": "invalid",
            },
            {
                "id": 6,
                "email": "user@example.org",
                "domain_status": "Domain OK",
                "email_status": "valid",
            },
        ]
        receipts = self.send(contacts)
        self.assertEqual(
            [r["status"] for r in receipts], ["filtered"] * 5 + ["exported"]
        )
        self.assertEqual([r["contact_id"] for r in receipts], list(range(1, 7)))
        self.assertEqual(self.post.call_count, 1)

    def test_no_filters_does_not_require_enrichment_verification_or_domain(self):
        receipts = self.send()
        self.assertTrue(all(receipt["status"] == "exported" for receipt in receipts))
        self.assertEqual(self.post.call_count, 1)

    def test_per_contact_mapping_validation_does_not_send_invalid_row(self):
        receipts = self.send(
            [dict(self.contacts[0], email="invalid"), self.contacts[1]]
        )
        self.assertEqual([r["status"] for r in receipts], ["failed", "exported"])
        self.assertFalse(receipts[0]["attempted"])
        self.assertEqual(receipts[0]["error"]["code"], "validation")
        self.assertEqual(self.post.call_count, 1)

    def test_http_validation_rejects_batch_without_replaying_contacts(self):
        self.post.return_value = response(422, {"error": "Invalid email"})
        receipts = self.send()
        self.assertEqual([r["status"] for r in receipts], ["failed", "failed"])
        self.assertEqual(receipts[0]["provider_response"], {"error": "Invalid email"})
        self.assertEqual(receipts[0]["error"]["code"], "provider_validation")
        self.assertFalse(receipts[0]["retryable"])
        self.assertEqual(self.post.call_count, 1)

    def test_read_timeout_and_connection_error_are_unknown_without_retry(self):
        for exception in (
            export.requests.exceptions.ReadTimeout,
            export.requests.exceptions.Timeout,
            export.requests.exceptions.ConnectionError,
        ):
            with self.subTest(exception=exception):
                self.post.reset_mock()
                self.post.side_effect = exception("message containing test-secret")
                receipts = self.send()
                self.assertEqual(
                    [r["status"] for r in receipts], ["unknown", "unknown"]
                )
                self.assertFalse(receipts[0]["retryable"])
                self.assertTrue(receipts[0]["attempted"])
                self.assertFalse(receipts[1]["retryable"])
                self.assertTrue(receipts[1]["attempted"])
                self.assertEqual(receipts[1]["error"]["code"], "ambiguous_transport")
                self.assertEqual(self.post.call_count, 1)
                self.assertNotIn("test-secret", json.dumps(receipts))

    def test_connect_timeout_retryable_without_retrying_in_adapter(self):
        self.post.side_effect = export.requests.exceptions.ConnectTimeout()
        receipts = self.send()
        self.assertEqual(receipts[0]["status"], "failed")
        self.assertEqual(receipts[0]["error"]["code"], "connect_timeout")
        self.assertTrue(receipts[0]["retryable"])
        self.assertEqual(self.post.call_count, 1)

    def test_429_retry_after_seconds_applies_to_entire_batch(self):
        self.post.return_value = response(
            429, {"error": "Too many requests"}, {"Retry-After": "17"}
        )
        receipts = self.send()
        self.assertEqual(receipts[0]["error"]["code"], "rate_limited")
        self.assertTrue(receipts[0]["retryable"])
        self.assertEqual(
            [r["error"]["retry_after_seconds"] for r in receipts], [17, 17]
        )
        self.assertEqual(self.post.call_count, 1)

    def test_retry_after_http_date_invalid_and_past_values(self):
        now = datetime(2026, 9, 9, tzinfo=timezone.utc)
        with patch.object(export, "datetime") as clock:
            clock.now.return_value = now
            self.assertEqual(
                export._retry_after(
                    {"Retry-After": format_datetime(now + timedelta(seconds=45))}
                ),
                45,
            )
            self.assertEqual(
                export._retry_after(
                    {"Retry-After": format_datetime(now - timedelta(seconds=45))}
                ),
                0,
            )
        self.assertIsNone(export._retry_after({"Retry-After": "invalid"}))
        self.assertIsNone(export._retry_after({}))

    def test_server_error_and_request_timeout_are_ambiguous_after_possible_acceptance(
        self,
    ):
        for status in (408, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.post.reset_mock()
                self.post.return_value = response(status, {"error": "failed"})
                receipt = self.send()[0]
                self.assertEqual(receipt["status"], "unknown")
                self.assertFalse(receipt["retryable"])
                self.assertEqual(receipt["error"]["http_status"], status)
                self.assertEqual(self.post.call_count, 1)

    def test_auth_destination_and_redirect_errors_are_not_retried(self):
        for status in (301, 307, 401, 403, 404, 409):
            with self.subTest(status=status):
                self.post.reset_mock()
                self.post.return_value = response(status, {"error": "Rejected"})
                receipt = self.send()[0]
                self.assertEqual(receipt["status"], "failed")
                self.assertFalse(receipt["retryable"])
                self.assertEqual(self.post.call_count, 1)

    def test_200_provider_error_is_not_success(self):
        self.post.return_value = response(
            body={
                "success": False,
                "errors": [{"email": "first@example.org", "message": "invalid"}],
            }
        )
        receipt = self.send(self.contacts[:1])[0]
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["error"]["code"], "provider_validation")

    def test_aggregate_sendread_counts_confirm_acceptance_and_skips(self):
        for body, status, code in (
            ({"total": 1, "created": 1}, "exported", None),
            ({"total": 1, "updated": 1, "pending": 1}, "exported", None),
            ({"total": 1, "skippedBlocked": 1}, "filtered", "provider_blocked"),
            (
                {"total": 1, "assigned": 1, "skippedConflicts": 1},
                "filtered",
                "provider_conflict",
            ),
            ({"total": 1, "assigned": 1}, "unknown", "unconfirmed_response"),
            ({"total": 2, "created": 2}, "unknown", "unconfirmed_response"),
            (
                {"total": 1, "created": 1, "skippedBlocked": 1},
                "unknown",
                "unconfirmed_response",
            ),
            ({"ok": True}, "unknown", "unconfirmed_response"),
        ):
            with self.subTest(body=body):
                self.post.return_value = response(body=body)
                receipt = self.send(self.contacts[:1])[0]
                self.assertEqual(receipt["status"], status)
                self.assertEqual(
                    receipt["error"]["code"] if receipt["error"] else None, code
                )

    def test_unparseable_success_is_unknown_not_retried(self):
        self.post.side_effect = None
        self.post.return_value.json.side_effect = ValueError("not JSON")
        receipts = self.send()
        self.assertEqual(receipts[0]["status"], "unknown")
        self.assertEqual(self.post.call_count, 1)

    def test_sendread_campaign_override_quotes_ids_and_passes_duplicate_setting(self):
        self.config["destination"] = {
            "target_type": "campaign",
            "target_id": "other/id?x=y",
            "skipExistingFromOtherCampaigns": True,
        }
        self.send(self.contacts[:1])
        self.assertTrue(
            self.post.call_args.args[0].endswith("/campaigns/other%2Fid%3Fx%3Dy/leads")
        )
        self.assertTrue(
            self.post.call_args.kwargs["json"]["skipExistingFromOtherCampaigns"]
        )
        self.assertNotIn("Idempotency-Key", self.post.call_args.kwargs["headers"])

    def test_invalid_config_returns_structured_unattempted_errors(self):
        for update in (
            {"destination": {"list_id": ""}},
            {"destination": {"target_type": "unknown"}},
            {"destination": {"service": "smartlead"}},
            {"filters": {"export_valid_only": "false"}},
            {"field_mappings": None},
            {"field_mappings": {"email": {"field": "email"}}},
        ):
            with self.subTest(update=update):
                config = {**self.config, **update}
                result = export.export_batch(config, self.contacts)
                self.assertEqual(result["error"]["code"], "configuration")
                self.assertTrue(all(not r["attempted"] for r in result["receipts"]))
        self.post.assert_not_called()

    def test_empty_batch_no_template_required_and_explicit_order_preserved(self):
        self.assertEqual(export.export_batch(None, [], None)["receipts"], [])
        receipts = self.send(list(reversed(self.contacts)))
        self.assertEqual([r["contact_id"] for r in receipts], [12, 11])
        self.assertEqual([r["input_index"] for r in receipts], [0, 1])

    def test_destination_key_ignores_credentials_filters_mapping_and_template_id(self):
        original_key = export.destination_key(self.config)
        self.config["template"]["id"] = 999
        self.config["template"]["api_config"]["api_key"] = "rotated-secret"
        self.config["field_mappings"] = {
            "email": "email",
            "custom1": "a different value",
        }
        self.config["filters"] = {"export_valid_only": True}
        self.assertEqual(export.destination_key(self.config), original_key)
        self.assertNotIn("secret", original_key)
        self.config["destination"] = {"list_id": "another-list"}
        self.assertNotEqual(export.destination_key(self.config), original_key)

    def test_destination_key_includes_provider_host_and_normalizes_base_url(self):
        self.config["template"]["api_config"]["base_url"] = "app.sendread.co/"
        default = export.destination_key(self.config)
        self.assertEqual(default, export.destination_key(self.config["template"]))
        self.config["template"]["api_config"]["api_base_url"] = (
            "https://another.example.org"
        )
        self.assertNotEqual(default, export.destination_key(self.config))

    def test_limits_match_existing_integrations_and_bulk_cap(self):
        for service, rpm in (
            ("sendread_list", 120),
            ("sendread_campaign", 120),
            ("smartlead", 300),
            ("manyreach", 60),
        ):
            self.config["template"]["service"] = service
            limits = export.limits(self.config)
            self.assertEqual(limits["requests_per_minute"], rpm)
            self.assertEqual(limits["min_interval_seconds"], 60 / rpm)
            self.assertEqual(
                limits["max_contacts_per_call"], 400 if service == "smartlead" else 500
            )
            self.assertEqual(limits["timeout_seconds"], 30)
            self.assertTrue(limits["endpoint_key"])
        self.post.assert_not_called()

    def test_smartlead_reuses_transform_and_settings_sanitizer(self):
        self.config["template"].update(
            service="smartlead",
            field_mappings={
                "email": "email",
                "website": "domain",
                "custom_1": "source_data.label",
            },
        )
        self.config["template"]["api_config"].update(
            smartlead_campaign_id="saved",
            settings={
                "ignore_duplicate_contacts_within_campaign": True,
                "ignore_global_block_list": False,
            },
        )
        self.config["destination"] = {"target_id": "run-campaign"}
        self.post.return_value = response(body={"ok": True})
        receipt = self.send(
            [
                dict(
                    self.contacts[0],
                    domain="https://www.example.org/page",
                    source_data={"label": "x"},
                )
            ]
        )[0]
        self.assertEqual(receipt["status"], "exported")
        self.assertTrue(
            self.post.call_args.args[0].endswith("/campaigns/run-campaign/leads")
        )
        self.assertEqual(
            self.post.call_args.kwargs["json"],
            {
                "lead_list": [
                    {
                        "email": "FIRST@example.org",
                        "website": "example.org",
                        "custom_fields": {"custom_1": "x"},
                    }
                ],
                "settings": {"ignore_global_block_list": False},
            },
        )

    def test_manyreach_transform_and_per_run_campaign_list_name(self):
        self.config["template"].update(
            service="manyreach",
            field_mappings={"email": "email", "www": "domain", "custom_1": "custom_1"},
        )
        self.config["template"]["api_config"]["manyreach_campaign_id"] = "saved"
        self.config["destination"] = {
            "manyreach_campaign_id": "run",
            "newListName": "List & more",
        }
        self.post.return_value = response(body={"ok": True})
        receipt = self.send(
            [dict(self.contacts[0], domain="https://www.example.org/page")]
        )[0]
        self.assertEqual(receipt["status"], "exported")
        self.assertEqual(
            self.post.call_args.kwargs["json"],
            [{"email": "FIRST@example.org", "www": "example.org", "campaignid": "run"}],
        )
        self.assertEqual(
            self.post.call_args.kwargs["params"]["newListName"], "List & more"
        )

    def test_provider_echoed_api_key_is_redacted(self):
        self.post.return_value = response(
            400, {"error": "Bad test-secret", "nested": ["test-secret"]}
        )
        receipts = self.send(self.contacts[:1])
        self.assertNotIn("test-secret", json.dumps(receipts))
        self.assertIn("[redacted]", json.dumps(receipts))

    def test_fifty_contacts_one_post_with_engine_snapshot_and_ab_override(self):
        self.config["template_snapshot"] = self.config.pop("template")
        self.config["sendread_ab_list_id"] = "run-ab"
        contacts = [
            {"id": index, "email": f"lead{index}@example.org"} for index in range(50)
        ]
        receipts = self.send(contacts)
        self.assertEqual(len(receipts), 50)
        self.assertTrue(
            all(r["status"] == "exported" and r["attempted"] for r in receipts)
        )
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(len(self.post.call_args.kwargs["json"]["leads"]), 50)
        self.assertTrue(
            self.post.call_args.args[0].endswith("/ab-test-lists/run-ab/leads")
        )
        self.assertIn("run-ab", export.destination_key(self.config))

    def test_partial_counts_unknown_for_every_unattributable_contact(self):
        self.post.return_value = response(
            body={"total": 2, "created": 1, "skippedBlocked": 1}
        )
        receipts = self.send()
        self.assertEqual([r["status"] for r in receipts], ["unknown", "unknown"])
        self.assertTrue(all(not r["retryable"] for r in receipts))
        self.assertEqual(self.post.call_count, 1)

    def test_per_item_results_correlated_by_email_not_response_order(self):
        self.post.return_value = response(
            body={
                "results": [
                    {
                        "email": "second@example.org",
                        "status": "invalid",
                        "error": "Rejected email",
                    },
                    {
                        "email": "first@example.org",
                        "status": "created",
                        "id": "provider-id",
                    },
                ]
            }
        )
        receipts = self.send()
        self.assertEqual([r["status"] for r in receipts], ["exported", "failed"])
        self.assertEqual(receipts[0]["provider_response"]["id"], "provider-id")
        self.assertEqual(self.post.call_count, 1)

    def test_incomplete_per_item_results_leave_unmatched_contact_unknown(self):
        self.post.return_value = response(
            body={"results": [{"email": "second@example.org", "status": "updated"}]}
        )
        self.assertEqual([r["status"] for r in self.send()], ["unknown", "exported"])

    def test_oversize_batch_rejected_before_any_send(self):
        receipts = self.send([self.contacts[0]] * 501)
        self.assertTrue(
            all(
                not r["attempted"] and r["error"]["code"] == "configuration"
                for r in receipts
            )
        )
        self.post.assert_not_called()

    def test_global_rate_bucket_does_not_change_with_destination(self):
        first = export.limits(self.config)["endpoint_key"]
        self.config["sendread_ab_list_id"] = "other-list"
        self.assertEqual(export.limits(self.config)["endpoint_key"], first)


class LegacyExportSnapshotTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / "main.py").read_text())
        nodes = [
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name
            in (
                "export_campaign",
                "_automation_step_export",
                "_matches_export_status_filter",
                "_resolve_contact_email_status",
                "_normalize_status_value",
                "_is_valid_email_status",
                "_is_catch_all_email_status",
            )
        ]
        for node in nodes:
            node.decorator_list = []

        class JsonRequest:
            def __init__(self, payload):
                self.payload = payload

            async def json(self):
                return self.payload

        class HTTPException(Exception):
            def __init__(self, status_code, detail):
                self.status_code, self.detail = status_code, detail

        self.request_class = JsonRequest
        self.frozen = {
            "id": 71,
            "service": "sendread_list",
            "field_mappings": {"email": "email"},
            "api_config": {"api_key": "frozen-key", "sendread_target_id": "old-list"},
        }
        cursor = Mock()
        cursor.fetchone.return_value = {"recent_exports": 0}
        cursor.fetchall.return_value = [{"id": 11, "email": "first@example.org"}]
        connection = Mock()
        connection.cursor.return_value = cursor
        self.lookup = Mock(return_value=deepcopy(self.frozen))
        self.namespace = {
            "Request": JsonRequest,
            "JsonRequest": JsonRequest,
            "HTTPException": HTTPException,
            "datetime": datetime,
            "timedelta": timedelta,
            "Optional": __import__("typing").Optional,
            "Any": __import__("typing").Any,
            "TemplateManager": SimpleNamespace(get_template=self.lookup),
            "SendReadIntegration": export.SendReadIntegration,
            "asyncio": asyncio,
            "get_db": lambda: nullcontext(connection),
            "_safe_int": lambda value, default: (
                int(value) if value is not None else default
            ),
            "_export_api_base_url": lambda config: config.get("base_url", ""),
            "_build_campaign_request_city_map": lambda *args: {},
            "_apply_city_fallback_for_export": lambda *args: None,
        }
        exec(
            compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"),
            self.namespace,
        )
        self.send = self.enterContext(
            patch.object(
                export.SendReadIntegration,
                "export_to_ab_test_list",
                return_value={"total": 1, "created": 1},
            )
        )

    def test_internal_snapshot_and_per_run_destination_used_without_template_lookup(
        self,
    ):
        request = self.request_class(
            {"template_id": 71, "sendread_ab_list_id": "run-list"}
        )
        request._template_snapshot = deepcopy(self.frozen)
        result = asyncio.run(self.namespace["export_campaign"](9, request))
        self.assertEqual(result["contacts_exported"], 1)
        self.lookup.assert_not_called()
        self.assertEqual(
            self.send.call_args.args, ("run-list", [{"email": "first@example.org"}])
        )
        self.assertEqual(request._template_snapshot, self.frozen)

    def test_raw_json_snapshot_ignored_even_when_request_is_internal(self):
        request = self.request_class(
            {
                "template_id": 71,
                "_template_snapshot": {
                    "id": 71,
                    "api_config": {"api_key": "untrusted"},
                },
                "template_snapshot": {"id": 71, "api_config": {"api_key": "untrusted"}},
            }
        )
        asyncio.run(self.namespace["export_campaign"](9, request))
        self.lookup.assert_called_once_with(71)
        self.assertEqual(self.send.call_args.args[0], "old-list")

    def test_external_request_attribute_cannot_supply_template_credentials(self):
        class ExternalRequest:
            _template_snapshot = {"id": 71, "api_config": {"api_key": "untrusted"}}

            async def json(self):
                return {"template_id": 71}

        asyncio.run(self.namespace["export_campaign"](9, ExternalRequest()))
        self.lookup.assert_called_once_with(71)

    def test_automation_passes_snapshot_as_attribute_on_each_batch(self):
        captured = []

        async def fake_export(_campaign, request):
            captured.append(request)
            if len(captured) > 1:
                raise self.namespace["HTTPException"](404, "No more contacts")
            return {"contacts_exported": 1}

        self.namespace["export_campaign"] = fake_export
        config = {
            "template_id": 71,
            "template_snapshot": self.frozen,
            "sendread_ab_list_id": "run-list",
            "batch_size": 50,
        }
        result = self.namespace["_automation_step_export"](9, config)
        self.assertTrue(result[0])
        self.assertEqual(len(captured), 2)
        for request in captured:
            self.assertEqual(request._template_snapshot, self.frozen)
            self.assertIsNot(request._template_snapshot, self.frozen)
            self.assertNotIn("_template_snapshot", request.payload)
            self.assertNotIn("template_snapshot", request.payload)
            self.assertEqual(request.payload["sendread_ab_list_id"], "run-list")


if __name__ == "__main__":
    unittest.main()
