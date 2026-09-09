import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import UUID, uuid4

import psycopg2
import pytest

from remote_mcp import CampaignInput, CampaignService
from remote_mcp.contracts import DraftError


def counts(get_db):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*) AS n FROM test_campaigns")
            campaigns = cursor.fetchone()["n"]
            cursor.execute(
                "SELECT count(*) AS n FROM mcp_campaign_drafts WHERE launch_result IS NOT NULL"
            )
            return campaigns, cursor.fetchone()["n"]


def test_preview_persists_and_does_not_launch(integration, payload, get_db):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    assert counts(get_db) == (0, 0)
    assert hooks.launch_calls == hooks.wake_calls == 0
    assert preview["submitted_requests"] == payload.requests
    assert preview["campaign"]["requests"] == ["niche service Austin TX"]
    assert preview["request_count"] == 1
    assert {item["kind"] for item in preview["templates"]} == {
        "source",
        "funnel",
        "export",
    }
    assert all(len(item["sha256"]) == 64 for item in preview["templates"])
    assert "SECRET" not in json.dumps(preview)
    assert "provider.invalid" not in json.dumps(preview)
    restarted = CampaignService(get_db, hooks.hooks)
    assert restarted.get_prepared_campaign(UUID(preview["preview_id"])) == preview


def test_preview_hook_cannot_write(integration, payload, get_db):
    service, hooks = integration

    def forbidden(cursor, payload):
        cursor.execute("INSERT INTO test_campaigns (plan) VALUES ('{}')")

    service.hooks = replace(hooks.hooks, prepare=forbidden)
    with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
        service.prepare_campaign(payload)
    assert counts(get_db) == (0, 0)


@pytest.mark.parametrize(
    "confirmed,hash_value,error",
    [
        (False, None, "confirmation"),
        ("true", None, "confirmation"),
        (1, None, "confirmation"),
        (True, "0" * 64, "hash"),
    ],
)
def test_launch_requires_exact_preview_confirmation(
    integration, payload, get_db, confirmed, hash_value, error
):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    with pytest.raises(DraftError, match=error):
        service.launch_prepared_campaign(
            UUID(preview["preview_id"]),
            hash_value or preview["preview_hash"],
            confirmed,
        )
    assert counts(get_db) == (0, 0)
    assert hooks.launch_calls == 0


def test_launch_is_atomic_idempotent_and_wakes_after_commit(
    integration, payload, get_db
):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    args = (UUID(preview["preview_id"]), preview["preview_hash"], True)
    first = service.launch_prepared_campaign(*args)
    retry = CampaignService(get_db, hooks.hooks).launch_prepared_campaign(*args)
    assert first["campaign_id"] == retry["campaign_id"]
    assert not first["idempotent"] and retry["idempotent"]
    assert counts(get_db) == (1, 1)
    assert hooks.launch_calls == 1 and hooks.wake_calls == 2
    assert hooks.seen_plan["source_snapshot"]["config"]["api_key"] == "SECRET_KEY"
    assert hooks.seen_plan["requests"] == preview["campaign"]["requests"]
    assert "SECRET" not in json.dumps(retry)


def test_concurrent_launch_has_one_campaign(integration, payload, get_db):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    args = (UUID(preview["preview_id"]), preview["preview_hash"], True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(service.launch_prepared_campaign, *args) for _ in range(4)
        ]
        results = [future.result(timeout=10) for future in futures]
    assert len({result["campaign_id"] for result in results}) == 1
    assert sum(not result["idempotent"] for result in results) == 1
    assert hooks.launch_calls == 1
    assert counts(get_db) == (1, 1)


def test_failed_insert_rolls_back_campaign_and_draft(integration, payload, get_db):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    args = (UUID(preview["preview_id"]), preview["preview_hash"], True)
    hooks.fail_launch = True
    with pytest.raises(RuntimeError):
        service.launch_prepared_campaign(*args)
    assert counts(get_db) == (0, 0)
    assert hooks.wake_calls == 0
    hooks.fail_launch = False
    assert service.launch_prepared_campaign(*args)["idempotent"] is False
    assert counts(get_db) == (1, 1)


def test_failed_wakeup_can_be_retried_without_relaunch(integration, payload):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    args = (UUID(preview["preview_id"]), preview["preview_hash"], True)
    hooks.fail_wake = True
    assert service.launch_prepared_campaign(*args)["worker_wake_pending"] is True
    hooks.fail_wake = False
    result = service.launch_prepared_campaign(*args)
    assert result["worker_wake_pending"] is False
    assert result["idempotent"] is True
    assert hooks.launch_calls == 1


def test_expired_draft_rejected_but_launched_retry_is_allowed(
    integration, payload, get_db
):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    args = (UUID(preview["preview_id"]), preview["preview_hash"], True)
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE mcp_campaign_drafts SET expires_at = now() - interval '1 second'"
            )
        conn.commit()
    with pytest.raises(DraftError, match="expired"):
        service.launch_prepared_campaign(*args)
    assert hooks.launch_calls == 0
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE mcp_campaign_drafts SET expires_at = now() + interval '1 hour'"
            )
        conn.commit()
    service.launch_prepared_campaign(*args)
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE mcp_campaign_drafts SET expires_at = now() - interval '1 second'"
            )
        conn.commit()
    assert service.launch_prepared_campaign(*args)["idempotent"] is True


def test_tampered_plan_cannot_be_previewed_or_launched(integration, payload, get_db):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE mcp_campaign_drafts SET frozen_plan = jsonb_set(frozen_plan, '{name}', '\"Changed\"')"
            )
        conn.commit()
    with pytest.raises(DraftError, match="integrity"):
        service.get_prepared_campaign(UUID(preview["preview_id"]))
    with pytest.raises(DraftError, match="integrity"):
        service.launch_prepared_campaign(
            UUID(preview["preview_id"]), preview["preview_hash"], True
        )
    assert hooks.launch_calls == 0


def test_template_changes_produce_new_hash(integration, payload):
    service, hooks = integration
    first = service.prepare_campaign(payload)

    def updated(cursor, submitted):
        plan = hooks.prepare(cursor, submitted)
        plan["source_snapshot"]["config"]["api_key"] = "SECRET_CHANGED"
        return plan

    service.hooks = replace(hooks.hooks, prepare=updated)
    second = service.prepare_campaign(payload)
    assert first["preview_hash"] != second["preview_hash"]
    assert first["templates"][0]["sha256"] != second["templates"][0]["sha256"]
    service.launch_prepared_campaign(
        UUID(first["preview_id"]), first["preview_hash"], True
    )
    assert hooks.seen_plan["source_snapshot"]["config"]["api_key"] == "SECRET_KEY"


def test_template_status_and_stop_responses_redact_private_data(integration, payload):
    service, hooks = integration
    preview = service.prepare_campaign(payload)
    result = service.launch_prepared_campaign(
        UUID(preview["preview_id"]), preview["preview_hash"], True
    )
    assert "SECRET" not in json.dumps(service.list_templates("source"))
    assert service.get_campaign_status(result["campaign_id"])["status"] == "running"
    assert "SECRET" not in json.dumps(service.get_run_status(result["run_id"]))
    assert service.stop_campaign(result["campaign_id"])["stop_requested"] is True
    assert service.get_campaign_status(result["campaign_id"])["status"] == "stopped"


def test_missing_preview(integration):
    service, hooks = integration
    with pytest.raises(DraftError, match="not found"):
        service.get_prepared_campaign(uuid4())


@pytest.mark.parametrize("requests", [[], [""], ["   "], ["a\nb"], ["a\x00b"], [123]])
def test_invalid_requests(requests):
    with pytest.raises(ValueError):
        CampaignInput(name="Test", requests=requests, funnel_template_id=2)
