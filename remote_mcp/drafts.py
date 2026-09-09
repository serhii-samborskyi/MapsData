"""Persistent previews and atomic launches without importing the app or its workers."""

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from psycopg2.extras import Json, RealDictCursor

from .contracts import (
    CampaignInput,
    DraftError,
    LaunchResult,
    ServiceHooks,
    TemplateSnapshot,
)
from .public import export_preview, stream_progress, template_metadata


def init_schema(cursor) -> None:
    """Called by the host's DB initializer; the caller owns the commit."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mcp_campaign_drafts (
            id UUID PRIMARY KEY,
            payload JSONB NOT NULL,
            frozen_plan JSONB NOT NULL,
            preview_hash TEXT NOT NULL,
            template_hashes JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            launched_at TIMESTAMPTZ,
            launch_result JSONB,
            CHECK ((launched_at IS NULL) = (launch_result IS NULL))
        )
    """)


def _json_copy(value):
    return json.loads(json.dumps(value, allow_nan=False, ensure_ascii=True))


def _hash(value) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _template_hashes(plan: dict) -> list[dict]:
    source = plan["source_snapshot"]
    snapshots = [
        {
            "kind": "source",
            "id": plan["source_template_id"],
            "name": source["name"],
            "configuration": source,
        },
        {
            "kind": "funnel",
            "id": plan["funnel_template_id"],
            "name": plan["funnel_name"],
            "configuration": {
                "steps": plan["steps"],
                "execution_mode": plan["execution_mode"],
                "default_retry_count": plan["default_retry_count"],
            },
        },
    ]
    kinds = {
        "enrichment": "enrichment",
        "dns_check": "verification",
        "email_verification": "verification",
        "export": "export",
    }
    for step in plan["steps"]:
        if not step.get("enabled", True) or step["type"] == "pipeline":
            continue
        record = step["config"]["template_snapshot"]
        snapshots.append(
            {
                "kind": kinds[step["type"]],
                "id": record["id"],
                "name": record["name"],
                "configuration": record,
            }
        )
    result = []
    seen = set()
    for raw in snapshots:
        snapshot = TemplateSnapshot.model_validate(raw)
        key = (snapshot.kind, snapshot.id)
        if key in seen:
            continue
        seen.add(key)
        result.append(
            {
                "kind": snapshot.kind,
                "id": snapshot.id,
                "name": snapshot.name,
                "sha256": _hash(raw),
                "configuration_redacted": True,
            }
        )
    return result


def _public_launch(result: dict) -> dict:
    parsed = LaunchResult.model_validate(result)
    public = {"campaign_id": parsed.campaign_id, "run_id": parsed.run_id}
    if result.get("execution_mode") in ("batch", "streaming"):
        public["execution_mode"] = result["execution_mode"]
    return public


_STATES = frozenset(
    {
        "active",
        "pending",
        "queued",
        "running",
        "paused",
        "completed",
        "failed",
        "canceled",
        "cancelled",
        "stopping",
        "stopped",
        "waiting_confirmation",
        "idle",
        "completed_with_errors",
        "not_started",
        "inactive",
    }
)
_COUNTERS = (
    "total_requests",
    "pending_requests",
    "completed_requests",
    "failed_requests",
    "total_contacts",
    "processed_contacts",
    "enriched_contacts",
    "failed_contacts",
    "skipped_contacts",
    "total_steps",
    "completed_steps",
    "exported_contacts",
)


def _public_status(raw: dict) -> dict:
    # Status endpoints may return full provider responses, contacts and logs.
    # Project metadata rather than trying to recognize every credential spelling.
    result = {
        "status": raw.get("status") if raw.get("status") in _STATES else "unknown"
    }
    for key in ("campaign_id", "run_id", *_COUNTERS):
        value = raw.get(key)
        if type(value) is int and value >= 0:
            result[key] = value
    for key in ("stop_requested", "source_closed"):
        if type(raw.get(key)) is bool:
            result[key] = raw[key]
    if raw.get("execution_mode") in ("batch", "streaming"):
        result["execution_mode"] = raw["execution_mode"]
    if "stream_progress" in raw:
        result["stream_progress"] = stream_progress(raw["stream_progress"])
    return result


class CampaignService:
    def __init__(self, get_db, hooks: ServiceHooks, *, draft_ttl_seconds: int = 86400):
        if not 1 <= draft_ttl_seconds <= 30 * 86400:
            raise ValueError("Draft TTL must be between 1 second and 30 days")
        self.get_db = get_db
        self.hooks = hooks
        self.draft_ttl_seconds = draft_ttl_seconds

    def list_templates(self, kind) -> dict:
        with self.get_db() as conn:
            conn.set_session(readonly=True)
            with conn.cursor() as cursor:
                rows = self.hooks.list_templates(cursor, kind)
        return {"templates": [template_metadata(row, kind) for row in rows]}

    def prepare_campaign(self, payload: CampaignInput) -> dict:
        submitted = payload.model_dump(mode="json")
        with self.get_db() as conn:
            # A read-only repeatable snapshot prevents preview hooks from inserting
            # campaigns and gives related templates a consistent database view.
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            with conn.cursor() as cursor:
                plan = _json_copy(self.hooks.prepare(cursor, _json_copy(submitted)))
            CampaignInput.model_validate(
                {
                    **submitted,
                    "name": plan["name"],
                    "requests": plan["requests"],
                    "execution_mode": plan["execution_mode"],
                }
            )
            if plan["requests"] != list(
                dict.fromkeys(text.strip() for text in submitted["requests"])
            ):
                raise ValueError(
                    "Frozen requests may only trim and deduplicate submitted requests"
                )
            if (
                submitted["execution_mode"]
                and submitted["execution_mode"] != plan["execution_mode"]
            ):
                raise ValueError(
                    "Frozen execution mode differs from the submitted mode"
                )
            template_hashes = _template_hashes(plan)
            selected = {
                (key.removesuffix("_template_id"), value)
                for key, value in submitted.items()
                if key.endswith("_template_id") and value is not None
            }
            captured = {(item["kind"], item["id"]) for item in template_hashes}
            if not selected <= captured:
                raise ValueError("Frozen plan is missing selected template snapshots")
        preview_hash = _hash({"payload": submitted, "plan": plan})
        now = datetime.now(timezone.utc)
        preview_id = str(uuid4())
        with self.get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    INSERT INTO mcp_campaign_drafts
                        (id, payload, frozen_plan, preview_hash, template_hashes,
                         created_at, expires_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *
                """,
                    (
                        preview_id,
                        Json(submitted),
                        Json(plan),
                        preview_hash,
                        Json(template_hashes),
                        now,
                        now + timedelta(seconds=self.draft_ttl_seconds),
                    ),
                )
                row = cursor.fetchone()
                preview = self._preview(row)
            conn.commit()
        return preview

    def get_prepared_campaign(self, preview_id: UUID) -> dict:
        with (
            self.get_db() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cursor,
        ):
            cursor.execute(
                "SELECT * FROM mcp_campaign_drafts WHERE id = %s",
                (str(preview_id),),
            )
            row = cursor.fetchone()
        if row is None:
            raise DraftError("Preview not found")
        return self._preview(row)

    @staticmethod
    def _preview(row) -> dict:
        if (
            _hash({"payload": row["payload"], "plan": row["frozen_plan"]})
            != row["preview_hash"]
            or _template_hashes(row["frozen_plan"]) != row["template_hashes"]
        ):
            raise DraftError("Stored preview integrity check failed")
        return {
            "preview_id": str(row["id"]),
            "preview_hash": row["preview_hash"],
            "created_at": row["created_at"].isoformat(),
            "expires_at": row["expires_at"].isoformat(),
            "status": "launched"
            if row["launch_result"] is not None
            else (
                "expired"
                if row["expires_at"] <= datetime.now(timezone.utc)
                else "prepared"
            ),
            "campaign": {
                **row["payload"],
                "name": row["frozen_plan"]["name"],
                "requests": row["frozen_plan"]["requests"],
                "execution_mode": row["frozen_plan"]["execution_mode"],
            },
            "submitted_requests": row["payload"]["requests"],
            "request_count": len(row["frozen_plan"]["requests"]),
            "templates": row["template_hashes"],
            "steps": [
                {"type": step["type"], "enabled": step.get("enabled", True)}
                for step in row["frozen_plan"]["steps"]
            ],
            "export": export_preview(row["frozen_plan"]),
            "launch": _public_launch(row["launch_result"])
            if row["launch_result"]
            else None,
            "coverage": (
                "Only the literal submitted requests are planned; "
                "results and ads are not exhaustive."
            ),
        }

    def launch_prepared_campaign(
        self, preview_id: UUID, preview_hash: str, confirmed: bool
    ) -> dict:
        if confirmed is not True:
            raise DraftError("Explicit human confirmation is required")
        with self.get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM mcp_campaign_drafts WHERE id = %s FOR UPDATE",
                    (str(preview_id),),
                )
                row = cursor.fetchone()
                if row is None:
                    raise DraftError("Preview not found")
                expected = _hash(
                    {"payload": row["payload"], "plan": row["frozen_plan"]}
                )
                if not hmac.compare_digest(row["preview_hash"], preview_hash):
                    raise DraftError(
                        "Preview hash does not match; review the saved preview"
                    )
                if not hmac.compare_digest(expected, row["preview_hash"]):
                    raise DraftError("Stored preview integrity check failed")
                if _template_hashes(row["frozen_plan"]) != row["template_hashes"]:
                    raise DraftError("Stored preview integrity check failed")
                reused = row["launch_result"] is not None
                if reused:
                    result = row["launch_result"]
                else:
                    if row["expires_at"] <= datetime.now(timezone.utc):
                        raise DraftError(
                            "Preview expired; prepare and confirm a new preview"
                        )
                    result = _json_copy(self.hooks.launch(cursor, row["frozen_plan"]))
                    LaunchResult.model_validate(result)
                    cursor.execute(
                        """
                        UPDATE mcp_campaign_drafts
                        SET launch_result = %s, launched_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """,
                        (Json(result), str(preview_id)),
                    )
            # Campaign, requests, run and launch result commit together. A retry
            # after a lost response observes the saved result under the row lock.
            conn.commit()
        wake_pending = False
        try:
            self.hooks.after_commit(_json_copy(result))
        except Exception:
            # The campaign is already durable. A retry must wake it, not relaunch.
            wake_pending = True
        return {
            **_public_launch(result),
            "preview_id": str(preview_id),
            "idempotent": reused,
            "worker_wake_pending": wake_pending,
        }

    def get_campaign_status(self, campaign_id: int) -> dict:
        return self._read_status(self.hooks.get_campaign_status, campaign_id)

    def get_run_status(self, run_id: int) -> dict:
        return self._read_status(self.hooks.get_run_status, run_id)

    def _read_status(self, hook, identifier: int) -> dict:
        with self.get_db() as conn:
            conn.set_session(readonly=True)
            with conn.cursor() as cursor:
                return _public_status(hook(cursor, identifier))

    def stop_campaign(self, campaign_id: int) -> dict:
        with self.get_db() as conn:
            with conn.cursor() as cursor:
                result = self.hooks.stop_campaign(cursor, campaign_id)
                public = _public_status(result)
            conn.commit()
        return public
