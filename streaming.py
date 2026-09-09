"""Durable per-contact funnel work. Legacy campaign workers do not use these queues."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

from psycopg2 import sql
from psycopg2.extras import Json

ACTIVE = ("pending", "running", "retry", "blocked", "uncertain")
SUCCESS = ("completed", "skipped")
TERMINAL_RUNS = ("cancelled", "completed", "failed")
CONTACT_FIELDS = {
    "address",
    "business_name",
    "category",
    "domain",
    "email",
    "facebook",
    "instagram",
    "phone",
    "place_id",
    "rating",
    "review_count",
    "twitter",
    "yelp",
    "full_name",
    "industry",
    "city",
    "www",
    "firstname",
    "lastname",
    "company",
    "country",
    "company_social",
    "company_size",
    "personal_job_position",
    "personal_prospect_location",
    "personal_user_social",
    "screenshot",
    "logo",
    "state",
    "icebreaker",
    "time_zone_offset_min",
    "notes",
    "tags_import",
    "email_status",
    "domain_status",
    "domain_dns_status",
    "domain_http_status",
    "domain_https_status",
    "domain_ssl_status",
    "domain_error",
    "domain_last_checked_at",
} | {f"custom_{n}" for n in range(1, 21)}


def init_schema(cursor):
    cursor.execute("""
        ALTER TABLE automation_funnel_templates ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'batch';
        ALTER TABLE automation_runs ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'batch';
        ALTER TABLE automation_runs ADD COLUMN IF NOT EXISTS stream_source_closed BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE automation_run_steps ADD COLUMN IF NOT EXISTS stream_bucket TEXT;
        ALTER TABLE automation_run_steps ADD COLUMN IF NOT EXISTS stream_interval DOUBLE PRECISION NOT NULL DEFAULT 0;
        ALTER TABLE automation_run_steps ADD COLUMN IF NOT EXISTS stream_concurrency INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS source_snapshot JSONB;
        ALTER TABLE enrichment_api_rate_limits ADD COLUMN IF NOT EXISTS last_dispatch_at TIMESTAMPTZ;
        CREATE TABLE IF NOT EXISTS automation_stream_tasks (
            id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL REFERENCES automation_runs(id) ON DELETE CASCADE,
            step_id BIGINT NOT NULL REFERENCES automation_run_steps(id) ON DELETE CASCADE,
            contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
            step_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 2,
            available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            lease_until TIMESTAMPTZ,
            lease_token TEXT,
            finished_token TEXT,
            bucket TEXT,
            input_data JSONB,
            result JSONB NOT NULL DEFAULT '{}'::jsonb,
            error TEXT,
            dispatched BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, step_id, contact_id)
        );
        ALTER TABLE automation_stream_tasks ADD COLUMN IF NOT EXISTS finished_token TEXT;
        CREATE INDEX IF NOT EXISTS idx_stream_tasks_claim ON automation_stream_tasks(run_id, status, available_at);
        CREATE INDEX IF NOT EXISTS idx_stream_tasks_bucket ON automation_stream_tasks(bucket, lease_until) WHERE status = 'running';
        CREATE TABLE IF NOT EXISTS automation_export_deliveries (
            id BIGSERIAL PRIMARY KEY,
            task_id BIGINT NOT NULL REFERENCES automation_stream_tasks(id) ON DELETE CASCADE,
            contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
            destination TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_token TEXT,
            receipt JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(contact_id, destination)
        );
        ALTER TABLE automation_export_deliveries ADD COLUMN IF NOT EXISTS attempt_token TEXT;
    """)


def clean_json(value):
    return json.loads(json.dumps(value, default=str))


def contact_input(contact):
    return clean_json(
        {key: contact.get(key) for key in CONTACT_FIELDS if key in contact}
    )


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def progress(cursor, run_id):
    cursor.execute(
        """
        SELECT s.step_type, s.step_order, t.status, COUNT(t.id) AS count
        FROM automation_run_steps s LEFT JOIN automation_stream_tasks t ON t.step_id = s.id
        WHERE s.run_id = %s GROUP BY s.step_type, s.step_order, t.status ORDER BY s.step_order
    """,
        (run_id,),
    )
    result = {}
    for row in cursor.fetchall():
        item = result.setdefault(
            row["step_type"],
            {
                "step_type": row["step_type"],
                "step_order": row["step_order"],
                **dict.fromkeys(
                    (*ACTIVE, "completed", "failed", "skipped", "cancelled"), 0
                ),
            },
        )
        if row["status"]:
            item[row["status"]] = row["count"]
    if "export" in result:
        cursor.execute(
            """SELECT COUNT(*) AS count FROM automation_export_deliveries d
               JOIN automation_stream_tasks t ON t.id = d.task_id
               WHERE t.run_id = %s AND d.status = 'exported'""",
            (run_id,),
        )
        result["export"]["exported"] = cursor.fetchone()["count"]
    return list(result.values())


def enroll(cursor, run, steps, needs_source_email=False):
    """ON CONFLICT permits continuous intake, including after an empty queue."""
    for step in steps:
        pipeline = step["step_type"] == "pipeline"
        cursor.execute(
            """
            INSERT INTO automation_stream_tasks (run_id, step_id, contact_id, step_type, status, max_retries)
            SELECT %s, %s, c.id, %s, %s, %s FROM contacts c
            WHERE c.campaign_id = %s
            ON CONFLICT (run_id, step_id, contact_id) DO NOTHING
        """,
            (
                run["id"],
                step["id"],
                "source_email"
                if pipeline and needs_source_email
                else step["step_type"],
                "completed" if pipeline and not needs_source_email else "pending",
                step["max_retries"],
                run["campaign_id"],
            ),
        )


def recover(cursor, run_id):
    cursor.execute("SELECT status FROM automation_runs WHERE id = %s FOR UPDATE", (run_id,))
    run = cursor.fetchone()
    if not run:
        return
    terminal = run["status"] in TERMINAL_RUNS
    # Keep the canonical delivery reference available to the review API even
    # when the worker died before copying the provider response into the task.
    cursor.execute(
        """
        UPDATE automation_stream_tasks t
        SET result = COALESCE(t.result, '{}'::jsonb) || jsonb_build_object('delivery_id', d.id)
        FROM automation_export_deliveries d
        WHERE t.run_id = %s AND t.step_type = 'export' AND d.task_id = t.id
          AND t.status = 'running' AND t.lease_until <= clock_timestamp()
          AND NOT (COALESCE(t.result, '{}'::jsonb) ? 'delivery_id')
    """,
        (run_id,),
    )
    cursor.execute(
        """
        UPDATE automation_stream_tasks t
        SET status = CASE d.status WHEN 'exported' THEN 'completed' WHEN 'filtered' THEN 'skipped' ELSE 'failed' END,
            result = d.receipt || jsonb_build_object('delivery_id', d.id),
            error = CASE WHEN d.status = 'failed' THEN d.receipt->'error'->>'message' ELSE NULL END,
            dispatched = FALSE, finished_token = t.lease_token,
            lease_token = NULL, lease_until = NULL, updated_at = CURRENT_TIMESTAMP
        FROM automation_export_deliveries d
        WHERE t.run_id = %s AND t.step_type = 'export' AND d.task_id = t.id
          AND (d.status IN ('exported', 'filtered') OR (d.status = 'failed' AND d.receipt->'retryable' = 'false'::jsonb))
          AND t.status = 'running' AND t.lease_until <= clock_timestamp()
    """,
        (run_id,),
    )
    # A lost export response is not evidence that the recipient rejected the batch.
    cursor.execute(
        """
        UPDATE automation_stream_tasks SET
            status = CASE WHEN step_type = 'export' AND dispatched THEN 'uncertain'
                          WHEN %s THEN 'cancelled'
                          WHEN attempts > max_retries THEN 'failed' ELSE 'retry' END,
            error = CASE WHEN step_type = 'export' AND dispatched THEN 'Delivery outcome unknown after worker interruption; review destination before retrying'
                         ELSE 'Worker lease expired' END,
            lease_token = NULL, lease_until = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND status = 'running' AND lease_until <= clock_timestamp()
    """,
        (terminal, run_id),
    )
    cursor.execute(
        """
        UPDATE automation_export_deliveries d SET status = 'uncertain', updated_at = CURRENT_TIMESTAMP
        FROM automation_stream_tasks t WHERE d.task_id = t.id AND t.run_id = %s
          AND t.status = 'uncertain' AND d.status = 'sending'
    """,
        (run_id,),
    )
    _release_reserved(cursor, run_id, stopping=terminal)
    if terminal:
        return
    cursor.execute(
        """
        UPDATE automation_stream_tasks t SET status = 'skipped', error = 'An earlier contact step failed', updated_at = CURRENT_TIMESTAMP
        FROM automation_run_steps s WHERE t.step_id = s.id AND t.run_id = %s AND t.status IN ('pending', 'retry')
        AND EXISTS (
            SELECT 1 FROM automation_stream_tasks p JOIN automation_run_steps ps ON ps.id = p.step_id
            WHERE p.run_id = t.run_id AND p.contact_id = t.contact_id AND ps.step_order < s.step_order
              AND (p.status IN ('failed', 'cancelled') OR p.error = 'An earlier contact step failed')
        )
    """,
        (run_id,),
    )
    cursor.execute(
        """
        UPDATE automation_runs SET status = 'waiting_confirmation', latest_error = 'A delivery outcome is unknown; review it before resuming'
        WHERE id = %s AND status IN ('queued', 'running')
          AND EXISTS (SELECT 1 FROM automation_stream_tasks WHERE run_id = %s AND status = 'uncertain')
    """,
        (run_id, run_id),
    )


def claim(
    cursor, run_id, step, limit, bucket, concurrency, interval=0, lease_seconds=180
):
    """Reserve provider capacity and durable work in the same short transaction."""
    cursor.execute(
        "SELECT status FROM automation_runs WHERE id = %s FOR UPDATE", (run_id,)
    )
    run = cursor.fetchone()
    if not run or run["status"] not in ("queued", "running"):
        return []
    cursor.execute(
        "INSERT INTO enrichment_api_rate_limits(endpoint_key) VALUES (%s) ON CONFLICT DO NOTHING",
        (bucket,),
    )
    cursor.execute(
        "SELECT next_request_at <= clock_timestamp() AS ready FROM enrichment_api_rate_limits WHERE endpoint_key = %s FOR UPDATE",
        (bucket,),
    )
    if not cursor.fetchone()["ready"]:
        return []
    cursor.execute(
        """
        SELECT MAX(s.stream_interval) AS delay, MIN(s.stream_concurrency) AS concurrency
        FROM automation_run_steps s JOIN automation_runs r ON r.id = s.run_id
        WHERE s.stream_bucket = %s AND r.status IN ('queued', 'running')
    """,
        (bucket,),
    )
    shared = cursor.fetchone()
    interval = max(interval, float(shared["delay"] or 0))
    concurrency = min(concurrency, int(shared["concurrency"] or concurrency))
    cursor.execute(
        """
        SELECT MIN((prompt_config::jsonb->>'requests_per_minute')::integer) AS rate
        FROM enrichment_runs WHERE service = 'prompt_http' AND status IN ('queued', 'running')
          AND NOT cancel_requested AND NOT pause_requested AND prompt_config::jsonb->>'endpoint_key' = %s
    """,
        (bucket,),
    )
    legacy = cursor.fetchone()["rate"]
    if legacy:
        interval = max(interval, 60 / legacy)
    cursor.execute(
        "SELECT COUNT(DISTINCT lease_token) AS n FROM automation_stream_tasks WHERE bucket = %s AND status = 'running' AND lease_until > CURRENT_TIMESTAMP",
        (bucket,),
    )
    slots = max(0, int(concurrency) - cursor.fetchone()["n"])
    count = (
        int(limit)
        if step["step_type"] == "export" and slots
        else min(int(limit), slots)
    )
    if interval and step["step_type"] != "export":
        count = min(count, 1)
    if count <= 0:
        return []
    cursor.execute(
        """
        SELECT t.* FROM automation_stream_tasks t
        WHERE t.run_id = %s AND t.step_id = %s AND t.status IN ('pending', 'retry')
          AND t.available_at <= CURRENT_TIMESTAMP
          AND NOT EXISTS (
            SELECT 1 FROM automation_stream_tasks p JOIN automation_run_steps s ON s.id = p.step_id
            WHERE p.run_id = t.run_id AND p.contact_id = t.contact_id AND s.step_order < %s
              AND p.status NOT IN ('completed', 'skipped')
          )
        ORDER BY t.id LIMIT %s FOR UPDATE OF t SKIP LOCKED
    """,
        (run_id, step["id"], step["step_order"], count),
    )
    tasks = [dict(row) for row in cursor.fetchall()]
    if not tasks:
        return []
    if step["step_type"] == "export":
        age = (datetime.now(timezone.utc) - tasks[0]["created_at"]).total_seconds()
        if len(tasks) < limit and age < 10:
            return []
    batch_token = str(uuid4())
    for task in tasks:
        token = batch_token if step["step_type"] == "export" else str(uuid4())
        cursor.execute("SELECT * FROM contacts WHERE id = %s", (task["contact_id"],))
        contact = dict(cursor.fetchone())
        cursor.execute(
            """
            UPDATE automation_stream_tasks SET status = 'running', attempts = attempts + 1,
                lease_token = %s, lease_until = CURRENT_TIMESTAMP + %s * INTERVAL '1 second',
                input_data = %s, bucket = %s, dispatched = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """,
            (token, lease_seconds, Json(contact_input(contact)), bucket, task["id"]),
        )
        task.update(
            lease_token=token,
            contact=contact,
            input_data=contact_input(contact),
            attempts=task["attempts"] + 1,
        )
    cursor.execute(
        "UPDATE enrichment_api_rate_limits SET next_request_at = clock_timestamp() + %s * INTERVAL '1 second' WHERE endpoint_key = %s",
        (interval, bucket),
    )
    return tasks


def heartbeat(cursor, tasks, lease_seconds=180):
    for task in tasks:
        cursor.execute(
            """
            UPDATE automation_stream_tasks t SET lease_until = clock_timestamp() + %s * INTERVAL '1 second'
            FROM automation_runs r WHERE t.id = %s AND t.lease_token = %s AND t.run_id = r.id
              AND t.lease_until > clock_timestamp()
              AND t.status = 'running' AND r.status IN ('queued', 'running', 'waiting_confirmation')
        """,
            (lease_seconds, task["id"], task["lease_token"]),
        )


def finish(cursor, task, outcome):
    """Commit an owned attempt under the run lock, shared with Stop/recovery.

    Explicit retry/deferred outcomes refund the claim's attempt by default;
    count_attempt=True opts into the normal retry budget. retryable=False at
    either the outcome or result level is a permanent failure. The runtime
    owns delivery writes and supplies result.delivery_id for canonical review.
    """
    if not task.get("lease_token"):
        return False
    # Resolve the actual run from the task instead of trusting caller metadata.
    cursor.execute(
        """
        SELECT r.id, r.status FROM automation_runs r
        WHERE r.id = (SELECT run_id FROM automation_stream_tasks WHERE id = %s)
        FOR UPDATE OF r
    """,
        (task["id"],),
    )
    run = cursor.fetchone()
    if not run:
        return False
    cursor.execute(
        """
        SELECT * FROM automation_stream_tasks WHERE id = %s AND run_id = %s FOR UPDATE
    """,
        (task["id"], run["id"]),
    )
    current = cursor.fetchone()
    if current and current["status"] != "running" and current.get("finished_token") == task["lease_token"]:
        return True
    if (
        not current
        or current["lease_token"] != task["lease_token"]
        or current["status"] != "running"
    ):
        return False
    cursor.execute(
        "SELECT lease_until > clock_timestamp() AS valid FROM automation_stream_tasks WHERE id = %s",
        (task["id"],),
    )
    if not cursor.fetchone()["valid"]:
        return False
    result = dict(outcome.get("result") or {})
    previous_result = current.get("result") or {}
    if "delivery_id" not in result and "delivery_id" in previous_result:
        result["delivery_id"] = previous_result["delivery_id"]
    status = outcome.get("status", "failed")
    explicit_retry = status in ("retry", "deferred")
    count_attempt = outcome.get("count_attempt", not explicit_retry) is not False
    if explicit_retry:
        status = "retry"
    elif status not in ("completed", "skipped", "failed", "blocked", "uncertain"):
        status = "failed"
    retryable = outcome.get("retryable") is not False and result.get("retryable") is not False
    terminal = run["status"] in TERMINAL_RUNS
    is_export = current["step_type"] == "export"
    # A reported unknown acceptance must never be turned into an automatic retry.
    if is_export and result.get("status") == "unknown":
        status = "uncertain"
    error = str(outcome.get("error") or "")[:10000] or None
    cursor.execute(
        "SELECT * FROM contacts WHERE id = %s FOR UPDATE", (task["contact_id"],)
    )
    contact = cursor.fetchone()
    updates = {
        key: value
        for key, value in outcome.get("updates", {}).items()
        if key in CONTACT_FIELDS
    }
    if terminal:
        updates = {}
        if not is_export or status not in ("completed", "skipped", "failed", "uncertain"):
            status = "cancelled"
    elif (
        current["step_type"] != "export"
        and contact_input(contact) != current["input_data"]
    ):
        status, updates, error = (
            "retry",
            {},
            "Contact changed during processing; retrying current values",
        )
        count_attempt = False
    attempts = max(0, current["attempts"] - int(not count_attempt))
    if status == "retry" and not retryable:
        status = "failed"
    if status == "retry" and count_attempt and attempts > current["max_retries"]:
        status = "failed"
    if status == "failed" and not terminal and retryable and attempts <= current["max_retries"]:
        status = "retry"
    if updates and status == "completed":
        if "email" in updates and updates["email"] != contact.get("email"):
            updates["email_status"] = "unverified"
        if "domain" in updates and updates["domain"] != contact.get("domain"):
            updates.update(
                domain_status="unchecked",
                domain_last_checked_at=None,
                domain_dns_status=None,
                domain_ssl_status=None,
                domain_error=None,
                domain_http_status=None,
                domain_https_status=None,
            )
        query = sql.SQL("UPDATE contacts SET {} WHERE id = %s").format(
            sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(key)) for key in updates
            )
        )
        cursor.execute(query, (*updates.values(), task["contact_id"]))
    delay = max(
        float(outcome.get("retry_after") or 0),
        min(300, 2 ** min(attempts, 8)) if count_attempt else 0,
    )
    retry_after = float(outcome.get("retry_after") or 0)
    if retry_after > 0 and current.get("bucket"):
        cursor.execute(
            """UPDATE enrichment_api_rate_limits
               SET next_request_at = GREATEST(next_request_at, clock_timestamp() + %s * INTERVAL '1 second')
               WHERE endpoint_key = %s""",
            (retry_after, current["bucket"]),
        )
    cursor.execute(
        """
        UPDATE automation_stream_tasks SET status = %s, result = %s, error = %s,
            available_at = CURRENT_TIMESTAMP + %s * INTERVAL '1 second',
            attempts = %s,
            finished_token = lease_token, lease_token = NULL, lease_until = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """,
        (
            status,
            Json(clean_json(result)),
            error,
            delay,
            attempts,
            task["id"],
        ),
    )
    if is_export:
        _release_reserved(cursor, run["id"], stopping=terminal)
    if status in ("blocked", "uncertain") and not terminal:
        cursor.execute(
            """UPDATE automation_runs SET status = 'waiting_confirmation', latest_error = %s
               WHERE id = %s AND status IN ('queued', 'running', 'waiting_confirmation')""",
            (error or status, current["run_id"]),
        )
    cursor.execute(
        """
        INSERT INTO automation_run_logs(run_id, campaign_id, step_id, level, message)
        SELECT %s, campaign_id, %s, %s, %s FROM automation_runs WHERE id = %s
    """,
        (
            current["run_id"],
            current["step_id"],
            "info" if status in SUCCESS else "warning",
            f"Contact #{task['contact_id']} {current['step_type']}: {status}"
            + (f" - {error}" if error else ""),
            current["run_id"],
        ),
    )
    return True


def finalize(cursor, run_id, source_closed):
    cursor.execute(
        """UPDATE automation_runs SET stream_source_closed = %s
           WHERE id = %s AND status IN ('queued', 'running', 'waiting_confirmation') RETURNING id""",
        (source_closed, run_id),
    )
    if not cursor.fetchone():
        return False
    cursor.execute(
        """
        UPDATE automation_run_steps s SET
            status = CASE WHEN EXISTS (SELECT 1 FROM automation_stream_tasks t WHERE t.step_id = s.id AND t.status IN ('blocked', 'uncertain')) THEN 'waiting_confirmation'
                          WHEN %s AND NOT EXISTS (SELECT 1 FROM automation_stream_tasks t WHERE t.step_id = s.id AND t.status IN ('pending', 'retry', 'running')) THEN 'completed'
                          ELSE 'running' END,
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
        WHERE s.run_id = %s AND NOT (s.step_type = 'export' AND COALESCE((s.config->>'require_confirmation')::boolean, FALSE)
            AND NOT COALESCE((s.config->>'confirmed')::boolean, FALSE))
    """,
        (source_closed, run_id),
    )
    if not source_closed:
        return False
    cursor.execute(
        "SELECT COUNT(*) AS n FROM automation_stream_tasks WHERE run_id = %s AND status = ANY(%s)",
        (run_id, list(ACTIVE)),
    )
    if cursor.fetchone()["n"]:
        return False
    cursor.execute(
        """
        UPDATE automation_runs SET status = 'completed', completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s AND status IN ('queued', 'running')
    """,
        (run_id,),
    )
    return cursor.rowcount > 0


def cancel(cursor, run_id):
    cursor.execute("SELECT id FROM automation_runs WHERE id = %s FOR UPDATE", (run_id,))
    if not cursor.fetchone():
        return
    # Running exports retain their lease until the response is recorded.
    cursor.execute(
        """
        UPDATE automation_stream_tasks SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND (status IN ('pending', 'retry', 'blocked') OR (status = 'running' AND step_type <> 'export'))
    """,
        (run_id,),
    )
    _release_reserved(cursor, run_id, stopping=True)


def _release_reserved(cursor, run_id, stopping=False):
    """Run/task locks precede receipt writes; reserved means no HTTP dispatch."""
    cursor.execute(
        """
        UPDATE automation_export_deliveries d
        SET status = 'failed', attempt_token = NULL, updated_at = CURRENT_TIMESTAMP,
            receipt = jsonb_build_object('status', 'failed', 'attempted', FALSE, 'retryable', TRUE,
                'delivery_id', d.id, 'error', jsonb_build_object('code', 'reservation_released',
                'message', 'Export reservation ended before dispatch'))
        FROM automation_stream_tasks t
        WHERE d.task_id = t.id AND t.run_id = %s AND d.status = 'reserved' AND NOT t.dispatched
          AND (%s OR t.status <> 'running' OR t.lease_token IS DISTINCT FROM d.attempt_token
               OR t.lease_until IS NULL OR t.lease_until <= clock_timestamp())
    """,
        (run_id, stopping),
    )


def recover_stopped(cursor):
    """Sweep stopped/paused work without restarting runs or live reservations."""
    cursor.execute(
        """SELECT r.id FROM automation_runs r WHERE r.status IN ('cancelled', 'waiting_confirmation')
           AND (EXISTS (SELECT 1 FROM automation_stream_tasks t WHERE t.run_id = r.id
                        AND t.status = 'running' AND t.lease_until <= clock_timestamp())
                OR EXISTS (SELECT 1 FROM automation_export_deliveries d
                           JOIN automation_stream_tasks t ON t.id = d.task_id
                           WHERE t.run_id = r.id AND d.status = 'reserved' AND NOT t.dispatched
                             AND (r.status = 'cancelled' OR t.status <> 'running'
                                  OR t.lease_token IS DISTINCT FROM d.attempt_token
                                  OR t.lease_until IS NULL OR t.lease_until <= clock_timestamp())))
           ORDER BY r.id"""
    )
    for run in cursor.fetchall():
        recover(cursor, run["id"])
