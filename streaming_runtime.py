"""Web-app scheduler for the opt-in streaming funnel engine."""

import asyncio
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from psycopg2.extras import Json

import streaming
import streaming_export
import streaming_services

log = logging.getLogger(__name__)


class _SharedWorkCapacity:
    """App-wide cap so many streaming funnels cannot each create 64 workers."""

    def __init__(self, limit=32):
        self._limit = limit
        self._in_use = 0
        self._lock = threading.Lock()

    def configure(self, limit):
        with self._lock:
            self._limit = max(1, min(256, int(limit or 32)))

    def try_acquire(self):
        with self._lock:
            if self._in_use >= self._limit:
                return False
            self._in_use += 1
            return True

    def release(self):
        with self._lock:
            self._in_use = max(0, self._in_use - 1)


_shared_capacity = _SharedWorkCapacity()
_shared_executor = ThreadPoolExecutor(max_workers=256, thread_name_prefix="stream-task")


def _refresh_shared_capacity(app):
    getter = getattr(app, "_daemon_capacity_settings_snapshot", None)
    if not callable(getter):
        return
    try:
        _shared_capacity.configure(getter().get("max_parallel_stream_tasks", 32))
    except Exception:
        log.warning("Could not refresh shared streaming capacity", exc_info=True)


def export_config(config):
    result = {**config, "template": config["template_snapshot"]}
    if config.get("sendread_ab_list_id"):
        result["destination"] = {"list_id": str(config["sendread_ab_list_id"])}
    return result


def scheduling(step):
    config = step["config"]
    if step["step_type"] == "export":
        details = streaming_export.limits(export_config(config))
        destination = json.loads(
            streaming_export.destination_key(export_config(config))
        )
        destination.pop("target_id", None)
        destination.pop("target_type", None)
        key = "export:" + streaming.fingerprint(destination)
        return (
            key,
            1,
            float(details["min_interval_seconds"]),
            180,
            min(
                int(config.get("batch_size", 50)),
                details.get("max_contacts_per_call", 50),
            ),
        )
    details = streaming_services.limits(step["step_type"], config)
    rate = details.get("requests_per_minute")
    concurrency = int(
        config.get("concurrency")
        or config["template_snapshot"].get("api_config", {}).get("concurrency")
        or 1
    )
    if (
        step["step_type"] == "enrichment"
        and config["template_snapshot"].get("service") == "prompt_http"
    ):
        concurrency = min(
            100, max(1, int((float(rate) * details["timeout_seconds"] + 59) // 60))
        )
    if step["step_type"] == "dns_check":
        concurrency = int(
            config["template_snapshot"].get("api_config", {}).get("concurrency", 10)
        )
    return (
        details["endpoint_key"],
        max(1, min(100, concurrency)),
        60 / rate if rate else 0,
        max(180, details["timeout_seconds"] + 60),
        100,
    )


def cancelled(app, run_id):
    with app.get_db() as conn:
        run = app._load_automation_run(conn.cursor(), run_id)
    return not run or run["status"] in ("cancelled", "completed", "failed")


def _wait_for_dispatch(app, tasks, bucket, interval, allow_paused=False):
    """Space actual starts too, since preparation can outlive a claim's rate slot."""
    active = (
        ("queued", "running", "waiting_confirmation")
        if allow_paused
        else ("queued", "running")
    )
    while True:
        with app.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM automation_runs WHERE id = %s FOR UPDATE",
                (tasks[0]["run_id"],),
            )
            run = cursor.fetchone()
            if not run or run["status"] not in active:
                return False
            for task in tasks:
                cursor.execute(
                    """SELECT id FROM automation_stream_tasks WHERE id = %s AND status = 'running'
                    AND lease_token = %s AND lease_until > clock_timestamp()""",
                    (task["id"], task["lease_token"]),
                )
                if not cursor.fetchone():
                    return False
            cursor.execute(
                "INSERT INTO enrichment_api_rate_limits(endpoint_key) VALUES (%s) ON CONFLICT DO NOTHING",
                (bucket,),
            )
            cursor.execute(
                "SELECT endpoint_key FROM enrichment_api_rate_limits WHERE endpoint_key = %s FOR UPDATE",
                (bucket,),
            )
            cursor.execute(
                """SELECT GREATEST(%s, COALESCE(MAX(s.stream_interval), 0)) AS spacing
                FROM automation_run_steps s JOIN automation_runs r ON r.id = s.run_id
                WHERE s.stream_bucket = %s AND r.status IN ('queued', 'running', 'waiting_confirmation')""",
                (interval, bucket),
            )
            spacing = float(cursor.fetchone()["spacing"])
            cursor.execute(
                """SELECT MIN((prompt_config::jsonb->>'requests_per_minute')::integer) AS rate
                FROM enrichment_runs WHERE service = 'prompt_http'
                  AND status IN ('queued', 'running') AND NOT cancel_requested AND NOT pause_requested
                  AND prompt_config::jsonb->>'endpoint_key' = %s""",
                (bucket,),
            )
            legacy_rate = cursor.fetchone()["rate"]
            if legacy_rate:
                spacing = max(spacing, 60.0 / legacy_rate)
            cursor.execute(
                """SELECT GREATEST(0, EXTRACT(EPOCH FROM
                (last_dispatch_at + %s * INTERVAL '1 second' - clock_timestamp()))) AS delay
                FROM enrichment_api_rate_limits WHERE endpoint_key = %s""",
                (spacing, bucket),
            )
            delay = float(cursor.fetchone()["delay"] or 0)
            if delay <= 0:
                cursor.execute(
                    """UPDATE enrichment_api_rate_limits SET last_dispatch_at = clock_timestamp(),
                    next_request_at = GREATEST(next_request_at, clock_timestamp() + %s * INTERVAL '1 second')
                    WHERE endpoint_key = %s""",
                    (spacing, bucket),
                )
                conn.commit()
                return True
            conn.commit()
        time.sleep(min(delay, 0.25))


def _contact_work(app, step, task):
    config = dict(step["config"])
    config.update((task.get("result") or {}).get("retry_config") or {})
    bucket, _, interval, _, _ = scheduling(step)
    context = {
        "app": app,
        "run_id": task["run_id"],
        "task_id": task["id"],
        "campaign_id": step["campaign_id"],
        "cancelled": lambda: cancelled(app, task["run_id"]),
        "before_request": lambda: _wait_for_dispatch(
            app, [task], bucket, interval, allow_paused=True
        ),
    }
    contact = dict(task["contact"])
    with app.get_db() as conn:
        cursor = conn.cursor()
        city_map = app._build_campaign_request_city_map(cursor, step["campaign_id"])
        app._apply_city_fallback_for_export([contact], city_map)
        cursor.execute(
            """
            SELECT result FROM automation_stream_tasks WHERE contact_id = %s AND step_type = %s
            AND status = 'completed' AND id <> %s ORDER BY updated_at DESC LIMIT 1
        """,
            (task["contact_id"], step["step_type"], task["id"]),
        )
        previous = cursor.fetchone()
        if previous:
            context["verification_fingerprints"] = {
                step["step_type"]: previous["result"].get("input_fingerprint")
            }
    return streaming_services.execute(step["step_type"], config, contact, context)


def _export_work(app, step, tasks):
    config = export_config(step["config"])
    destination = streaming_export.destination_key(config)
    ready = []
    results = {}
    with app.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM automation_runs WHERE id = %s FOR UPDATE",
            (tasks[0]["run_id"],),
        )
        run = cursor.fetchone()
        active = run and run["status"] in ("queued", "running")
        for task in tasks:
            cursor.execute(
                """SELECT * FROM automation_stream_tasks WHERE id = %s AND status = 'running'
                AND lease_token = %s AND lease_until > clock_timestamp() FOR UPDATE""",
                (task["id"], task["lease_token"]),
            )
            if not cursor.fetchone():
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Export lease lost before dispatch",
                }
                continue
            if not active:
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Run paused or stopped before export",
                }
                continue
            cursor.execute(
                "SELECT * FROM contacts WHERE id = %s FOR UPDATE", (task["contact_id"],)
            )
            latest = dict(cursor.fetchone())
            if streaming.contact_input(latest) != task["input_data"]:
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Contact changed before export; checking current values",
                }
                continue
            cursor.execute(
                """
                SELECT t.result, t.step_type, s.step_order FROM automation_stream_tasks t
                JOIN automation_run_steps s ON s.id = t.step_id
                WHERE t.run_id = %s AND t.contact_id = %s AND t.step_type IN ('dns_check', 'email_verification')
                  AND t.status IN ('completed', 'skipped') ORDER BY s.step_order
            """,
                (task["run_id"], task["contact_id"]),
            )
            stale_order = None
            for check in cursor.fetchall():
                snapshot = check["result"].get("input_snapshot", {})
                field = "domain" if check["step_type"] == "dns_check" else "email"
                value = (
                    app._normalize_domain(latest.get(field))
                    if field == "domain"
                    else str(latest.get(field) or "").strip().lower()
                )
                if field in snapshot and snapshot[field] != value:
                    stale_order = check["step_order"]
                    break
            if stale_order is not None:
                cursor.execute(
                    """
                    UPDATE automation_stream_tasks t SET status = 'retry', attempts = 0, available_at = CURRENT_TIMESTAMP,
                        error = 'Contact input changed after verification'
                    FROM automation_run_steps s WHERE t.step_id = s.id AND t.run_id = %s AND t.contact_id = %s
                      AND s.step_order >= %s AND t.status IN ('completed', 'skipped')
                """,
                    (task["run_id"], task["contact_id"], stale_order),
                )
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Contact input changed; verification requeued before export",
                }
                continue
            if not streaming_export.eligibility(task["contact"], config, app):
                results[task["id"]] = {
                    "status": "skipped",
                    "result": {"reason": "Export filters did not match"},
                }
                continue
            cursor.execute(
                """
                INSERT INTO automation_export_deliveries(task_id, contact_id, destination, status, attempt_token)
                VALUES (%s, %s, %s, 'reserved', %s) ON CONFLICT (contact_id, destination) DO NOTHING
            """,
                (task["id"], task["contact_id"], destination, task["lease_token"]),
            )
            cursor.execute(
                "SELECT * FROM automation_export_deliveries WHERE contact_id = %s AND destination = %s FOR UPDATE",
                (task["contact_id"], destination),
            )
            receipt = cursor.fetchone()
            if receipt["status"] == "exported":
                results[task["id"]] = {
                    "status": "completed"
                    if receipt["task_id"] == task["id"]
                    else "skipped",
                    "result": {
                        "reason": "Already delivered to this destination",
                        "delivery_id": receipt["id"],
                    },
                }
                continue
            if receipt["status"] in ("uncertain", "sending"):
                results[task["id"]] = {
                    "status": "uncertain",
                    "error": "Another delivery to this destination needs review",
                    "result": {"delivery_id": receipt["id"]},
                }
                continue
            if receipt["status"] == "reserved" and receipt["task_id"] != task["id"]:
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Another worker is preparing this delivery",
                }
                continue
            cursor.execute(
                "UPDATE automation_export_deliveries SET status = 'reserved', task_id = %s, attempt_token = %s WHERE id = %s",
                (task["id"], task["lease_token"], receipt["id"]),
            )
            task["delivery_id"] = receipt["id"]
            ready.append(task)
        conn.commit()
    if not ready:
        return results
    with app.get_db() as conn:
        config["request_city_map"] = app._build_campaign_request_city_map(
            conn.cursor(), step["campaign_id"]
        )
    bucket, _, interval, _, _ = scheduling(step)
    if not _wait_for_dispatch(app, ready, bucket, interval):
        for task in ready:
            results[task["id"]] = {
                "status": "retry",
                "error": "Export deferred before dispatch",
            }
        return results
    # Fence immediately before HTTP, after any potentially slow preparation.
    # Once dispatched, a timeout or lost lease is unknown until reviewed.
    dispatch = []
    with app.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM automation_runs WHERE id = %s FOR UPDATE",
            (tasks[0]["run_id"],),
        )
        run = cursor.fetchone()
        for task in ready:
            cursor.execute(
                """SELECT id FROM automation_stream_tasks WHERE id = %s AND status = 'running'
                AND lease_token = %s AND lease_until > clock_timestamp() FOR UPDATE""",
                (task["id"], task["lease_token"]),
            )
            valid = bool(cursor.fetchone())
            if not valid or not run or run["status"] not in ("queued", "running"):
                cursor.execute(
                    """UPDATE automation_export_deliveries SET status = 'failed', attempt_token = NULL
                    WHERE id = %s AND attempt_token = %s AND status = 'reserved'""",
                    (task["delivery_id"], task["lease_token"]),
                )
                results[task["id"]] = {
                    "status": "retry",
                    "error": "Export deferred before dispatch",
                }
                continue
            cursor.execute(
                """UPDATE automation_export_deliveries SET status = 'sending'
                WHERE id = %s AND attempt_token = %s AND status = 'reserved'""",
                (task["delivery_id"], task["lease_token"]),
            )
            if not cursor.rowcount:
                results[task["id"]] = {
                    "status": "uncertain",
                    "result": {"delivery_id": task["delivery_id"]},
                    "error": "Export reservation changed before dispatch",
                }
                continue
            cursor.execute(
                "UPDATE automation_stream_tasks SET dispatched = TRUE WHERE id = %s AND lease_token = %s AND status = 'running'",
                (task["id"], task["lease_token"]),
            )
            if cursor.rowcount:
                dispatch.append(task)
        if dispatch:
            cursor.execute(
                """UPDATE enrichment_api_rate_limits SET last_dispatch_at = clock_timestamp(),
                next_request_at = GREATEST(next_request_at, clock_timestamp() + %s * INTERVAL '1 second')
                WHERE endpoint_key = %s""",
                (interval, bucket),
            )
        conn.commit()
    ready = dispatch
    if not ready:
        return results
    try:
        receipts = streaming_export.send_batch(
            config, [task["contact"] for task in ready]
        )
    except Exception:
        log.exception(
            "Streaming export response unavailable for run %s", tasks[0]["run_id"]
        )
        receipts = [
            {
                "contact_id": task["contact_id"],
                "status": "unknown",
                "error": {
                    "message": "Delivery response unavailable; review destination"
                },
            }
            for task in ready
        ]
    by_contact = {receipt["contact_id"]: receipt for receipt in receipts}
    with app.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM automation_runs WHERE id = %s FOR UPDATE",
            (tasks[0]["run_id"],),
        )
        cursor.execute(
            "SELECT id FROM automation_stream_tasks WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
            ([task["id"] for task in ready],),
        )
        exported = 0
        for task in ready:
            receipt = by_contact.get(
                task["contact_id"],
                {"status": "unknown", "error": {"message": "Missing delivery receipt"}},
            )
            state = receipt["status"]
            error = receipt.get("error") or {}
            status = {
                "exported": "completed",
                "filtered": "skipped",
                "unknown": "uncertain",
            }.get(state, "failed")
            if error.get("http_status") in (401, 403):
                status = "blocked"
            exported += int(state == "exported")
            # Record provider acceptance even if Stop was clicked while HTTP was in flight.
            cursor.execute(
                """
                UPDATE automation_export_deliveries SET status = %s, receipt = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND attempt_token = %s
            """,
                (
                    "uncertain" if state == "unknown" else state,
                    Json(streaming.clean_json(receipt)),
                    task["delivery_id"],
                    task["lease_token"],
                ),
            )
            results[task["id"]] = {
                "status": status,
                "result": {**receipt, "delivery_id": task["delivery_id"]},
                "error": error.get("message"),
                "retry_after": error.get("retry_after_seconds"),
                "retryable": receipt.get("retryable", True),
            }
            if state != "unknown":
                cursor.execute(
                    "UPDATE automation_stream_tasks SET dispatched = FALSE WHERE id = %s AND lease_token = %s",
                    (task["id"], task["lease_token"]),
                )
        cursor.execute(
            """
            INSERT INTO export_logs(campaign_id, template_id, contacts_exported, status)
            VALUES (%s, %s, %s, %s)
        """,
            (
                step["campaign_id"],
                step["config"]["template_id"],
                exported,
                "success" if exported == len(ready) else "partial",
            ),
        )
        conn.commit()
    return results


def start_source(app, run, steps):
    if not any(step["step_type"] == "pipeline" for step in steps):
        return
    with app.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, daemon_ignore FROM search_campaigns WHERE id = %s",
            (run["campaign_id"],),
        )
        campaign = cursor.fetchone()
        source_type = app._get_campaign_source_type(cursor, run["campaign_id"])
        cursor.execute(
            """SELECT 1 FROM pipeline_runs WHERE campaign_id = %s
            AND status IN ('pending', 'running') AND execution_mode = 'batch' LIMIT 1""",
            (run["campaign_id"],),
        )
        if cursor.fetchone():
            raise ValueError(
                "A batch scraping pipeline is already active; wait for it to finish, then resume this funnel"
            )
    if campaign["status"] == "completed":
        return
    if campaign["daemon_ignore"]:
        raise ValueError(
            "Campaign is excluded from scraping; enable sourcing before starting the funnel"
        )
    pipeline = next(step for step in steps if step["step_type"] == "pipeline")
    if source_type == "http_api":
        app._start_http_source_campaign_job(run["campaign_id"])
        app._remember_automation_external_run(run["id"], pipeline["id"], "http_source")
    else:
        result = asyncio.run(
            app.start_campaign_pipeline(
                run["campaign_id"], app.JsonRequest({"actor": "funnel"})
            )
        )
        if result.get("execution_mode", "batch") != "streaming":
            raise ValueError(
                "A batch scraping pipeline is already active; wait for it to finish, then resume this funnel"
            )
        app._remember_automation_external_run(
            run["id"], pipeline["id"], str(result["run_id"])
        )


def source_state(cursor, app, run, steps):
    cursor.execute(
        "SELECT * FROM search_campaigns WHERE id = %s", (run["campaign_id"],)
    )
    campaign = cursor.fetchone()
    source_type = app._get_campaign_source_type(cursor, run["campaign_id"])
    cursor.execute(
        "SELECT COUNT(*) FILTER (WHERE status IN ('pending', 'inuse', 'reserved', 'processing')) AS active FROM requests WHERE campaign_id = %s",
        (run["campaign_id"],),
    )
    pending = cursor.fetchone()["active"]
    needs_email = (
        not campaign["scrape_maps_only"]
        and source_type != "http_api"
        and not campaign["daemon_ignore"]
    )
    has_pipeline = any(step["step_type"] == "pipeline" for step in steps)
    return not pending, needs_email and has_pipeline


def run(app, run_id):
    inflight = {}
    next_capacity_refresh = 0.0
    try:
        _refresh_shared_capacity(app)
        # Session lock keeps two web processes from starting this coordinator together.
        with app.get_db() as leader:
            leader.autocommit = True
            cursor = leader.cursor()
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtext('streaming_funnel'), %s) AS acquired",
                (run_id,),
            )
            if not cursor.fetchone()["acquired"]:
                return
            with app.get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM automation_runs WHERE id = %s FOR UPDATE", (run_id,)
                )
                current = app._load_automation_run(cursor, run_id)
                steps = app._load_automation_steps(cursor, run_id)
                if not current or current["status"] not in ("queued", "running"):
                    return
                cursor.execute(
                    "UPDATE automation_runs SET status = 'running', started_at = COALESCE(started_at, CURRENT_TIMESTAMP) WHERE id = %s",
                    (run_id,),
                )
                for step in steps:
                    if step["step_type"] != "pipeline":
                        bucket, concurrency, interval, _, _ = scheduling(step)
                        cursor.execute(
                            "UPDATE automation_run_steps SET stream_bucket = %s, stream_interval = %s, stream_concurrency = %s WHERE id = %s",
                            (bucket, interval, concurrency, step["id"]),
                        )
                conn.commit()
            start_source(app, current, steps)
            while True:
                if time.monotonic() >= next_capacity_refresh:
                    _refresh_shared_capacity(app)
                    next_capacity_refresh = time.monotonic() + 10.0
                for future in list(inflight):
                    if not future.done():
                        continue
                    tasks, step = inflight.pop(future)
                    try:
                        outcome = future.result()
                    except Exception:
                        log.exception("Streaming task failed for run %s", run_id)
                        outcome = {
                            "status": "failed",
                            "error": "Worker failed; inspect server logs",
                        }
                    with app.get_db() as conn:
                        cursor = conn.cursor()
                        for task in tasks:
                            result = (
                                outcome.get(
                                    task["id"],
                                    {
                                        "status": "uncertain",
                                        "error": "Delivery result unavailable",
                                    },
                                )
                                if step["step_type"] == "export"
                                else outcome
                            )
                            streaming.finish(cursor, task, result)
                        conn.commit()
                with app.get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT id FROM automation_runs WHERE id = %s FOR UPDATE",
                        (run_id,),
                    )
                    current = app._load_automation_run(cursor, run_id)
                    if not current:
                        return
                    for tasks, _step in inflight.values():
                        streaming.heartbeat(cursor, tasks)
                    if current["status"] not in ("queued", "running"):
                        conn.commit()
                        if not inflight:
                            return
                        time.sleep(0.5)
                        continue
                    if current["status"] == "queued":
                        cursor.execute(
                            "UPDATE automation_runs SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                            (run_id,),
                        )
                    steps = app._load_automation_steps(cursor, run_id)
                    closed, needs_email = source_state(cursor, app, current, steps)
                    streaming.enroll(cursor, current, steps, needs_email)
                    streaming.recover(cursor, run_id)
                    done = streaming.finalize(cursor, run_id, closed)
                    conn.commit()
                if done and not inflight:
                    return
                for step in steps:
                    if step["step_type"] == "pipeline" or len(inflight) >= 64:
                        continue
                    config = step["config"]
                    if (
                        step["step_type"] == "export"
                        and config.get("require_confirmation")
                        and not config.get("confirmed")
                    ):
                        with app.get_db() as conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT status FROM automation_runs WHERE id = %s FOR UPDATE",
                                (run_id,),
                            )
                            current = cursor.fetchone()
                            cursor.execute(
                                "UPDATE automation_run_steps SET status = 'waiting_confirmation' WHERE id = %s AND %s",
                                (
                                    step["id"],
                                    bool(
                                        current
                                        and current["status"] in ("queued", "running")
                                    ),
                                ),
                            )
                            conn.commit()
                        continue
                    key, concurrency, interval, lease, limit = scheduling(step)
                    if not _shared_capacity.try_acquire():
                        # Preserve capacity for all funnels. The next scheduler
                        # pass will retry this durable task without consuming an
                        # API request slot.
                        break
                    with app.get_db() as conn:
                        tasks = streaming.claim(
                            conn.cursor(),
                            run_id,
                            step,
                            (
                                min(limit, 64 - len(inflight))
                                if step["step_type"] == "export"
                                else 1
                            ),
                            key,
                            concurrency,
                            interval,
                            lease,
                        )
                        conn.commit()
                    if not tasks:
                        _shared_capacity.release()
                        continue
                    try:
                        if step["step_type"] == "export":
                            future = _shared_executor.submit(_export_work, app, step, tasks)
                            inflight[future] = (tasks, step)
                        else:
                            task = tasks[0]
                            future = _shared_executor.submit(_contact_work, app, step, task)
                            inflight[future] = ([task], step)
                        future.add_done_callback(lambda _future: _shared_capacity.release())
                    except Exception:
                        _shared_capacity.release()
                        raise
                time.sleep(0.5)
    except Exception as exc:
        log.exception("Streaming funnel %s paused", run_id)
        with app.get_db() as conn:
            conn.cursor().execute(
                "UPDATE automation_runs SET status = 'waiting_confirmation', latest_error = %s WHERE id = %s AND status IN ('queued', 'running')",
                (f"Streaming worker stopped: {type(exc).__name__}: {exc}", run_id),
            )
            conn.commit()
    finally:
        # Shared workers finish their leased task and release capacity through
        # their completion callbacks. Durable lease recovery handles a process
        # interruption before a result can be written.
        pass
