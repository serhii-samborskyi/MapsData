"""Daemon queue endpoints and explicit recovery actions for streaming funnels."""

from fastapi import APIRouter, HTTPException, Request

import streaming

_CANONICAL_DELIVERY_JOIN = """
    d.id::text = t.result->>'delivery_id'
    OR (t.result->>'delivery_id' IS NULL AND d.task_id = t.id)
"""


def _canonical_delivery(cursor, task_id):
    cursor.execute(
        f"""SELECT d.* FROM automation_stream_tasks t
            JOIN automation_export_deliveries d ON ({_CANONICAL_DELIVERY_JOIN})
            WHERE t.id = %s""",
        (task_id,),
    )
    deliveries = cursor.fetchall()
    if len(deliveries) != 1:
        raise HTTPException(
            409, "Canonical delivery is missing or ambiguous; refresh delivery review"
        )
    return dict(deliveries[0])


def _related_delivery_tasks(cursor, delivery, task_id):
    cursor.execute(
        """SELECT * FROM automation_stream_tasks
           WHERE id = ANY(%s)
              OR (status = 'uncertain' AND result->>'delivery_id' = %s)
           ORDER BY id""",
        ([delivery["task_id"], task_id], str(delivery["id"])),
    )
    return [dict(row) for row in cursor.fetchall()]


def _task_membership(tasks):
    return {(task["id"], task["run_id"]) for task in tasks}


def _lock_delivery_review(cursor, run_id, task_id):
    cursor.execute(
        "SELECT * FROM automation_stream_tasks WHERE id = %s AND run_id = %s AND step_type = 'export'",
        (task_id, run_id),
    )
    requested = cursor.fetchone()
    if not requested or requested["status"] not in ("uncertain", "cancelled"):
        raise HTTPException(409, "Delivery is not awaiting review")
    observed = _canonical_delivery(cursor, task_id)
    related = _related_delivery_tasks(cursor, observed, task_id)
    run_ids = sorted({task["run_id"] for task in related})

    # Discover the entire lock set first. Never acquire a newly discovered run
    # while holding task/receipt locks: engine and Stop transactions lock runs first.
    cursor.execute(
        "SELECT * FROM automation_runs WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
        (run_ids,),
    )
    runs = {row["id"]: dict(row) for row in cursor.fetchall()}
    if len(runs) != len(run_ids) or any(
        run["execution_mode"] != "streaming" for run in runs.values()
    ):
        raise HTTPException(409, "Related streaming run is unavailable")
    if _task_membership(
        _related_delivery_tasks(cursor, observed, task_id)
    ) != _task_membership(related):
        raise HTTPException(
            409, "Related delivery tasks changed; refresh delivery review"
        )
    task_ids = sorted(task["id"] for task in related)
    cursor.execute(
        "SELECT * FROM automation_stream_tasks WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
        (task_ids,),
    )
    tasks = {row["id"]: dict(row) for row in cursor.fetchall()}
    if _task_membership(tasks.values()) != _task_membership(related):
        raise HTTPException(
            409, "Related delivery tasks changed; refresh delivery review"
        )
    cursor.execute(
        "SELECT * FROM automation_export_deliveries WHERE id = %s FOR UPDATE",
        (observed["id"],),
    )
    delivery = cursor.fetchone()
    identity = ("id", "task_id", "contact_id", "destination", "status", "attempt_token")
    if not delivery or any(delivery[key] != observed[key] for key in identity):
        raise HTTPException(409, "Canonical delivery changed; refresh delivery review")
    if delivery["status"] not in (
        "sending",
        "uncertain",
        "exported",
        "failed",
        "filtered",
    ):
        raise HTTPException(409, "Canonical delivery is not awaiting review")
    if _canonical_delivery(cursor, task_id)["id"] != delivery["id"]:
        raise HTTPException(
            409, "Task now references a different delivery; refresh delivery review"
        )
    if _task_membership(
        _related_delivery_tasks(cursor, delivery, task_id)
    ) != _task_membership(tasks.values()):
        raise HTTPException(
            409, "Related delivery tasks changed; refresh delivery review"
        )
    if task_id not in tasks or delivery["task_id"] not in tasks:
        raise HTTPException(409, "Canonical delivery owner is unavailable")
    if tasks[task_id]["run_id"] != run_id or tasks[task_id]["status"] not in (
        "uncertain",
        "cancelled",
    ):
        raise HTTPException(409, "Delivery is no longer awaiting review")
    if any(
        task["step_type"] != "export" or task["contact_id"] != delivery["contact_id"]
        for task in tasks.values()
    ):
        raise HTTPException(
            409, "Canonical delivery does not match its related export tasks"
        )
    owner = tasks[delivery["task_id"]]
    if _canonical_delivery(cursor, owner["id"])["id"] != delivery["id"]:
        raise HTTPException(
            409, "Canonical delivery owner now references a different receipt"
        )
    return runs, tasks, dict(delivery)


def _resolved_task_status(task, run, owner_id, resolution):
    if task["status"] == "cancelled" or run["status"] == "cancelled":
        return "cancelled"
    if resolution == "delivered":
        return "completed" if task["id"] == owner_id else "skipped"
    if run["status"] not in ("queued", "running", "waiting_confirmation"):
        return "failed"
    return "retry"


def router(app):
    routes = APIRouter()

    def require_user(request):
        if not app._is_ui_authenticated(request):
            raise HTTPException(401, "Sign in to manage streaming runs")

    @routes.post("/api/streaming/source-tasks/claim")
    async def claim_source(request: Request):
        data = await request.json()
        count = max(1, min(10, int(data.get("limit") or 1)))
        campaign_id = int(data.get("campaign_id") or 0)
        tasks = []
        with app.get_db() as conn:
            cursor = conn.cursor()
            machine_id = app._resolve_claim_machine_id(data)
            daemon_state = "running"
            if machine_id:
                daemon_state = app._record_daemon_worker(
                    cursor,
                    machine_id,
                    str(data.get("worker_id") or machine_id),
                    "daemon",
                    data.get("worker_metadata"),
                    current_stage="source_email",
                )
            if daemon_state != "running":
                conn.commit()
                return {"tasks": [], "daemon_state": daemon_state}
            cursor.execute(
                """
                SELECT s.* FROM automation_run_steps s JOIN automation_runs r ON r.id = s.run_id
                WHERE r.execution_mode = 'streaming' AND r.status IN ('queued', 'running') AND s.step_type = 'pipeline'
                  AND (%s = 0 OR r.campaign_id = %s)
                  AND EXISTS (SELECT 1 FROM automation_stream_tasks t WHERE t.step_id = s.id AND t.step_type = 'source_email' AND t.status IN ('pending', 'retry'))
                ORDER BY r.id LIMIT 20
            """,
                (campaign_id, campaign_id),
            )
            for step in cursor.fetchall():
                found = streaming.claim(
                    cursor,
                    step["run_id"],
                    step,
                    count - len(tasks),
                    "source_email",
                    10,
                    lease_seconds=180,
                )
                tasks.extend(found)
                if len(tasks) >= count:
                    break
            conn.commit()
        return {
            "daemon_state": daemon_state,
            "tasks": [
                {
                    "id": t["id"],
                    "lease_token": t["lease_token"],
                    "contact": t["contact"],
                    "campaign_id": t["contact"]["campaign_id"],
                }
                for t in tasks
            ]
        }

    @routes.post("/api/streaming/source-tasks/{task_id}/heartbeat")
    async def heartbeat_source(task_id: int, request: Request):
        data = await request.json()
        with app.get_db() as conn:
            cursor = conn.cursor()
            machine_id = app._resolve_claim_machine_id(data)
            daemon_state = "running"
            if machine_id:
                daemon_state = app._record_daemon_worker(
                    cursor,
                    machine_id,
                    str(data.get("worker_id") or machine_id),
                    "daemon",
                    data.get("worker_metadata"),
                    current_stage="source_email",
                )
            if daemon_state == "stopped":
                conn.commit()
                return {"active": False, "daemon_state": daemon_state}
            cursor.execute(
                """
                UPDATE automation_stream_tasks t SET lease_until = CURRENT_TIMESTAMP + INTERVAL '180 seconds'
                FROM automation_runs r WHERE t.id = %s AND t.lease_token = %s AND t.run_id = r.id
                AND t.step_type = 'source_email' AND t.status = 'running'
                AND t.lease_until > clock_timestamp()
                AND r.status IN ('queued', 'running', 'waiting_confirmation')
                RETURNING t.id
            """,
                (task_id, str(data.get("lease_token") or "")),
            )
            active = bool(cursor.fetchone())
            conn.commit()
        return {"active": active, "daemon_state": daemon_state}

    @routes.post("/api/streaming/source-tasks/{task_id}/release")
    async def release_source(task_id: int, request: Request):
        data = await request.json()
        token = str(data.get("lease_token") or "")
        with app.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM automation_stream_tasks WHERE id = %s AND step_type = 'source_email' FOR UPDATE",
                (task_id,),
            )
            task = cursor.fetchone()
            if not task or task["status"] != "running" or task["lease_token"] != token:
                raise HTTPException(409, "Source task lease lost")
            ok = streaming.finish(
                cursor,
                dict(task),
                {
                    "status": "retry",
                    "error": "Daemon stop requested",
                    "count_attempt": False,
                },
            )
            conn.commit()
        if not ok:
            raise HTTPException(409, "Source task no longer running")
        return {"status": "released"}

    @routes.post("/api/streaming/source-tasks/{task_id}/complete")
    async def complete_source(task_id: int, request: Request):
        data = await request.json()
        state = data.get("status")
        if state not in ("completed", "failed"):
            raise HTTPException(400, "status must be completed or failed")
        with app.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM automation_stream_tasks WHERE id = %s AND step_type = 'source_email'",
                (task_id,),
            )
            task = cursor.fetchone()
            if (
                task
                and task["status"] in ("completed", "failed", "retry", "cancelled")
                and task.get("finished_token") == str(data.get("lease_token") or "")
            ):
                return {"status": "saved", "idempotent": True}
            if not task or task["lease_token"] != str(data.get("lease_token") or ""):
                raise HTTPException(409, "Source task lease lost")
            updates = {}
            email = str(data.get("email") or "").strip()
            if email:
                updates["email"] = email
            ok = streaming.finish(
                cursor,
                dict(task),
                {
                    "status": state,
                    "updates": updates,
                    "error": data.get("error"),
                    "result": data.get("result") or {},
                },
            )
            conn.commit()
        if not ok:
            raise HTTPException(409, "Source task no longer running")
        return {"status": "saved"}

    @routes.post("/api/funnel-runs/{run_id}/resume")
    async def resume(run_id: int, request: Request):
        require_user(request)
        with app.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM automation_runs WHERE id = %s FOR UPDATE", (run_id,)
            )
            run = cursor.fetchone()
            if (
                not run
                or run["execution_mode"] != "streaming"
                or run["status"] != "waiting_confirmation"
            ):
                raise HTTPException(409, "Streaming funnel is not waiting for recovery")
            cursor.execute(
                "SELECT 1 FROM automation_stream_tasks WHERE run_id = %s AND status = 'uncertain' LIMIT 1",
                (run_id,),
            )
            if cursor.fetchone():
                raise HTTPException(409, "Resolve uncertain deliveries before resuming")
            cursor.execute(
                "UPDATE automation_stream_tasks SET status = 'retry', available_at = CURRENT_TIMESTAMP WHERE run_id = %s AND status = 'blocked'",
                (run_id,),
            )
            cursor.execute(
                "UPDATE automation_runs SET status = 'queued', latest_error = NULL WHERE id = %s",
                (run_id,),
            )
            app._append_automation_log(
                cursor,
                run_id,
                run["campaign_id"],
                "Streaming processing resumed by user",
            )
            conn.commit()
        app._ensure_automation_run_worker(run_id)
        return {"status": "queued"}

    @routes.get("/api/funnel-runs/{run_id}/deliveries/uncertain")
    async def uncertain_deliveries(run_id: int, request: Request):
        require_user(request)
        with app.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT t.id AS task_id, t.contact_id, c.business_name, c.email, t.error,
                       d.id AS delivery_id, d.task_id AS owner_task_id, owner.run_id AS owner_run_id,
                       d.status AS delivery_status, d.destination, d.receipt
                FROM automation_stream_tasks t JOIN contacts c ON c.id = t.contact_id
                LEFT JOIN automation_export_deliveries d ON ({_CANONICAL_DELIVERY_JOIN})
                    AND d.contact_id = t.contact_id
                LEFT JOIN automation_stream_tasks owner ON owner.id = d.task_id
                WHERE t.run_id = %s AND t.step_type = 'export'
                  AND (t.status = 'uncertain'
                       OR (t.status = 'cancelled' AND d.status IN ('sending', 'uncertain')))
                ORDER BY t.id, d.id LIMIT 500
            """,
                (run_id,),
            )
            return {"deliveries": [dict(row) for row in cursor.fetchall()]}

    @routes.post("/api/funnel-runs/{run_id}/deliveries/{task_id}/resolve")
    async def resolve_delivery(run_id: int, task_id: int, request: Request):
        require_user(request)
        data = await request.json()
        resolution = data.get("resolution")
        if (
            resolution not in ("delivered", "not_delivered")
            or data.get("confirmed") is not True
        ):
            raise HTTPException(
                400,
                "Explicitly confirm delivered or not_delivered after checking the destination",
            )
        with app.get_db() as conn:
            cursor = conn.cursor()
            runs, tasks, delivery = _lock_delivery_review(cursor, run_id, task_id)
            if data.get("delivery_id") is not None and str(data["delivery_id"]) != str(
                delivery["id"]
            ):
                raise HTTPException(
                    409, "Reviewed receipt does not match the canonical delivery"
                )
            if (delivery["status"] == "exported" and resolution != "delivered") or (
                delivery["status"] in ("failed", "filtered")
                and resolution != "not_delivered"
            ):
                raise HTTPException(
                    409, "Resolution contradicts the canonical delivery outcome"
                )
            delivery_status = "exported" if resolution == "delivered" else "failed"
            cursor.execute(
                """UPDATE automation_export_deliveries
                   SET status = %s, attempt_token = NULL, updated_at = CURRENT_TIMESTAMP,
                       receipt = COALESCE(receipt, '{}'::jsonb) || jsonb_build_object(
                           'manual_resolution', %s::text, 'review_task_id', %s::bigint,
                           'resolved_at', clock_timestamp())
                   WHERE id = %s AND task_id = %s AND status = %s
                     AND attempt_token IS NOT DISTINCT FROM %s
                   RETURNING id""",
                (
                    delivery_status,
                    resolution,
                    task_id,
                    delivery["id"],
                    delivery["task_id"],
                    delivery["status"],
                    delivery["attempt_token"],
                ),
            )
            if cursor.rowcount != 1:
                raise HTTPException(409, "Canonical delivery could not be resolved")
            affected_runs = set()
            for task in tasks.values():
                status = _resolved_task_status(
                    task, runs[task["run_id"]], delivery["task_id"], resolution
                )
                cursor.execute(
                    """UPDATE automation_stream_tasks
                       SET status = %s, dispatched = FALSE, available_at = CURRENT_TIMESTAMP,
                           error = %s, lease_token = NULL, lease_until = NULL, finished_token = NULL,
                           result = COALESCE(result, '{}'::jsonb) || jsonb_build_object(
                               'delivery_id', %s::bigint, 'resolution', %s::text, 'delivery_status', %s::text),
                           updated_at = CURRENT_TIMESTAMP
                       WHERE id = %s AND run_id = %s AND status = %s RETURNING id""",
                    (
                        status,
                        f"User confirmed {resolution}",
                        delivery["id"],
                        resolution,
                        delivery_status,
                        task["id"],
                        task["run_id"],
                        task["status"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise HTTPException(
                        409, "Related export task could not be resolved"
                    )
                affected_runs.add(task["run_id"])
            for affected_run in sorted(affected_runs):
                app._append_automation_log(
                    cursor,
                    affected_run,
                    runs[affected_run]["campaign_id"],
                    f"Delivery #{delivery['id']} for contact #{delivery['contact_id']} resolved: {resolution}",
                )
            conn.commit()
        return {
            "status": "resolved",
            "delivery_id": delivery["id"],
            "owner_task_id": delivery["task_id"],
            "task_ids": sorted(tasks),
        }

    return routes
