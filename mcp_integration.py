"""Mount the remote MCP using the same campaign services as the web UI."""

import os
from contextlib import asynccontextmanager

import streaming
import streaming_campaigns


def hooks(host):
    from fastapi import HTTPException

    from remote_mcp import ServiceHooks
    from remote_mcp.contracts import DraftError

    def prepare(cursor, payload):
        try:
            return streaming_campaigns.prepare(cursor, host, payload)
        except HTTPException as exc:
            if (
                exc.status_code == 400
                and exc.detail == streaming_campaigns.NEW_CAMPAIGN_PIPELINE_REQUIRED
            ):
                raise DraftError(
                    streaming_campaigns.NEW_CAMPAIGN_PIPELINE_REQUIRED
                ) from None
            raise

    def launch(cursor, plan):
        try:
            return streaming_campaigns.launch(cursor, host, plan)
        except HTTPException as exc:
            if exc.status_code == 404 and exc.detail == "Funnel template not found":
                raise DraftError(
                    "Prepared funnel no longer exists; "
                    "prepare a new preview with an existing funnel"
                ) from None
            raise

    def list_templates(cursor, kind):
        table = {
            "source": "source_templates",
            "enrichment": "enrichment_templates",
            "funnel": "automation_funnel_templates",
            "export": "export_templates",
            "verification": "email_verification_templates",
        }[kind]
        cursor.execute(f"SELECT * FROM {table} ORDER BY name")
        rows = [dict(row) for row in cursor.fetchall()]
        if kind == "source":
            rows.insert(
                0,
                {
                    "id": None,
                    "name": "Google Maps (built-in)",
                    "source_type": "builtin_google_maps",
                    "enabled": True,
                },
            )
        return rows

    def run_status(cursor, run_id):
        row = host._load_automation_run(cursor, run_id)
        if not row:
            return {"status": "unknown", "run_id": run_id}
        result = {
            "run_id": run_id,
            "campaign_id": row["campaign_id"],
            "status": row["status"],
            "execution_mode": row["execution_mode"],
        }
        if row["execution_mode"] == "streaming":
            result["stream_progress"] = streaming.progress(cursor, run_id)
            result["source_closed"] = row["stream_source_closed"]
        else:
            steps = host._load_automation_steps(cursor, run_id)
            result.update(
                total_steps=len(steps),
                completed_steps=sum(s["status"] == "completed" for s in steps),
            )
        return result

    def campaign_status(cursor, campaign_id):
        cursor.execute(
            "SELECT status FROM search_campaigns WHERE id = %s", (campaign_id,)
        )
        row = cursor.fetchone()
        if not row:
            return {"campaign_id": campaign_id, "status": "unknown"}
        result = {"campaign_id": campaign_id, "status": row["status"]}
        cursor.execute(
            """
            SELECT COUNT(*) AS total_requests,
                COUNT(*) FILTER (WHERE status = 'completed') AS completed_requests,
                COUNT(*) FILTER (WHERE status = 'failed') AS failed_requests,
                COUNT(*) FILTER (WHERE status IN ('pending', 'inuse', 'reserved'))
                    AS pending_requests
            FROM requests WHERE campaign_id = %s
        """,
            (campaign_id,),
        )
        result.update(dict(cursor.fetchone()))
        cursor.execute(
            "SELECT COUNT(*) AS total_contacts FROM contacts WHERE campaign_id = %s",
            (campaign_id,),
        )
        result.update(dict(cursor.fetchone()))
        cursor.execute(
            "SELECT id FROM automation_runs WHERE campaign_id = %s "
            "ORDER BY id DESC LIMIT 1",
            (campaign_id,),
        )
        run = cursor.fetchone()
        if run:
            result.update(run_status(cursor, run["id"]))
        return result

    def stop(cursor, campaign_id):
        cursor.execute(
            "SELECT * FROM automation_runs WHERE campaign_id = %s "
            "AND status IN ('queued', 'running', 'waiting_confirmation') "
            "ORDER BY id FOR UPDATE",
            (campaign_id,),
        )
        for run in cursor.fetchall():
            host._cancel_automation_run(
                cursor, dict(run), "Campaign stopped through MCP"
            )
        cursor.execute(
            "UPDATE search_campaigns SET status = 'inactive', daemon_ignore = TRUE "
            "WHERE id = %s",
            (campaign_id,),
        )
        return {
            "campaign_id": campaign_id,
            "status": "stopping",
            "stop_requested": True,
        }

    def wake(result):
        host._ensure_automation_run_worker(result["run_id"])

    return ServiceHooks(
        prepare=prepare,
        launch=launch,
        after_commit=wake,
        list_templates=list_templates,
        get_campaign_status=campaign_status,
        get_run_status=run_status,
        stop_campaign=stop,
    )


def install(host):
    if not os.environ.get("MAPSDATA_MCP_TOKEN"):
        return
    from remote_mcp import CampaignService, MCPSettings, create_mcp_server

    remote = create_mcp_server(
        CampaignService(host.get_db, hooks(host)), MCPSettings.from_env()
    )
    previous = host.app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app):
        async with previous(app), remote.lifespan():
            yield

    host.app.router.lifespan_context = lifespan
    host.app.mount("/mcp", remote.app)
