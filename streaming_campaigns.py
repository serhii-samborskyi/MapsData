"""Campaign planning and immutable settings shared by the UI and remote MCP."""

import copy
import json

from fastapi import HTTPException
from psycopg2.extras import Json

import streaming

NEW_CAMPAIGN_PIPELINE_REQUIRED = (
    "New campaigns require a funnel with an enabled pipeline step; "
    "select a sourcing funnel and prepare a new preview"
)


def execution_mode(value, default="batch"):
    mode = str(value or default).strip().lower()
    if mode not in ("batch", "streaming"):
        raise HTTPException(400, "execution_mode must be batch or streaming")
    return mode


def parse_template(row):
    item = dict(row)
    for key in (
        "api_config",
        "input_mapping",
        "output_mapping",
        "schema_cache",
        "status_mapping",
        "field_mappings",
        "config",
    ):
        if isinstance(item.get(key), str):
            item[key] = json.loads(item[key] or "{}")
    return streaming.clean_json(item)


def freeze_steps(cursor, app, template, overrides=None):
    overrides = overrides or {}
    steps = app._normalize_funnel_steps(
        template.get("steps")
        if isinstance(template.get("steps"), list)
        else json.loads(template.get("steps") or "[]")
    )
    seen = set()
    order = {
        "pipeline": 0,
        "enrichment": 1,
        "dns_check": 2,
        "email_verification": 3,
        "export": 4,
    }
    previous = -1
    for step in steps:
        if not step["enabled"]:
            continue
        kind = step["type"]
        if kind in seen or order[kind] < previous:
            raise HTTPException(
                400,
                "Streaming funnel steps must be unique and in pipeline, "
                "enrichment, DNS, verification, export order",
            )
        seen.add(kind)
        previous = order[kind]
        config = dict(step["config"])
        if kind == "pipeline":
            step["config"] = config
            continue
        if kind == "export" and overrides.get("export_template_id"):
            config["template_id"] = int(overrides["export_template_id"])
        template_id = app._safe_int(config.get("template_id"), 0)
        table = {
            "enrichment": "enrichment_templates",
            "dns_check": "email_verification_templates",
            "email_verification": "email_verification_templates",
            "export": "export_templates",
        }[kind]
        cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (template_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(400, f"{kind} template #{template_id} not found")
        snapshot = parse_template(row)
        config["template_snapshot"] = snapshot
        if kind == "enrichment" and snapshot.get("service") != "prompt_http":
            snapshot.setdefault("api_config", {}).setdefault(
                "api_url", app.DEFAULT_ENRICHMENT_API_URL
            )
        if kind == "enrichment" and snapshot.get("service") == "prompt_http":
            app._prompt_template_config(snapshot)
        if kind in ("dns_check", "email_verification"):
            is_dns = app._is_domain_checker_template(snapshot)
            if is_dns != (kind == "dns_check"):
                raise HTTPException(
                    400, f"Incorrect verification template type for {kind}"
                )
        if kind == "export":
            import streaming_export

            if overrides.get("sendread_ab_list_id"):
                if snapshot.get("service") not in (
                    "sendread_campaign",
                    "sendread_list",
                ):
                    raise HTTPException(
                        400, "A/B list override requires a SendRead export template"
                    )
                config["sendread_ab_list_id"] = str(
                    overrides["sendread_ab_list_id"]
                ).strip()
            if config.get("field_mappings") is None:
                config["field_mappings"] = copy.deepcopy(
                    snapshot.get("field_mappings") or {}
                )
            if "batch_size" not in config or int(config["batch_size"]) > 500:
                config["batch_size"] = 50
            config["batch_size"] = max(1, min(500, int(config["batch_size"])))
            try:
                # Freeze the effective values in the fields the batch worker
                # consumes, as well as the structured streaming configuration.
                filters = config.get("filters", {})
                if not isinstance(filters, dict):
                    raise ValueError("filters must be an object")
                for key in streaming_export.FILTER_KEYS:
                    value = filters.get(key, config.get(key, False))
                    if type(value) is not bool:
                        raise ValueError(f"{key} must be a boolean")
                    config[key] = value
                destination = dict(config.get("destination") or {})
                if "newListName" in config:
                    destination["newListName"] = config["newListName"]
                if config.get("sendread_ab_list_id") is not None:
                    destination["list_id"] = config["sendread_ab_list_id"]
                resolved = streaming_export.resolve_destination(snapshot, destination)
                config["destination"] = resolved
                api = snapshot.setdefault("api_config", {})
                if resolved["service"].startswith("sendread"):
                    api["sendread_target_id"] = resolved["target_id"]
                    api["sendread_target_type"] = resolved["target_type"]
                    if "skipExistingFromOtherCampaigns" in resolved:
                        api["skipExistingFromOtherCampaigns"] = resolved[
                            "skipExistingFromOtherCampaigns"
                        ]
                else:
                    api[f"{resolved['service']}_campaign_id"] = resolved["target_id"]
                if "newListName" in resolved:
                    config["newListName"] = resolved["newListName"]
            except ValueError as exc:
                raise HTTPException(400, str(exc)) from exc
        else:
            import streaming_services

            try:
                streaming_services.limits(kind, config)
            except ValueError as exc:
                raise HTTPException(400, f"{kind}: {exc}") from exc
        step["config"] = config
    if not seen:
        raise HTTPException(400, "Funnel has no enabled steps")
    return steps


def prepare(cursor, app, payload):
    name = str(payload.get("name") or "").strip()
    phrases = payload.get("requests", payload.get("search_phrases", []))
    if isinstance(phrases, str):
        phrases = phrases.splitlines()
    if not isinstance(phrases, list) or any(not isinstance(p, str) for p in phrases):
        raise HTTPException(400, "requests must be an array of search strings")
    phrases = list(dict.fromkeys(p.strip() for p in phrases if p.strip()))
    if not name or not phrases:
        raise HTTPException(400, "Campaign name and at least one request are required")
    if len(phrases) > 20000 or any(len(p) > 4000 for p in phrases):
        raise HTTPException(
            400,
            "Campaign request limit exceeded (20000 requests, 4000 characters each)",
        )
    source_id = app._normalize_source_template_id(payload.get("source_template_id"))
    source = app._ensure_source_template(cursor, source_id)
    cursor.execute(
        "SELECT * FROM automation_funnel_templates WHERE id = %s AND enabled = TRUE",
        (app._safe_int(payload.get("funnel_template_id"), 0),),
    )
    template = cursor.fetchone()
    if not template:
        raise HTTPException(400, "Select an enabled funnel template")
    template = streaming.clean_json(dict(template))
    mode = execution_mode(
        payload.get("execution_mode"), template.get("execution_mode", "batch")
    )
    steps = freeze_steps(cursor, app, template, payload)
    if not any(step["enabled"] and step["type"] == "pipeline" for step in steps):
        raise HTTPException(400, NEW_CAMPAIGN_PIPELINE_REQUIRED)
    export = next((s for s in steps if s["enabled"] and s["type"] == "export"), None)
    mode_scrape = str(payload.get("maps_scrape_mode") or "slow")
    if mode_scrape not in ("fast", "slow"):
        raise HTTPException(400, "Invalid maps_scrape_mode")
    return {
        "name": name,
        "requests": phrases,
        "source_template_id": source_id,
        "source_snapshot": source
        or {"source_type": "builtin_google_maps", "name": "Google Maps", "config": {}},
        "funnel_template_id": template["id"],
        "funnel_name": template["name"],
        "execution_mode": mode,
        "steps": steps,
        "default_retry_count": template["default_retry_count"],
        "maps_scrape_mode": mode_scrape,
        "scrape_maps_only": bool(payload.get("scrape_maps_only", False)),
        "request_count": len(phrases),
        "export": {
            "template_id": export["config"].get("template_id"),
            "template_name": export["config"]["template_snapshot"]["name"],
            "sendread_ab_list_id": export["config"].get("sendread_ab_list_id"),
        }
        if export
        else None,
    }


def launch(cursor, app, plan):
    cursor.execute(
        """
        INSERT INTO search_campaigns
            (name, status, maps_scrape_mode, source_template_id,
             scrape_maps_only, source_snapshot)
        VALUES (%s, 'active', %s, %s, %s, %s) RETURNING id
    """,
        (
            plan["name"],
            plan["maps_scrape_mode"],
            plan["source_template_id"],
            plan["scrape_maps_only"],
            Json(plan["source_snapshot"]),
        ),
    )
    campaign_id = cursor.fetchone()["id"]
    cursor.executemany(
        "INSERT INTO requests(campaign_id, req_text, status) "
        "VALUES (%s, %s, 'pending')",
        [(campaign_id, text) for text in plan["requests"]],
    )
    run_id = app._create_automation_run(
        cursor,
        campaign_id,
        plan["funnel_template_id"],
        "mcp",
        overrides={"execution_mode": plan["execution_mode"]},
        frozen_plan=plan,
    )
    return {
        "campaign_id": campaign_id,
        "run_id": run_id,
        "execution_mode": plan["execution_mode"],
    }
