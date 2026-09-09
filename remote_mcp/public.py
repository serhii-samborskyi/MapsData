"""Allowlisted client views of credential-bearing plans and worker results."""

import re
from urllib.parse import parse_qsl, urlsplit

IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
SENSITIVE_KEY = re.compile(
    r"key|token|secret|password|authorization|credential|cookie|header", re.I
)
STEPS = {
    "pipeline",
    "source_email",
    "enrichment",
    "dns_check",
    "email_verification",
    "export",
}
COUNTS = {
    "pending",
    "queued",
    "running",
    "retry",
    "blocked",
    "uncertain",
    "completed",
    "failed",
    "skipped",
    "cancelled",
    "canceled",
    "exported",
    "total",
}
FILTERS = (
    "export_valid_only",
    "export_catch_all",
    "export_catch_all_only",
    "exclude_public_emails",
    "export_domain_ok",
)


def template_metadata(row: dict, kind: str) -> dict:
    result = {
        "kind": kind,
        "id": row["id"],
        "name": row["name"],
        "enabled": row.get("enabled", True),
        "configuration_redacted": True,
    }
    for key in ("source_type", "service"):
        value = row.get(key)
        if isinstance(value, str) and IDENTIFIER.fullmatch(value):
            result[key] = value
    if row.get("execution_mode") in ("batch", "streaming"):
        result["execution_mode"] = row["execution_mode"]
    return result


def stream_progress(rows) -> list[dict]:
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows[:32]:
        if not isinstance(row, dict) or row.get("step_type") not in STEPS:
            continue
        item = {"step_type": row["step_type"]}
        for key in ("step_order", *sorted(COUNTS)):
            value = row.get(key)
            if type(value) is int and 0 <= value <= 2**53 - 1:
                item[key] = value
        result.append(item)
    return result


def _secrets(value) -> set[str]:
    found = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if SENSITIVE_KEY.search(key):
                found.update(_strings(item))
            found.update(_secrets(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_secrets(item))
    elif isinstance(value, str) and "://" in value:
        try:
            parsed = urlsplit(value)
            found.update(v for _, v in parse_qsl(parsed.query) if v)
            found.update(v for v in (parsed.username, parsed.password) if v)
        except ValueError:
            found.add(value)
    return found


def _strings(value) -> set[str]:
    if isinstance(value, str):
        parts = {value} if value else set()
        if value.lower().startswith("bearer "):
            parts.add(value[7:])
        return parts
    if isinstance(value, (dict, list)):
        return {
            s
            for item in (value.values() if isinstance(value, dict) else value)
            for s in _strings(item)
        }
    return set()


def _display(value, secrets: set[str]):
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 2000:
        return "[REDACTED]"
    if (
        "://" in value
        or value.lower().startswith("bearer ")
        or any(secret in value for secret in secrets)
    ):
        return "[REDACTED]"
    return value


def export_preview(plan: dict) -> dict | None:
    step = next(
        (s for s in plan["steps"] if s.get("enabled", True) and s["type"] == "export"),
        None,
    )
    if step is None:
        return None
    config = step["config"]
    snapshot = config["template_snapshot"]
    secrets = _secrets(plan)
    # This existing helper resolves local frozen settings only; no provider IO.
    from streaming_export import resolve_destination

    override = dict(config.get("destination") or {})
    if config.get("sendread_ab_list_id") is not None:
        override["list_id"] = config["sendread_ab_list_id"]
    destination = resolve_destination(snapshot, override)
    public_destination = {
        key: _display(destination[key], secrets)
        for key in ("service", "target_type", "target_id", "newListName")
        if key in destination
    }
    if type(destination.get("skipExistingFromOtherCampaigns")) is bool:
        public_destination["skipExistingFromOtherCampaigns"] = destination[
            "skipExistingFromOtherCampaigns"
        ]
    filters = {
        key: config.get("filters", {}).get(key, config.get(key, False))
        for key in FILTERS
    }
    if any(type(value) is not bool for value in filters.values()):
        raise ValueError("Frozen export filters must be booleans")
    mapping = config.get("field_mappings", snapshot.get("field_mappings", {}))
    fields = {
        key: "[REDACTED]" if SENSITIVE_KEY.search(key) else _display(value, secrets)
        for key, value in mapping.items()
        if isinstance(key, str) and IDENTIFIER.fullmatch(key)
    }
    return {
        "template_id": snapshot["id"],
        "template_name": snapshot["name"],
        "destination": public_destination,
        "filters": filters,
        "field_mappings": fields,
        "require_confirmation": config.get("require_confirmation", False) is True,
        "sendread_ab_list_id": _display(config.get("sendread_ab_list_id"), secrets),
    }
