"""Export explicit contacts without querying campaigns or changing templates.

``export_batch(snapshot, contacts, destination)`` is synchronous. Snapshot is a
frozen template (service/api_config/field_mappings), or an object containing
``template_snapshot``, optional ``field_mappings``, ``filters``, and
``request_city_map``. ``template`` is also accepted as a snapshot wrapper.
Filter keys are the existing export_* toggles and exclude_public_emails. They
may also be supplied at snapshot's top level. Destination accepts target_id /
target_type, the existing provider configuration keys, or SendRead list_id.
The engine's ``sendread_ab_list_id`` option is accepted directly on config.

The result contains ordered receipts with contact_id, input_index, status,
attempted, retryable, error, and provider_response. Status is exported, filtered,
failed, or unknown. Errors contain code/message/http_status/retry_after_seconds.
There is at most one HTTP request per batch, after per-contact validation.
Consistent all-accepted counts confirm the batch; partial counts without
identifiable item results are unknown. The caller owns durable receipts,
leases, throttling, retry scheduling, and reconciliation.

SendRead's MCP push tools advertise idempotentHint, but the inspected public
POST schemas expose no idempotency key. Pushes upsert leads and can schedule
sends; this is not an exactly-once guarantee. Unknown outcomes must not be
retried automatically. This module performs no retries or provider lookups.
"""

import json
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests

from templates import (
    ManyReachIntegration,
    SendReadIntegration,
    SmartLeadIntegration,
    extract_city_from_address,
)

PUBLIC_EMAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "icloud.com",
        "aol.com",
        "mail.com",
        "proton.me",
        "protonmail.com",
        "live.com",
        "msn.com",
        "gmx.com",
        "zoho.com",
        "yandex.com",
        "yandex.ru",
        "mail.ru",
        "fastmail.com",
        "tutanota.com",
        "hushmail.com",
        "qq.com",
        "126.com",
        "163.com",
    }
)

# SELECT * supplies these nullable columns to legacy transforms. Padding sparse
# rows prevents the existing resolver from treating missing column names as text.
CONTACT_FIELDS = frozenset(
    [
        "id",
        "address",
        "business_name",
        "campaign_id",
        "category",
        "domain",
        "email",
        "facebook",
        "instagram",
        "phone",
        "place_id",
        "rating",
        "request_id",
        "review_count",
        "twitter",
        "yelp",
        "status",
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
        "nomail_pulled_at",
    ]
) | {f"custom_{index}" for index in range(1, 21)}

FILTER_KEYS = (
    "export_valid_only",
    "export_catch_all",
    "export_catch_all_only",
    "exclude_public_emails",
    "export_domain_ok",
)


def matches_export_status_filter(
    contact, valid_only, include_catch_all, catch_all_only
):
    """Match the legacy email-status predicate, including status fallback."""
    status = contact.get("email_status")
    if status is None or not str(status).strip():
        status = contact.get("status")
    normalized = "".join(ch for ch in str(status or "").lower() if ch.isalpha())
    is_valid = normalized.startswith("valid") or normalized == "verified"
    is_catch_all = "catchall" in normalized
    if catch_all_only:
        return is_catch_all
    if valid_only:
        return is_valid or (include_catch_all and is_catch_all)
    return True


def contact_filter_reason(contact, filters):
    """Return a stable exclusion code, or None when eligible."""
    email = str(contact.get("email") or "")
    if not email.strip():
        return "missing_email"
    # Match the export SQL lower(split_part(email, '@', 2)) expression exactly.
    domain = email.split("@")[1].lower() if "@" in email else ""
    if filters.get("exclude_public_emails") and domain in PUBLIC_EMAIL_DOMAINS:
        return "public_email"
    if filters.get("export_domain_ok") and contact.get("domain_status") != "Domain OK":
        return "domain_status"
    if not matches_export_status_filter(
        contact,
        filters.get("export_valid_only", False),
        filters.get("export_catch_all", False),
        filters.get("export_catch_all_only", False),
    ):
        return "email_status"
    return None


def _filters(config):
    filters = {key: config.get(key, False) for key in FILTER_KEYS}
    filters.update(_object(config.get("filters", {}), "filters"))
    for key in FILTER_KEYS:
        if not isinstance(filters.get(key), bool):
            raise ValueError(f"{key} must be a boolean")
    return filters


def eligibility(contact, config, main):
    """Engine predicate using the application's existing status helper."""
    filters = _filters(config)
    without_status = {
        key: filters[key] for key in ("exclude_public_emails", "export_domain_ok")
    }
    return contact_filter_reason(
        contact, without_status
    ) is None and main._matches_export_status_filter(
        contact,
        filters["export_valid_only"],
        filters["export_catch_all"],
        filters["export_catch_all_only"],
    )


def _object(value, name):
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return deepcopy(dict(value))


def _template(config):
    return _object(
        config.get("template_snapshot", config.get("template", config)),
        "template_snapshot",
    )


def _destination(config, override=None):
    result = _object(config.get("destination", {}), "destination")
    if config.get("sendread_ab_list_id") is not None:
        result["list_id"] = config["sendread_ab_list_id"]
    if override is not None:
        result.update(_object(override, "destination"))
        # An explicit call-level target takes precedence over the run default.
        if "target_id" in override or "sendread_target_id" in override:
            result.pop("list_id", None)
    return result


def resolve_destination(template, destination=None):
    """Resolve only routing overrides, without mutating the frozen template."""
    service = template.get("service")
    config = _object(template.get("api_config", {}), "api_config")
    override = _object(destination, "destination") if destination is not None else {}
    if override.get("service", service) != service:
        raise ValueError("destination service must match the frozen template")
    if service in ("sendread_list", "sendread_campaign"):
        default_type = "ab_test_list" if service == "sendread_list" else "campaign"
        target_type = override.get(
            "target_type",
            override.get(
                "sendread_target_type",
                config.get("sendread_target_type") or default_type,
            ),
        )
        target_id = override.get(
            "target_id",
            override.get(
                "sendread_target_id",
                config.get("sendread_target_id"),
            ),
        )
        list_key = next(
            (key for key in ("list_id", "ab_list_id", "listId") if key in override),
            None,
        )
        if list_key:
            target_id, target_type = override[list_key], "ab_test_list"
        target_type = str(target_type or "").strip().lower()
        if target_type not in ("campaign", "ab_test_list"):
            raise ValueError("Invalid SendRead target type")
    elif service in ("manyreach", "smartlead"):
        if any(key in override for key in ("list_id", "ab_list_id", "listId")):
            raise ValueError("A/B list override requires a SendRead template")
        target_type = "campaign"
        key = f"{service}_campaign_id"
        target_id = override.get("target_id", override.get(key, config.get(key)))
    else:
        raise ValueError("Export service not supported")
    target_id = str(target_id if target_id is not None else "").strip()
    if not target_id:
        raise ValueError("Export destination ID is required")
    result = {"service": service, "target_type": target_type, "target_id": target_id}
    if service.startswith("sendread"):
        skip_existing = override.get(
            "skipExistingFromOtherCampaigns",
            config.get("skipExistingFromOtherCampaigns"),
        )
        if skip_existing is not None:
            if not isinstance(skip_existing, bool):
                raise ValueError("skipExistingFromOtherCampaigns must be a boolean")
            result["skipExistingFromOtherCampaigns"] = skip_existing
    if service == "manyreach":
        result["newListName"] = override.get("newListName", "")
    return result


def _integration(template):
    config = _object(template.get("api_config", {}), "api_config")
    integration_class = {
        "sendread_list": SendReadIntegration,
        "sendread_campaign": SendReadIntegration,
        "smartlead": SmartLeadIntegration,
        "manyreach": ManyReachIntegration,
    }.get(template.get("service"))
    if integration_class is None:
        raise ValueError("Export service not supported")
    return integration_class(
        str(config.get("api_key") or "").strip(),
        config.get("api_base_url") or config.get("base_url"),
    )


def destination_key(config):
    """Stable, credential-free key for the engine's unique delivery constraint.

    Raises ValueError for invalid configuration, before reserving a delivery.
    Mapping/filter changes do not create a new destination.
    """
    template = _template(config)
    destination = resolve_destination(template, _destination(config))
    provider = (
        "sendread"
        if destination["service"].startswith("sendread")
        else destination["service"]
    )
    return json.dumps(
        {
            "provider": provider,
            "base_url": _integration(template).base_url,
            "target_type": destination["target_type"],
            "target_id": destination["target_id"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def limits(config):
    """Provider request budget; caller schedules requests and handles Retry-After."""
    template = _template(config)
    integration = _integration(template)
    rpm = integration.rate_limit
    provider = (
        "sendread"
        if template["service"].startswith("sendread")
        else template["service"]
    )
    return {
        "endpoint_key": f"{provider}:{integration.base_url}",
        "requests_per_minute": rpm,
        "min_interval_seconds": 60 / rpm,
        "max_contacts_per_call": getattr(integration, "max_leads_per_request", 500),
        "timeout_seconds": 30,
    }


def send_batch(config, contacts):
    """Engine facade returning only per-contact receipts; never persists them."""
    return export_batch(config, contacts)["receipts"]


def _prepare_contact(contact, city_map):
    result = dict.fromkeys(CONTACT_FIELDS)
    result["source_data"] = {}
    result.update(deepcopy(dict(contact)))
    if not str(result.get("city") or "").strip():
        city = extract_city_from_address(
            result.get("address") or result.get("__address_fallback")
        )
        request_id = result.get("request_id")
        if request_id is None:
            request_id = result.get("__request_id_fallback")
        try:
            request_id = int(request_id)
        except (TypeError, ValueError):
            request_id = None
        city = city or city_map.get(str(request_id)) or city_map.get(request_id)
        if city:
            result["city"] = result["__request_city"] = city
    return result


def _error(code, message, http_status=None, retry_after_seconds=None):
    return {
        "code": code,
        "message": message,
        "http_status": http_status,
        "retry_after_seconds": retry_after_seconds,
    }


def _receipt(
    index,
    contact,
    status,
    *,
    attempted=False,
    retryable=False,
    error=None,
    response=None,
):
    return {
        "input_index": index,
        "contact_id": contact.get("id", contact.get("contact_id"))
        if isinstance(contact, Mapping)
        else None,
        "status": status,
        "attempted": attempted,
        "retryable": retryable,
        "error": error,
        "provider_response": response,
    }


def _retry_after(headers):
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        try:
            when = parsedate_to_datetime(str(value))
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            return max(0, (when - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _redact(value, api_key):
    if isinstance(value, str):
        return value.replace(api_key, "[redacted]") if api_key else value
    if isinstance(value, dict):
        return {key: _redact(item, api_key) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, api_key) for item in value]
    return value


def _post_batch(integration, destination, leads, config):
    target = quote(destination["target_id"], safe="")
    service = destination["service"]
    kwargs = {"timeout": 30, "allow_redirects": False}
    if service.startswith("sendread"):
        collection = (
            "campaigns" if destination["target_type"] == "campaign" else "ab-test-lists"
        )
        url = f"{integration.base_url}/api/public/{collection}/{target}/leads"
        payload = {"leads": leads}
        if "skipExistingFromOtherCampaigns" in destination:
            payload["skipExistingFromOtherCampaigns"] = destination[
                "skipExistingFromOtherCampaigns"
            ]
        kwargs.update(json=payload, headers=integration._auth_headers())
    elif service == "smartlead":
        url = f"{integration.base_url}/campaigns/{target}/leads"
        payload = {"lead_list": leads}
        settings = integration._sanitize_settings(config.get("settings"))
        if settings:
            payload["settings"] = settings
        kwargs.update(json=payload, params={"api_key": integration.api_key})
    else:
        url = f"{integration.base_url}/api/campaigns/prospects/add/bulk"
        params = {"apikey": integration.api_key, "campaignid": destination["target_id"]}
        if destination.get("newListName"):
            params["newListName"] = destination["newListName"]
        kwargs.update(
            json=leads, params=params, headers={"apiKey": integration.api_key}
        )
    kwargs.setdefault("headers", {}).update(
        {"Content-Type": "application/json", "Accept": "application/json"}
    )
    return requests.post(url, **kwargs)


def _response_outcome(response, service, api_key, batch_count):
    status = response.status_code
    try:
        body = _redact(response.json(), api_key)
    except ValueError:
        body = None
    retry_after = _retry_after(response.headers)
    if status == 429:
        return (
            "failed",
            True,
            _error("rate_limited", "Provider rate limit exceeded", status, retry_after),
            body,
        )
    if status == 408 or status >= 500:
        return (
            "unknown",
            False,
            _error(
                "ambiguous_response",
                "Provider may have accepted the contact; reconcile before retrying",
                status,
                retry_after,
            ),
            body,
        )
    if status not in (200, 201):
        code = "provider_validation" if status in (400, 422) else "provider_error"
        return (
            "failed",
            False,
            _error(code, f"Provider rejected the request (HTTP {status})", status),
            body,
        )
    if not isinstance(body, dict):
        return (
            "unknown",
            False,
            _error("invalid_response", "Provider returned no usable receipt", status),
            body,
        )
    if (
        body.get("error")
        or body.get("errors")
        or body.get("success") is False
        or body.get("ok") is False
    ):
        if body.get("created") or body.get("updated"):
            return (
                "unknown",
                False,
                _error(
                    "partial_response",
                    "Provider reported both acceptance and errors",
                    status,
                ),
                body,
            )
        return (
            "failed",
            False,
            _error("provider_validation", "Provider reported an error", status),
            body,
        )
    if service.startswith("sendread"):
        # assigned includes conflicts in SendRead, and scheduled is not an
        # acceptance count. Only created + updated confirms all submitted rows.
        counts = [
            body.get(key, 0)
            for key in ("created", "updated", "skippedBlocked", "skippedConflicts")
        ]
        if (
            all(type(value) is int and value >= 0 for value in counts)
            and body.get("total") == batch_count
        ):
            created, updated, blocked, conflicts = counts
            if created + updated == batch_count and blocked + conflicts == 0:
                return "exported", False, None, body
            if created + updated == 0 and blocked + conflicts == batch_count:
                reason = (
                    "provider_skipped"
                    if blocked and conflicts
                    else "provider_blocked"
                    if blocked
                    else "provider_conflict"
                )
                return (
                    "filtered",
                    False,
                    _error(reason, "Provider skipped this contact", status),
                    body,
                )
        return (
            "unknown",
            False,
            _error(
                "unconfirmed_response",
                "Provider counts do not confirm this contact's outcome",
                status,
            ),
            body,
        )
    return "exported", False, None, body


def _item_outcomes(body, leads, http_status):
    """Use explicit email-addressed receipts when a provider supplies them."""
    if not isinstance(body, dict):
        return None
    items = body.get("results", body.get("leads"))
    if not isinstance(items, list):
        return None
    by_email = {}
    for item in items:
        if not isinstance(item, dict) or not item.get("email"):
            continue
        email = str(item["email"]).strip().lower()
        by_email.setdefault(email, []).append(item)
    outcomes = []
    for lead in leads:
        matches = by_email.get(str(lead.get("email") or "").strip().lower(), [])
        error = _error(
            "unconfirmed_response",
            "No unambiguous per-contact provider receipt",
            http_status,
        )
        outcome = ("unknown", False, error, body)
        if len(matches) == 1:
            item = matches[0]
            state = str(item.get("status") or "").lower()
            if (
                item.get("error")
                or item.get("errors")
                or item.get("success") is False
                or state in ("failed", "invalid", "rejected")
            ):
                outcome = (
                    "failed",
                    False,
                    _error(
                        "provider_validation",
                        "Provider rejected this contact",
                        http_status,
                    ),
                    item,
                )
            elif item.get("success") is True or state in (
                "created",
                "updated",
                "accepted",
                "exported",
                "success",
            ):
                outcome = ("exported", False, None, item)
            elif state in ("skipped", "blocked", "conflict"):
                outcome = (
                    "filtered",
                    False,
                    _error(
                        "provider_skipped", "Provider skipped this contact", http_status
                    ),
                    item,
                )
        outcomes.append(outcome)
    return outcomes


def export_batch(snapshot, contacts, destination=None):
    """Attempt only supplied contacts and return JSON-serializable receipts.

    No contact/template/database lookup occurs. Configuration errors become
    unattempted failed receipts plus a top-level error. One HTTP request contains
    every locally eligible and valid contact. Never retry an attempted unknown
    receipt without external reconciliation. Oversize batches are rejected before
    sending; the parent must split them using limits()['max_contacts_per_call'].
    """
    contacts = list(contacts)
    receipts = [None] * len(contacts)
    if not contacts:
        return {"receipts": [], "destination": None, "requests_made": 0, "error": None}
    try:
        snapshot = _object(snapshot, "snapshot")
        template = _template(snapshot)
        config = _object(template.get("api_config", {}), "api_config")
        resolved = resolve_destination(template, _destination(snapshot, destination))
        filters = _filters(snapshot)
        mappings = _object(
            snapshot.get("field_mappings", template.get("field_mappings", {})),
            "field_mappings",
        )
        if any(
            not isinstance(key, str)
            or (value is not None and not isinstance(value, str))
            for key, value in mappings.items()
        ):
            raise ValueError("field_mappings must map string keys to strings or null")
        city_map = _object(snapshot.get("request_city_map", {}), "request_city_map")
        api_key = str(config.get("api_key") or "").strip()
        if not api_key:
            raise ValueError("Export API key is required")
        service = resolved["service"]
        integration = _integration(template)
        if len(contacts) > limits(snapshot)["max_contacts_per_call"]:
            raise ValueError("Export batch exceeds max_contacts_per_call")
        if service == "manyreach" and not resolved.get("newListName"):
            resolved["newListName"] = snapshot.get("newListName", "")
    except (TypeError, ValueError) as exc:
        error = _error("configuration", str(exc))
        return {
            "receipts": [
                _receipt(index, contact, "failed", error=error)
                for index, contact in enumerate(contacts)
            ],
            "destination": None,
            "requests_made": 0,
            "error": error,
        }

    pending = []
    leads = []
    for index, contact in enumerate(contacts):
        if not isinstance(contact, Mapping):
            receipts[index] = _receipt(
                index,
                contact,
                "failed",
                error=_error("validation", "Contact must be an object"),
            )
            continue
        reason = contact_filter_reason(contact, filters)
        if reason:
            receipts[index] = _receipt(
                index,
                contact,
                "filtered",
                error=_error(reason, "Contact excluded by export filters"),
            )
            continue
        prepared = _prepare_contact(contact, city_map)
        if service == "manyreach":
            lead = integration.transform_contact(
                prepared, mappings, resolved["target_id"], resolved.get("newListName")
            )
            valid = integration.validate_contact(prepared)
        else:
            lead = integration.transform_contact(prepared, mappings)
            valid = integration.validate_contact(lead)
        if not valid:
            receipts[index] = _receipt(
                index,
                contact,
                "failed",
                error=_error("validation", "Mapped email is required or invalid"),
            )
            continue
        pending.append((index, contact))
        leads.append(lead)
    if not leads:
        return {
            "receipts": receipts,
            "destination": resolved,
            "requests_made": 0,
            "error": None,
        }
    outcomes = None
    try:
        response = _post_batch(integration, resolved, leads, config)
        outcome = _response_outcome(response, service, api_key, len(leads))
        if response.status_code in (200, 201):
            outcomes = _item_outcomes(outcome[3], leads, response.status_code)
    except requests.exceptions.ConnectTimeout:
        outcome = (
            "failed",
            True,
            _error(
                "connect_timeout", "Connection timed out before the request was sent"
            ),
            None,
        )
    except (
        requests.exceptions.InvalidURL,
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
    ):
        outcome = (
            "failed",
            False,
            _error("configuration", "Invalid export API URL"),
            None,
        )
    except requests.exceptions.RequestException:
        outcome = (
            "unknown",
            False,
            _error(
                "ambiguous_transport",
                "Request outcome is unknown; reconcile before retrying",
            ),
            None,
        )
    for (index, contact), (status, retryable, error, body) in zip(
        pending, outcomes or [outcome] * len(pending), strict=True
    ):
        receipts[index] = _receipt(
            index,
            contact,
            status,
            attempted=True,
            retryable=retryable,
            error=error,
            response=body,
        )
    return {
        "receipts": receipts,
        "destination": resolved,
        "requests_made": 1,
        "error": None,
    }
