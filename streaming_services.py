"""Single-attempt contact processors; the caller owns commits, retries and pacing.

``execute(step_type, config, contact, context)`` requires a frozen
``config['template_snapshot']`` and the main module as ``context['app']``.
No template lookup, task persistence, contact write, sleep or rate-limit claim
happens here. Enrichment options use the existing run option names.

``limits`` returns endpoint_key, requests_per_minute (None means unconfigured),
and timeout_seconds. The engine should share the lowest configured RPM across
all tasks for an endpoint. DNS timeout is per probe, not a whole-check deadline;
the existing system resolver has no explicit timeout. Email's helper uses 30s.

Results include input_snapshot, input_fingerprint and output_fingerprint (SHA256
of canonical JSON, excluding check timestamps). After committing a successful
verification, the parent may pass its input_fingerprint back in
``context['verification_fingerprints'][step_type]`` to skip an unchanged, checked
input. Status alone never permits that skip. The parent must not store failed or
cancelled attempts as verified. Fingerprints include verification configuration.

Legacy POST encoding defaults to json_input; json_flat can be specified in the
run config or template api_config. A format rejection returns
result['retry_config'] for the parent to merge into the next queued attempt.
Errors are short log-safe summaries; extracted values and diagnostics live only
in result. No response bodies are logged by this module.
"""

import hashlib
import json
import math
import re
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import prompt_enrichment

STEP_TYPES = frozenset({"enrichment", "dns_check", "email_verification"})
_HTTP_STATUS = re.compile(r"\bHTTP\s+(\d{3})\b", re.IGNORECASE)


def _object(value):
    if value is None:
        return {}
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("Expected an object in template configuration")
    return deepcopy(value)


def _template(config):
    snapshot = config.get("template_snapshot")
    if not isinstance(snapshot, dict) or not snapshot:
        raise ValueError("template_snapshot is required")
    template = deepcopy(snapshot)
    for field in (
        "api_config",
        "input_mapping",
        "output_mapping",
        "schema_cache",
        "status_mapping",
    ):
        template[field] = _object(template.get(field))
    template["service"] = (
        str(template.get("service") or "http_enrichment").strip().lower()
    )
    if template["service"] != prompt_enrichment.SERVICE:
        for field in (
            "api_url",
            "api_key",
            "timeout_seconds",
            "requests_per_minute",
            "request_encoding",
        ):
            if config.get(field) is not None:
                template["api_config"][field] = config[field]
        for field in ("input_mapping", "output_mapping"):
            if config.get(field) is not None:
                template[field] = _object(config[field])
    return template


def _integer(value, default, minimum, maximum):
    try:
        value = int(value)
    except (ValueError, TypeError, OverflowError):
        value = default
    return max(minimum, min(value, maximum))


def _rate(value, default=None):
    if value is None:
        return default
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("requests_per_minute must be positive and finite")
    return value


def _endpoint(url):
    parts = urlsplit(str(url or "").strip())
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError("API URL must be an HTTP or HTTPS URL")
    if parts.username or parts.password or parts.fragment:
        raise ValueError("API URL cannot contain credentials or a fragment")
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path,
            urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True))),
            "",
        )
    )


def limits(step_type, config):
    """Return scheduling metadata without importing main or performing I/O.

    Legacy snapshots must contain api_url, as frozen during run creation.
    Invalid configurations raise ValueError for the engine to reject at creation.
    """
    if step_type not in STEP_TYPES:
        raise ValueError("Unsupported streaming step type")
    template = _template(config)
    api = template["api_config"]
    if step_type == "dns_check":
        return {
            "endpoint_key": "dns_ssl_checker",
            "requests_per_minute": _rate(api.get("requests_per_minute")),
            "timeout_seconds": _integer(api.get("timeout_seconds"), 8, 1, 60),
        }
    if step_type == "email_verification":
        if template["service"] != "myemailverifier":
            raise ValueError("Unsupported email verification service")
        rate = min(30, _rate(api.get("requests_per_minute"), 30))
        delay = float(config.get("delay") or 0)
        if math.isfinite(delay) and delay > 0:
            rate = min(rate, 60 / delay)
        return {
            "endpoint_key": "https://client.myemailverifier.com/verifier/validate_single",
            "requests_per_minute": rate,
            "timeout_seconds": 30,
        }
    if template["service"] == prompt_enrichment.SERVICE:
        api["timeout_seconds"] = _integer(api.get("timeout_seconds"), 120, 15, 600)
        # Field membership is validated against main's allowlist during execute.
        fields = set(template["output_mapping"].values())
        fields.update(
            prompt_enrichment.TAG.findall(str(api.get("prompt_template") or ""))
        )
        normalized = prompt_enrichment.normalize_config(
            api, template["output_mapping"], fields
        )
        return {
            key: normalized[key]
            for key in ("endpoint_key", "requests_per_minute", "timeout_seconds")
        }
    return {
        "endpoint_key": _endpoint(api.get("api_url")),
        "requests_per_minute": _rate(api.get("requests_per_minute")),
        "timeout_seconds": _integer(api.get("timeout_seconds"), 120, 15, 600),
    }


def _fingerprint(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _input_result(step_type, snapshot, template):
    return {
        "input_snapshot": snapshot,
        "input_fingerprint": _fingerprint(
            {"step_type": step_type, "input": snapshot, "template": template}
        ),
    }


def _outcome(status, result=None, updates=None, error="", retry_after=None):
    updates = updates or {}
    result = dict(result or {})
    result["output_fingerprint"] = _fingerprint(
        {
            key: value
            for key, value in updates.items()
            if key != "domain_last_checked_at"
        }
    )
    outcome = {"status": status, "updates": updates, "result": result, "error": error}
    if retry_after is not None:
        outcome["retry_after"] = retry_after
    return outcome


def _skip(reason, result=None):
    return _outcome("skipped", {**(result or {}), "reason": reason})


def _retry_after(details):
    value = details.get("retry_after")
    if value is None:
        headers = details.get("headers") or details.get("response_headers") or {}
        value = next(
            (value for key, value in headers.items() if key.lower() == "retry-after"),
            None,
        )
    if value is None:
        return None
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(str(value))
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = (date - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0, seconds) if math.isfinite(seconds) else None


def _failure(app, result, details):
    error = str(details.get("error") or "Service request failed")
    code = details.get("status_code")
    if code is None:
        match = _HTTP_STATUS.search(error)
        code = match.group(1) if match else None
    if str(code) in {"401", "403"}:
        return _outcome(
            "blocked", result, error=f"API credentials rejected (HTTP {code})."
        )
    summary = app._enrichment_error_summary(error)
    if code is not None and str(code).isdigit() and int(code) >= 400:
        summary = f"API request failed (HTTP {code})."
    elif summary == "Enrichment request failed.":
        summary = "Service request failed."
    return _outcome("failed", result, error=summary, retry_after=_retry_after(details))


def _cancelled(context):
    callback = context.get("cancelled")
    return bool(callback and callback())


def _known_verification(step_type, config, contact, context, result, app):
    if not app._coerce_bool_flag(config.get("skip_verified"), True):
        return False
    field = "domain_status" if step_type == "dns_check" else "email_status"
    status = str(contact.get(field) or "").strip().lower()
    if status in {"", "unknown", "unverified", "unchecked", "pending", "check failed"}:
        return False
    known = context.get("verification_fingerprints") or {}
    return known.get(step_type) == result["input_fingerprint"]


def _enrichment(app, config, template, contact, context):
    output_mapping = {}
    for key, field in template["output_mapping"].items():
        key, field = (
            app._normalize_mapping_value(key),
            app._normalize_mapping_value(field),
        )
        if not key or not field:
            continue
        if field not in app.ENRICHMENT_LOCAL_FIELD_SET:
            raise ValueError("Invalid local output mapping")
        output_mapping[key] = field
    if not output_mapping:
        raise ValueError("At least one output mapping is required")
    template["output_mapping"] = output_mapping
    targets = set(output_mapping.values())
    if (
        app._coerce_bool_flag(config.get("emails_only"), False)
        and not str(contact.get("email") or "").strip()
    ):
        return _skip("missing_email")
    if app._coerce_bool_flag(
        config.get("valid_emails_only"), False
    ) and not app._is_valid_email_lead(contact):
        return _skip("email_not_valid")
    if app._coerce_bool_flag(config.get("missing_field_only"), False):
        field = str(config.get("missing_field_name") or "").strip()
        if field not in app.ENRICHMENT_LOCAL_FIELD_SET:
            raise ValueError("Invalid missing_field_name")
        if not app._is_enrichment_field_missing(contact.get(field)):
            return _skip("filter_field_populated")

    overwrite = app._coerce_bool_flag(config.get("overwrite_existing"), False)
    is_prompt = template["service"] == prompt_enrichment.SERVICE
    if is_prompt:
        api = app._prompt_template_config(template)
        snapshot = {
            "prompt": prompt_enrichment.render_prompt(api["prompt_template"], contact)
        }
    else:
        api = template["api_config"]
        required = config.get("required_inputs")
        if not isinstance(required, list):
            required = template["schema_cache"].get("required_input_fields") or []
        required = [str(field).strip() for field in required if str(field).strip()]
        payload, missing = app._build_enrichment_payload(
            contact, template["input_mapping"], required
        )
        snapshot = {"input": payload}
    result = _input_result("enrichment", snapshot, template)
    if not overwrite and all(
        not app._is_enrichment_field_missing(contact.get(field)) for field in targets
    ):
        return _skip("output_already_present", result)
    if (
        not is_prompt
        and missing
        and app._coerce_bool_flag(config.get("skip_missing_input"), True)
    ):
        return _skip("missing_required_input", {**result, "missing_required": missing})
    if _cancelled(context):
        return _skip("cancelled", result)

    if is_prompt:
        if context.get("before_request") and not context["before_request"]():
            return _outcome("retry", result, error="Request deferred before dispatch")
        details = app._fetch_prompt_enrichment(api, contact, output_mapping)
        values = details.get("values") or {}
    else:
        api_url = str(api.get("api_url") or app.DEFAULT_ENRICHMENT_API_URL).strip()
        _endpoint(api_url)
        headers = {"Content-Type": "application/json"}
        if api.get("api_key"):
            headers["x-api-key"] = str(api["api_key"]).strip()
        encoding = api.get("request_encoding") or "json_input"
        if encoding not in {"json_input", "json_flat"}:
            raise ValueError("Unsupported enrichment request_encoding")
        builder = (
            app._flat_enrichment_payload
            if encoding == "json_flat"
            else app._wrapped_enrichment_payload
        )
        body = builder(payload, app._selected_enrichment_fields(output_mapping))
        if context.get("before_request") and not context["before_request"]():
            return _outcome("retry", result, error="Request deferred before dispatch")
        response = app.requests.post(
            api_url,
            headers=headers,
            json=body,
            timeout=app._normalize_enrichment_timeout(api.get("timeout_seconds")),
        )
        details = {
            "status_code": response.status_code,
            "request_encoding": encoding,
            "response_text": response.text,
            "headers": dict(getattr(response, "headers", {}) or {}),
        }
        values = {}
        if response.status_code >= 400:
            details["error"] = f"HTTP {response.status_code}"
            if (
                encoding == "json_input"
                and app._response_reports_missing_sent_enrichment_field(
                    response, payload
                )
            ):
                result["retry_config"] = {"request_encoding": "json_flat"}
        else:
            try:
                data = response.json()
                details["response_json"] = data
                if not isinstance(data, dict):
                    raise ValueError("API response must be a JSON object")
                if (
                    data.get("ok") is False
                    or data.get("error")
                    or data.get("success") is False
                ):
                    raise ValueError("API reported failure")
                values = {
                    key: data[key]
                    for key in output_mapping
                    if key in data and not app._is_enrichment_field_missing(data[key])
                }
                if not values:
                    raise ValueError(
                        "API response has no usable values for the selected mappings"
                    )
            except (ValueError, TypeError) as exc:
                details["error"] = str(exc)
    result.update(
        {
            "values": values,
            "field_results": app._enrichment_field_results(values, output_mapping),
            "diagnostics": details,
        }
    )
    if _cancelled(context):
        return _skip("cancelled", result)
    if details.get("error"):
        return _failure(app, result, details)
    updates = app._prompt_contact_updates(contact, values, output_mapping, overwrite)
    if (
        "email" in updates
        and str(updates["email"]).strip().lower()
        != str(contact.get("email") or "").strip().lower()
    ):
        updates["email_status"] = "unverified"
    return _outcome("completed", result, updates)


def _verification(app, step_type, config, template, contact, context):
    is_domain = step_type == "dns_check"
    if is_domain:
        if not app._is_domain_checker_template(template):
            raise ValueError("DNS checks require a dns_ssl_checker template")
        template["api_config"] = app._normalize_domain_checker_api_config(
            template["api_config"]
        )
        value = app._normalize_domain(contact.get("domain"))
        snapshot = {"domain": value}
    else:
        value = str(contact.get("email") or "").strip().lower()
        snapshot = {"email": value}
    result = _input_result(step_type, snapshot, template)
    if not value:
        return _skip("missing_domain" if is_domain else "missing_email", result)
    if is_domain and not app._is_valid_domain(value):
        return _skip("invalid_domain", result)
    if (
        not is_domain
        and app._coerce_bool_flag(config.get("skip_public_providers"), False)
        and value.rsplit("@", 1)[-1] in app.PUBLIC_EMAIL_DOMAINS
    ):
        return _skip("public_email_provider", result)
    if _known_verification(step_type, config, contact, context, result, app):
        return _skip("already_verified", result)
    if _cancelled(context):
        return _skip("cancelled", result)

    service = app.EmailVerificationService()
    if context.get("before_request") and not context["before_request"]():
        return _outcome("retry", result, error="Request deferred before dispatch")
    if is_domain:
        details = service.verify_domain(value, template)
    else:
        if not template["api_config"].get("api_key"):
            return _outcome(
                "blocked", result, error="Email verification API key is required."
            )
        batch = service.verify_batch([value], template, 0)
        if not isinstance(batch, list) or len(batch) != 1:
            raise ValueError("Expected one email verification result")
        details = batch[0]
    if not isinstance(details, dict):
        raise ValueError("Expected verification details")
    result["diagnostics"] = details
    if _cancelled(context):
        return _skip("cancelled", result)

    if not is_domain:
        if not details.get("success") or not details.get("mapped_status"):
            return _failure(app, result, details)
        updates = {"email_status": details["mapped_status"]}
    else:
        # A negative DNS/SSL finding is a completed check, not an API auth error.
        if (
            not isinstance(details.get("dns_ok"), bool)
            or not details.get("mapped_status")
            or details["mapped_status"] == "Check Failed"
        ):
            return _outcome("failed", result, error="Domain check failed.")
        http, https, ssl = (details.get(key) or {} for key in ("http", "https", "ssl"))
        updates = {
            "domain_status": details["mapped_status"],
            "domain_dns_status": "resolved" if details["dns_ok"] else "not_resolved",
            "domain_http_status": str(
                http.get("status_code")
                or ("reachable" if http.get("reachable") else "unreachable")
            ),
            "domain_https_status": str(
                https.get("status_code")
                or ("reachable" if https.get("reachable") else "unreachable")
            ),
            "domain_ssl_status": "valid"
            if ssl.get("valid")
            else str(ssl.get("status") or "issue"),
            "domain_error": str(
                details.get("error")
                or http.get("error")
                or https.get("error")
                or ssl.get("error")
                or ""
            )[:1000],
            "domain_last_checked_at": datetime.now(timezone.utc).isoformat(),
        }
    result["mapped_status"] = details["mapped_status"]
    return _outcome("completed", result, updates)


def execute(step_type, config, contact, context):
    """Execute one contact attempt and return changes for the parent's transaction."""
    try:
        if _cancelled(context):
            return _skip("cancelled")
        if step_type not in STEP_TYPES:
            raise ValueError("Unsupported streaming step type")
        app = context["app"]
        template = _template(config)
        contact = deepcopy(dict(contact))
        if step_type == "enrichment":
            return _enrichment(app, config, template, contact, context)
        return _verification(app, step_type, config, template, contact, context)
    except Exception as exc:
        response = getattr(exc, "response", None)
        details = {"error": str(exc), "exception_type": type(exc).__name__}
        if response is not None:
            details.update(
                {
                    "status_code": response.status_code,
                    "headers": dict(getattr(response, "headers", {}) or {}),
                }
            )
        elif getattr(exc, "status_code", None) is not None:
            details["status_code"] = exc.status_code
        app = context.get("app")
        if app is None:
            return _outcome(
                "failed", {"diagnostics": details}, error="Service adapter is required."
            )
        if step_type == "dns_check":
            return _outcome(
                "failed", {"diagnostics": details}, error="Domain check failed."
            )
        return _failure(app, {"diagnostics": details}, details)
