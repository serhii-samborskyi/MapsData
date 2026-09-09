"""Prompt template rendering and extraction for per-contact HTTP enrichment."""

import json
import math
import re
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


SERVICE = "prompt_http"
TAG = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}")


def normalize_config(config, output_mapping, allowed_fields):
    config = dict(config or {})
    url = str(config.get("api_url") or "").strip()
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError("API URL must be an HTTP or HTTPS URL")
    if parts.username or parts.password or parts.fragment:
        raise ValueError("API URL cannot contain credentials or a fragment")
    if "{prompt}" not in parts.query or "{prompt}" in parts.netloc + parts.path:
        raise ValueError("API URL must contain {prompt} in its query parameters")
    prompt = str(config.get("prompt_template") or "").strip()
    if not prompt:
        raise ValueError("Prompt instruction is required")
    unknown = set(TAG.findall(prompt)) - set(allowed_fields)
    if unknown:
        raise ValueError("Unknown prompt fields: " + ", ".join(sorted(unknown)))
    if not output_mapping or not any(output_mapping.values()):
        raise ValueError("Add at least one JSON output mapping")
    for key, field in output_mapping.items():
        if not re.fullmatch(r"[\w-]+(?:\.[\w-]+)*", key) or field not in allowed_fields:
            raise ValueError("Invalid JSON output mapping")
    try:
        rate = int(config.get("requests_per_minute", 30))
    except (ValueError, TypeError):
        raise ValueError("Requests per minute must be an integer from 1 to 600")
    if not 1 <= rate <= 600:
        raise ValueError("Requests per minute must be from 1 to 600")
    # Templates differing only in prompt text share one endpoint budget.
    query = [(key, "" if "{prompt}" in value else value)
             for key, value in parse_qsl(parts.query, keep_blank_values=True)]
    endpoint = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path,
                          urlencode(sorted(query)), ""))
    return {"api_url": url, "prompt_template": prompt, "requests_per_minute": rate,
            "endpoint_key": endpoint, "timeout_seconds": config.get("timeout_seconds", 120)}


def automatic_concurrency(config, maximum=100):
    # Allow overlap up to the timeout window; the shared limiter controls starts.
    return max(1, min(maximum, math.ceil(config["requests_per_minute"] * config["timeout_seconds"] / 60)))


def render_prompt(template, contact):
    def replace(match):
        key = match.group(1)
        value = contact.get(key)
        if key == "company" and (value is None or not str(value).strip()):
            value = contact.get("business_name")
        return "" if value is None else str(value).strip()
    return TAG.sub(replace, template)


def request_url(config, contact):
    prompt = render_prompt(config["prompt_template"], contact)
    return config["api_url"].replace("{prompt}", quote(prompt, safe="")), prompt


def extract_object(payload):
    if not isinstance(payload, dict):
        raise ValueError("API response must be a JSON object")
    result = payload.get("result")
    for envelope in (payload, result):
        if isinstance(envelope, dict) and (envelope.get("ok") is False or envelope.get("error")):
            raise ValueError("API reported failure: " + str(envelope.get("error") or "ok=false"))
    answer = result.get("ai_answer") if isinstance(result, dict) else None
    if answer is None:
        answer = payload.get("ai_answer", payload)
    if isinstance(answer, dict):
        return answer
    if isinstance(answer, str):
        decoder = json.JSONDecoder()
        for match in re.finditer(r"\{", answer):
            try:
                value, _ = decoder.raw_decode(answer, match.start())
            except ValueError:
                continue
            if isinstance(value, dict):
                return value
    raise ValueError("Could not extract a JSON object from result.ai_answer")


def mapped_values(answer, output_mapping):
    values = {}
    for key in output_mapping:
        value = answer.get(key)
        if key not in answer:
            value = answer
            for part in key.split("."):
                value = value.get(part) if isinstance(value, dict) else None
        if value is not None and not isinstance(value, (dict, list)):
            text = str(value).strip()
            if text.lower() not in {"", "null", "none", "unknown", "n/a"}:
                values[key] = text
    return values
