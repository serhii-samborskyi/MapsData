"""Generate literal search requests from a pinned Census Gazetteer release.

Public contract (no database, campaign, worker or LLM dependencies):
    catalog(state, *, config=DatasetConfig()) -> places + provenance
    generate_city_requests(state, request_template, *, include_cdps=True,
                           catalog_data=None, config=DatasetConfig(),
                           max_requests=20000) -> requests + coverage
    generate is an alias for generate_city_requests.

Pass an unfiltered catalog_data returned by catalog for entirely pure generation.
Otherwise generation lazily loads the catalog; import performs no I/O. Cache
misses fetch the official national places and states ZIPs, then publish one
validated JSON snapshot atomically without replacing an existing snapshot.
The cache is year/parser-version specific and never silently refreshed or
substituted with another year. GEOGRAPHIC_REQUESTS_CACHE_DIR overrides the
default XDG cache directory. Corrupt snapshots raise GazetteerError.

Default 2025 verified against the official directory on 2026-09-09; the 2026
directory had no published downloads. New years require explicit configuration.
Source: https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
Layout: https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/gaz-record-layouts.2025.html
LSAD: https://www.census.gov/library/reference/code-lists/legal-status-codes.html

2025 text is pipe-delimited; older tab-delimited text is also supported by csv.
Places include incorporated places (including balance records) and CDPs, not
all communities, postal cities, businesses, or ads. LSAD 00 names are preserved
verbatim because the source supplies no suffix to remove.
"""

import csv
import hashlib
import io
import json
import math
import os
import re
import tempfile
import time
import zipfile
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_YEAR = 2025
PARSER_VERSION = 1
MAX_REQUESTS = 20_000
MAX_REQUEST_LENGTH = 4_000
MAX_TEMPLATE_LENGTH = 2_000
MAX_CACHE_BYTES = 48 * 1024 * 1024
MAX_DATASET_PLACES = 100_000
SOURCE_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"
CDP_CODES = frozenset({"55", "57", "62"})
_LSAD_SUFFIXES = {
    "00": "",
    "21": "borough",
    "25": "city",
    "37": "municipality",
    "43": "town",
    "47": "village",
    "53": "city and borough",
    "55": "comunidad",
    "57": "CDP",
    "62": "zona urbana",
    "BL": "(balance)",
    "CB": "consolidated government (balance)",
    "CG": "consolidated government",
    "CN": "corporation",
    "MB": "metropolitan government (balance)",
    "MG": "metropolitan government",
    "MT": "metro government",
    "UB": "unified government (balance)",
    "UC": "urban county",
    "UG": "unified government",
}
_TAG = re.compile(r"(?<!\{)\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}(?!\})")
_FIELDS = frozenset({"city", "state", "state_name", "geoid"})
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class GazetteerError(RuntimeError):
    """Official data could not be loaded or validated; no partial list is returned."""


@dataclass(frozen=True)
class DatasetConfig:
    year: int = DEFAULT_YEAR
    cache_dir: str | Path | None = None
    timeout_seconds: float = 20
    max_download_bytes: int = 8 * 1024 * 1024
    max_uncompressed_bytes: int = 32 * 1024 * 1024

    def __post_init__(self):
        if type(self.year) is not int or not 2013 <= self.year <= 2099:
            raise ValueError(
                "Dataset year must be an explicit integer from 2013 to 2099"
            )
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 60
        ):
            raise ValueError("timeout_seconds must be greater than zero and at most 60")
        for name, ceiling in (
            ("max_download_bytes", 32 * 1024 * 1024),
            ("max_uncompressed_bytes", 64 * 1024 * 1024),
        ):
            value = getattr(self, name)
            if type(value) is not int or not 0 < value <= ceiling:
                raise ValueError(f"{name} must be a positive integer at most {ceiling}")


def _source_url(config, kind):
    return (
        f"{SOURCE_ROOT}/{config.year}_Gazetteer/{config.year}_Gaz_{kind}_national.zip"
    )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ARG002
        return None


def _download(url, config):
    started = time.monotonic()
    request = Request(
        url,
        headers={"User-Agent": "MapsData-Gazetteer/1.0", "Accept": "application/zip"},
    )
    try:
        with build_opener(_NoRedirect()).open(
            request, timeout=config.timeout_seconds
        ) as response:
            length = response.headers.get("Content-Length")
            if length is not None and (
                not length.isdigit() or int(length) > config.max_download_bytes
            ):
                raise GazetteerError(
                    "Census download exceeds the configured size limit"
                )
            data = bytearray()
            while True:
                if time.monotonic() - started >= config.timeout_seconds:
                    raise GazetteerError("Census download exceeded its time limit")
                # read1 allows a deadline check after each underlying socket read.
                chunk = response.read1(
                    min(64 * 1024, config.max_download_bytes + 1 - len(data))
                )
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > config.max_download_bytes:
                    raise GazetteerError(
                        "Census download exceeds the configured size limit"
                    )
            if length is not None and len(data) != int(length):
                raise GazetteerError("Census download was truncated")
            return bytes(data)
    except HTTPError as exc:
        raise GazetteerError(
            f"Census download failed (HTTP {exc.code}); requested release was not substituted"
        ) from exc
    except (URLError, OSError, HTTPException) as exc:
        raise GazetteerError("Census download failed or timed out") from exc


def _parse_archive(data, kind, config):
    if len(data) > config.max_download_bytes:
        raise GazetteerError("Census archive exceeds the configured size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = archive.infolist()
            expected = f"{config.year}_Gaz_{kind}_national.txt".casefold()
            if len(members) != 1 or members[0].filename.casefold() != expected:
                raise GazetteerError(
                    "Census ZIP must contain exactly the expected national text file"
                )
            member = members[0]
            if member.flag_bits & 1 or member.file_size > config.max_uncompressed_bytes:
                raise GazetteerError(
                    "Census ZIP is encrypted or exceeds the uncompressed size limit"
                )
            with archive.open(member) as source:
                text_bytes = source.read(config.max_uncompressed_bytes + 1)
            if (
                len(text_bytes) > config.max_uncompressed_bytes
                or len(text_bytes) != member.file_size
            ):
                raise GazetteerError(
                    "Census text exceeds the configured size limit or is truncated"
                )
        text = text_bytes.decode("utf-8-sig")
        stream = io.StringIO(text, newline="")
        header = stream.readline()
        dialect = csv.Sniffer().sniff(header, delimiters="|\t")
        stream.seek(0)
        reader = csv.DictReader(stream, delimiter=dialect.delimiter, strict=True)
        columns = [column.strip() for column in (reader.fieldnames or [])]
        required = {"USPS", "GEOID", "NAME"} | ({"LSAD"} if kind == "place" else set())
        if not required <= set(columns) or len(columns) != len(set(columns)):
            raise GazetteerError(
                "Census text has missing or duplicate required columns"
            )
        reader.fieldnames = columns
        rows, seen = [], set()
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise GazetteerError("Census text contains a malformed record")
            row = {key: value.strip() for key, value in row.items()}
            code, geoid, name = row["USPS"], row["GEOID"], row["NAME"]
            width = 7 if kind == "place" else 2
            if (
                not re.fullmatch(r"[A-Z]{2}", code)
                or not re.fullmatch(rf"[0-9]{{{width}}}", geoid)
                or not name
                or _CONTROL.search(name)
                or geoid in seen
            ):
                raise GazetteerError(
                    "Census text contains an invalid or duplicate geographic identifier/name"
                )
            seen.add(geoid)
            rows.append(row)
            if len(rows) > (MAX_DATASET_PLACES if kind == "place" else 60):
                raise GazetteerError("Census record count exceeds the supported limit")
        if not rows:
            raise GazetteerError("Census text contains no records")
        return rows, {
            "url": _source_url(config, kind),
            "archive_sha256": hashlib.sha256(data).hexdigest(),
            "content_sha256": hashlib.sha256(text_bytes).hexdigest(),
            "record_count": len(rows),
        }
    except (
        zipfile.BadZipFile,
        UnicodeError,
        csv.Error,
        NotImplementedError,
        EOFError,
        zlib.error,
    ) as exc:
        raise GazetteerError("Census archive or delimited text is invalid") from exc


def _assemble_dataset(state_rows, place_rows):
    states = {
        row["USPS"]: {
            "state": row["USPS"],
            "state_name": row["NAME"],
            "geoid": row["GEOID"],
        }
        for row in state_rows
    }
    if len(states) != len(state_rows):
        raise GazetteerError("Census state abbreviations are not unique")
    places = []
    for row in place_rows:
        state, name, lsad = states.get(row["USPS"]), row["NAME"], row["LSAD"]
        if not state or not row["GEOID"].startswith(state["geoid"]):
            raise GazetteerError("Census place and state identifiers do not match")
        if lsad not in _LSAD_SUFFIXES:
            raise GazetteerError(f"Unsupported Census place LSAD code: {lsad}")
        suffix = _LSAD_SUFFIXES[lsad]
        city = name
        if suffix:
            suffix = " " + suffix
            if not name.endswith(suffix):
                raise GazetteerError("Census place name does not match its LSAD suffix")
            city = name[: -len(suffix)].strip()
        if not city:
            raise GazetteerError("Census place has no base name")
        places.append(
            {
                "city": city,
                "state": state["state"],
                "state_name": state["state_name"],
                "geoid": row["GEOID"],
                "census_name": name,
                "lsad": lsad,
                "is_cdp": lsad in CDP_CODES,
            }
        )
    if {place["state"] for place in places} != set(states):
        raise GazetteerError(
            "Census national file is missing places for a listed state"
        )
    places.sort(
        key=lambda place: (place["state"], place["city"].casefold(), place["geoid"])
    )
    return list(states.values()), places


def _json_bytes(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")


def _cache_path(config):
    root = config.cache_dir or os.environ.get("GEOGRAPHIC_REQUESTS_CACHE_DIR")
    if not root:
        root = (
            Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
            / "mapsdata"
            / "census-gazetteer"
        )
    return Path(root) / f"gazetteer-{config.year}-v{PARSER_VERSION}.json"


def _read_cache(path, config):
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_CACHE_BYTES + 1)
    except FileNotFoundError:
        return None
    if len(raw) > MAX_CACHE_BYTES:
        raise GazetteerError("Cached Census snapshot exceeds the size limit")
    try:
        envelope = json.loads(raw)
        payload = envelope["payload"]
        expected = hashlib.sha256(_json_bytes(payload)).hexdigest()
        if (
            envelope["sha256"] != expected
            or payload["year"] != config.year
            or payload["parser_version"] != PARSER_VERSION
            or any(
                payload["sources"][kind]["url"] != _source_url(config, kind)
                for kind in ("state", "place")
            )
            or len(payload["places"]) != payload["sources"]["place"]["record_count"]
            or len(payload["states"]) != payload["sources"]["state"]["record_count"]
        ):
            raise ValueError("Snapshot identity/integrity mismatch")
        return envelope
    except (ValueError, TypeError, KeyError) as exc:
        raise GazetteerError(
            "Cached Census snapshot is corrupt or incompatible; use a new cache directory"
        ) from exc


def _load_dataset(config):
    path = _cache_path(config)
    try:
        cached = _read_cache(path, config)
        if cached is not None:
            return cached
        sources, records = {}, {}
        for kind in ("state", "place"):
            data = _download(_source_url(config, kind), config)
            records[kind], sources[kind] = _parse_archive(data, kind, config)
        states, places = _assemble_dataset(records["state"], records["place"])
        payload = {
            "year": config.year,
            "parser_version": PARSER_VERSION,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "states": states,
            "places": places,
        }
        envelope = {
            "payload": payload,
            "sha256": hashlib.sha256(_json_bytes(payload)).hexdigest(),
        }
        encoded = _json_bytes(envelope)
        if len(encoded) > MAX_CACHE_BYTES:
            raise GazetteerError("Census snapshot exceeds the cache size limit")
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".gazetteer-", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            try:
                temporary.write(encoded)
                temporary.flush()
                os.fsync(temporary.fileno())
                # Atomic, no-clobber publication also handles simultaneous processes.
                try:
                    os.link(temporary_path, path)
                except FileExistsError:
                    return _read_cache(path, config)
            finally:
                temporary_path.unlink(missing_ok=True)
        return envelope
    except OSError as exc:
        raise GazetteerError("Cannot read or publish the Census cache") from exc


def _state_key(state):
    if (
        not isinstance(state, str)
        or not state.strip()
        or len(state) > 100
        or _CONTROL.search(state)
    ):
        raise ValueError("state must be a US state name or postal abbreviation")
    return " ".join(state.split()).casefold()


_DEFAULT_CONFIG = DatasetConfig()


def catalog(state, *, config=_DEFAULT_CONFIG):
    """Return all Gazetteer places for one state, DC or Puerto Rico, including CDPs.

    Accepts either case-insensitive USPS abbreviation or official full name.
    Returns fresh dictionaries; callers cannot mutate the persisted catalog.
    """
    key = _state_key(state)
    envelope = _load_dataset(config)
    payload = envelope["payload"]
    selected = next(
        (
            row
            for row in payload["states"]
            if key in {row["state"].casefold(), row["state_name"].casefold()}
        ),
        None,
    )
    if selected is None:
        raise ValueError("Unknown US state, District of Columbia or Puerto Rico")
    places = [
        place for place in payload["places"] if place["state"] == selected["state"]
    ]
    return {
        "places": places,
        "place_count": len(places),
        "state": selected["state"],
        "state_name": selected["state_name"],
        "source_url": _source_url(config, "place"),
        "year": config.year,
        "provenance": {
            "year": config.year,
            "parser_version": PARSER_VERSION,
            "retrieved_at": payload["retrieved_at"],
            "snapshot_sha256": envelope["sha256"],
            "sources": payload["sources"],
        },
    }


def _template(request_template):
    if (
        not isinstance(request_template, str)
        or not request_template.strip()
        or len(request_template) > MAX_TEMPLATE_LENGTH
        or _CONTROL.search(request_template)
    ):
        raise ValueError(
            f"request_template must be a single line of 1 to {MAX_TEMPLATE_LENGTH} characters"
        )
    fields = set(_TAG.findall(request_template))
    remainder = _TAG.sub("", request_template)
    if fields - _FIELDS or "{{" in remainder or "}}" in remainder:
        raise ValueError(
            "Only {{city}}, {{state}}, {{state_name}} and {{geoid}} placeholders are supported"
        )
    if "city" not in fields:
        raise ValueError("request_template must contain {{city}}")
    return request_template.strip()


def generate_city_requests(
    state,
    request_template,
    *,
    include_cdps=True,
    max_requests=MAX_REQUESTS,
    config=_DEFAULT_CONFIG,
    catalog_data=None,
):
    """Render unique literal requests; passing catalog_data makes this I/O-free.

    {{state}} renders the USPS abbreviation. Metadata retains every GEOID when
    equal queries (ignoring case and whitespace) collapse into one request.
    Limits reject the entire expansion instead of silently truncating coverage.
    ValueError means invalid input; GazetteerError means unavailable/invalid data.
    """
    key = _state_key(state)
    template = _template(request_template)
    if type(include_cdps) is not bool:
        raise ValueError("include_cdps must be a boolean")
    if type(max_requests) is not int or not 1 <= max_requests <= MAX_REQUESTS:
        raise ValueError(f"max_requests must be an integer from 1 to {MAX_REQUESTS}")
    data = catalog(state, config=config) if catalog_data is None else catalog_data
    if key not in {data["state"].casefold(), data["state_name"].casefold()}:
        raise ValueError("catalog_data does not match the requested state")
    if data["year"] != config.year:
        raise ValueError("catalog_data does not match the configured dataset year")
    places = data["places"]
    if len(places) != data["place_count"] or len({p["geoid"] for p in places}) != len(
        places
    ):
        raise ValueError(
            "catalog_data has an inconsistent place count or duplicate GEOIDs"
        )
    if any(
        p["state"] != data["state"] or p["state_name"] != data["state_name"]
        for p in places
    ):
        raise ValueError("catalog_data mixes states")
    selected = [p for p in places if include_cdps or not p["is_cdp"]]
    queries = {}
    for place in sorted(selected, key=lambda p: (p["city"].casefold(), p["geoid"])):
        query = _TAG.sub(lambda match, place=place: place[match.group(1)], template)
        if len(query) > MAX_REQUEST_LENGTH:
            raise ValueError(
                f"Generated requests must not exceed {MAX_REQUEST_LENGTH} characters"
            )
        unique_key = " ".join(query.split()).casefold()
        if unique_key not in queries:
            if len(queries) >= max_requests:
                raise ValueError(
                    f"Generated request count exceeds {max_requests}; no partial expansion returned"
                )
            queries[unique_key] = {"request": query, "geoids": []}
        queries[unique_key]["geoids"].append(place["geoid"])
    metadata = list(queries.values())
    cdp_count = sum(p["is_cdp"] for p in places)
    return {
        "requests": [row["request"] for row in metadata],
        "request_count": len(metadata),
        "request_metadata": metadata,
        "state": data["state"],
        "state_name": data["state_name"],
        "source_url": data["source_url"],
        "year": data["year"],
        "provenance": json.loads(json.dumps(data["provenance"])),
        "coverage": {
            "include_cdps": include_cdps,
            "catalog_place_count": len(places),
            "included_place_count": len(selected),
            "incorporated_place_count": len(places) - cdp_count,
            "cdp_count": cdp_count,
            "excluded_cdp_count": 0 if include_cdps else cdp_count,
            "duplicate_query_count": len(selected) - len(metadata),
            "place_types": ["incorporated_place", "census_designated_place"]
            if include_cdps
            else ["incorporated_place"],
            "scope": "Census Gazetteer places for the selected state and release, including balance records. "
            "Not every community or postal city; no exhaustive business or advertising coverage is implied.",
        },
    }


generate = generate_city_requests
