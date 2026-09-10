"""Authenticated MCP setup with one-time, database-backed bearer credentials."""

import hashlib
import ipaddress
import json
import secrets
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, HTTPException, Request
from starlette.responses import JSONResponse

PRIVATE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "Referrer-Policy": "no-referrer",
}


def _error(status, detail):
    return HTTPException(status, detail, headers=PRIVATE_HEADERS)


def init_schema(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mcp_connections (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            public_url TEXT,
            token_digest TEXT CHECK (token_digest IS NULL OR token_digest ~ '^[0-9a-f]{64}$'),
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO mcp_connections(id) VALUES (1) ON CONFLICT DO NOTHING;
    """)


def validate_public_url(value):
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 2048
        or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise _error(400, "Enter the public HTTPS MCP URL ending in /mcp/")
    try:
        parts = urlsplit(value)
        port = parts.port
        hostname = parts.hostname
        if (
            parts.scheme != "https"
            or not hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or "\\" in value
            or "*" in parts.netloc
            or not parts.path.isascii()
            or not parts.path.endswith("/mcp/")
            or any(segment in (".", "..") for segment in parts.path.split("/"))
        ):
            raise ValueError("Invalid MCP URL")
        hostname = hostname.encode("idna").decode("ascii").lower()
        authority = f"[{hostname}]" if ":" in hostname else hostname
        if port is not None and port != 443:
            authority += f":{port}"
        return urlunsplit(("https", authority, parts.path, "", ""))
    except (ValueError, UnicodeError) as exc:
        raise _error(400, "Enter a valid public HTTPS MCP URL ending in /mcp/") from exc


def connection_blocks(public_url, token):
    codex = (
        "[mcp_servers.mapsdata]\n"
        f"url = {json.dumps(public_url)}\n"
        "http_headers = { Authorization = " + json.dumps(f"Bearer {token}") + " }\n"
    )
    claude = json.dumps(
        {
            "mcpServers": {
                "mapsdata": {
                    "type": "http",
                    "url": public_url,
                    "headers": {"Authorization": f"Bearer {token}"},
                }
            }
        },
        indent=2,
    )
    assistant = (
        "Connect Codex to my MapsData app using this remote Streamable HTTP MCP server.\n"
        "Merge this entry into my user-level ~/.codex/config.toml (or $CODEX_HOME/config.toml), "
        "preserving unrelated settings. Replace only an existing mapsdata entry. "
        "Remove stale mapsdata bearer_token_env_var or Authorization overrides if present.\n"
        "The Authorization value is a secret: keep the config private, do not commit it, "
        "and do not echo the token in your reply or logs. No local server or shell script is needed.\n\n"
        f"```toml\n{codex}```\n\n"
        "Check the connection and list source and funnel templates. "
        "If MCP connections need reloading, tell me to restart the client. "
        "Do not create or launch a campaign until I ask and confirm its preview."
    )
    return {"codex": codex, "claude_code": claude, "assistant": assistant}


class ConnectionStore:
    def __init__(self, get_db, environment_settings=None):
        self.get_db = get_db
        self.environment_settings = environment_settings

    def active_configuration(self):
        if self.environment_settings is not None:
            return {
                "public_url": self.environment_settings.public_url,
                "token_digest": hashlib.sha256(
                    self.environment_settings.token.get_secret_value().encode()
                ).hexdigest(),
            }
        with self.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT public_url, token_digest FROM mcp_connections WHERE id = 1"
            )
            row = cursor.fetchone()
        return dict(row) if row and row["token_digest"] else None

    def metadata(self, default_url=""):
        if self.environment_settings is not None:
            return {
                "enabled": True,
                "managed": False,
                "environment_managed": True,
                "public_url": self.environment_settings.public_url,
                "token_available": True,
            }
        with self.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT public_url, token_digest IS NOT NULL AS enabled FROM mcp_connections WHERE id = 1"
            )
            row = cursor.fetchone() or {}
        return {
            "enabled": bool(row.get("enabled")),
            "managed": True,
            "environment_managed": False,
            "public_url": row.get("public_url") or default_url,
            "token_available": False,
        }

    def _managed_only(self):
        if self.environment_settings is not None:
            raise _error(
                409,
                "This token is managed by MAPSDATA_MCP_TOKEN in the server environment",
            )

    def generate(self, public_url, replace_confirmed=False):
        self._managed_only()
        public_url = validate_public_url(public_url)
        with self.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT token_digest FROM mcp_connections WHERE id = 1 FOR UPDATE"
            )
            row = cursor.fetchone()
            if not row:
                raise _error(
                    503, "MCP settings are not initialized; restart the updated web app"
                )
            if row["token_digest"] and replace_confirmed is not True:
                raise _error(
                    409,
                    "Confirm replacement of the existing token; connected clients will need the new token",
                )
            token = "mapsdata_" + secrets.token_urlsafe(32)
            digest = hashlib.sha256(token.encode()).hexdigest()
            cursor.execute(
                """UPDATE mcp_connections SET public_url = %s, token_digest = %s,
                created_at = COALESCE(created_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = 1""",
                (public_url, digest),
            )
            conn.commit()
        return {
            "enabled": True,
            "managed": True,
            "environment_managed": False,
            "public_url": public_url,
            "token": token,
            "blocks": connection_blocks(public_url, token),
        }

    def reveal(self):
        if self.environment_settings is None:
            raise _error(
                409,
                "Generated tokens are shown only once; generate a replacement if needed",
            )
        settings = self.environment_settings
        token = settings.token.get_secret_value()
        return {
            "enabled": True,
            "managed": False,
            "environment_managed": True,
            "public_url": settings.public_url,
            "token": token,
            "blocks": connection_blocks(settings.public_url, token),
        }

    def revoke(self, confirmed=False):
        self._managed_only()
        if confirmed is not True:
            raise _error(
                400, "Confirm token revocation; connected clients will lose access"
            )
        with self.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE mcp_connections SET token_digest = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = 1"
            )
            conn.commit()
        return self.metadata()


def _authorize(host, request, mutation=False):
    if not host.UI_AUTH_ENABLED:
        raise _error(
            403,
            "Configure LOGIN and PASSWORD on the web app before managing MCP access",
        )
    if not host._is_ui_authenticated(request):
        raise _error(401, "Sign in to manage MCP access")
    if not mutation:
        return
    try:
        origin = urlsplit(request.headers.get("origin", ""))
        target = urlsplit(str(request.url))
        local = origin.hostname in ("localhost", "127.0.0.1", "::1")
        if local:
            local = bool(
                request.client and ipaddress.ip_address(request.client.host).is_loopback
            )
        # The browser's HTTPS Origin remains reliable when TLS terminates at a proxy.
        # Never take a replacement Host from X-Forwarded-Host.
        same_host = origin.hostname == target.hostname
        same_port = (origin.port or (443 if origin.scheme == "https" else 80)) == (
            target.port or (443 if origin.scheme == "https" else 80)
        )
        valid = (
            same_host
            and same_port
            and origin.scheme in ("http", "https")
            and not origin.username
            and not origin.password
            and not origin.path
            and not origin.query
            and not origin.fragment
            and (origin.scheme == "https" or local)
        )
    except (ValueError, UnicodeError):
        valid = False
    if (
        not valid
        or request.headers.get("sec-fetch-site", "same-origin")
        not in ("same-origin", "none")
        or request.headers.get("x-requested-with") != "XMLHttpRequest"
    ):
        raise _error(403, "MCP changes require a same-origin HTTPS browser request")
    if (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
    ):
        raise _error(415, "MCP changes require application/json")


async def _payload(host, request):
    _authorize(host, request, mutation=True)
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8192:
            raise _error(413, "MCP settings request is too large")
    try:
        value = json.loads(body)
    except (ValueError, UnicodeError) as exc:
        raise _error(400, "Invalid JSON settings") from exc
    if not isinstance(value, dict):
        raise _error(400, "MCP settings must be an object")
    return value


def router(host, store):
    routes = APIRouter()

    @routes.get("/api/mcp/connection")
    async def connection(request: Request):
        if not host.UI_AUTH_ENABLED:
            return JSONResponse(
                {
                    "enabled": False,
                    "managed": False,
                    "environment_managed": False,
                    "can_manage": False,
                    "public_url": "",
                    "token_available": False,
                    "message": "Configure LOGIN and PASSWORD on the web app before managing MCP access",
                },
                headers=PRIVATE_HEADERS,
            )
        _authorize(host, request)
        base = urlsplit(str(request.base_url))
        default_url = urlunsplit(
            ("https", base.netloc, base.path.rstrip("/") + "/mcp/", "", "")
        )
        return JSONResponse(
            {**store.metadata(default_url), "can_manage": True}, headers=PRIVATE_HEADERS
        )

    @routes.post("/api/mcp/connection/token")
    async def generate(request: Request):
        data = await _payload(host, request)
        result = store.generate(
            data.get("public_url"), data.get("replace_confirmed", False)
        )
        return JSONResponse(result, headers=PRIVATE_HEADERS)

    @routes.post("/api/mcp/connection/reveal")
    async def reveal(request: Request):
        await _payload(host, request)
        return JSONResponse(store.reveal(), headers=PRIVATE_HEADERS)

    @routes.post("/api/mcp/connection/revoke")
    async def revoke(request: Request):
        data = await _payload(host, request)
        return JSONResponse(
            store.revoke(data.get("confirmed", False)), headers=PRIVATE_HEADERS
        )

    return routes
