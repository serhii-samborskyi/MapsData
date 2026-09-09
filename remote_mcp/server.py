"""Official MCP SDK Streamable HTTP transport with a pre-shared bearer token."""

import hashlib
import hmac
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from typing import Annotated, Any
from urllib.parse import urlsplit
from uuid import UUID

import anyio
from mcp.server.auth.middleware.bearer_auth import (
    BearerAuthBackend,
    RequireAuthMiddleware,
)
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StrictBool,
    field_validator,
)
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware

from .contracts import CampaignInput, DraftError, PositiveID, TemplateKind
from .drafts import CampaignService


class MCPSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: SecretStr
    public_url: str

    @field_validator("token")
    @classmethod
    def validate_token(cls, value: SecretStr) -> SecretStr:
        token = value.get_secret_value()
        if len(token) < 32 or not token.isascii() or any(c.isspace() for c in token):
            raise ValueError(
                "MCP token must be at least 32 ASCII characters without whitespace"
            )
        return value

    @field_validator("public_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or not parsed.path.endswith("/")
        ):
            raise ValueError(
                "MCP public URL must be HTTPS, end in /, "
                "and have no credentials, query or fragment"
            )
        return value

    @classmethod
    def from_env(cls):
        return cls(
            token=os.environ.get("MAPSDATA_MCP_TOKEN", ""),
            public_url=os.environ.get("MAPSDATA_MCP_PUBLIC_URL", ""),
        )


class _StaticTokenVerifier(TokenVerifier):
    def __init__(self, settings: MCPSettings):
        self._digest = hashlib.sha256(
            settings.token.get_secret_value().encode()
        ).digest()
        self._resource = settings.public_url

    async def verify_token(self, token: str) -> AccessToken | None:
        actual = hashlib.sha256(token.encode()).digest()
        if not hmac.compare_digest(self._digest, actual):
            return None
        return AccessToken(
            token=token,
            client_id="mapsdata-mcp",
            scopes=["campaigns"],
            resource=self._resource,
        )


@dataclass
class RemoteMCP:
    sdk: FastMCP
    app: Starlette

    @asynccontextmanager
    async def lifespan(self):
        """Enter from the parent FastAPI lifespan (mounts do not run lifespans)."""
        async with self.sdk.session_manager.run():
            yield


def create_mcp_server(service: CampaignService, settings: MCPSettings) -> RemoteMCP:
    parsed = urlsplit(settings.public_url)
    sdk = FastMCP(
        "MapsData",
        stateless_http=True,
        json_response=True,
        streamable_http_path="/",
        instructions=(
            "Prepare a campaign and show its complete saved preview to the human. "
            "Launch only after the human explicitly confirms that preview. "
            "Do not invent approval or change/expand literal requests "
            "after confirmation. Prepared searches do not guarantee exhaustive "
            "business or advertising coverage."
        ),
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[parsed.netloc],
            allowed_origins=[f"{parsed.scheme}://{parsed.netloc}"],
        ),
    )

    async def invoke(method, *args):
        try:
            return await anyio.to_thread.run_sync(partial(method, *args))
        except DraftError as exc:
            raise ToolError(str(exc)) from None
        except Exception:
            # Provider/DB exceptions can contain URLs, credentials or source rows.
            raise ToolError(
                "Campaign service operation failed; check the host integration"
            ) from None

    read = ToolAnnotations(
        readOnlyHint=True, destructiveHint=False, openWorldHint=False
    )

    @sdk.tool(
        annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, openWorldHint=True
        )
    )
    async def generate_city_requests(
        state: Annotated[str, Field(min_length=1, max_length=100)],
        request_template: Annotated[str, Field(min_length=1, max_length=2000)],
    ) -> dict[str, Any]:
        """Generate literal city requests from the cached official Census dataset.

        Returns provenance and coverage limits; may fetch Census files.
        Does not create a preview or launch anything.
        """

        def generate():
            from geographic_requests import generate_city_requests as generate_requests

            return generate_requests(state, request_template)

        return await invoke(generate)

    @sdk.tool(annotations=read)
    async def list_templates(kind: TemplateKind) -> dict[str, Any]:
        """List source, enrichment, funnel, export or verification template metadata.

        Configurations are redacted.
        """
        return await invoke(service.list_templates, kind)

    @sdk.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False, destructiveHint=False, openWorldHint=False
        )
    )
    async def prepare_campaign(payload: CampaignInput) -> dict[str, Any]:
        """Persist a preview and frozen templates WITHOUT creating a campaign.

        Does not make provider requests or start workers.
        """
        return await invoke(service.prepare_campaign, payload)

    @sdk.tool(annotations=read)
    async def get_prepared_campaign(preview_id: UUID) -> dict[str, Any]:
        """Read saved literal requests, template hashes, expiry and launch state."""
        return await invoke(service.get_prepared_campaign, preview_id)

    @sdk.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=True,
            openWorldHint=True,
        )
    )
    async def launch_prepared_campaign(
        preview_id: UUID,
        preview_hash: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")],
        confirmed: StrictBool,
    ) -> dict[str, Any]:
        """Launch this exact saved preview ONLY after explicit human confirmation.

        All three arguments are required; confirmed must be true. Retry the same
        preview to retrieve its original campaign/run IDs.
        """
        return await invoke(
            service.launch_prepared_campaign, preview_id, preview_hash, confirmed
        )

    @sdk.tool(annotations=read)
    async def get_campaign_status(campaign_id: PositiveID) -> dict[str, Any]:
        """Get campaign state and counters.

        Contacts, logs, URLs and provider configuration are omitted.
        """
        return await invoke(service.get_campaign_status, campaign_id)

    @sdk.tool(annotations=read)
    async def get_run_status(run_id: PositiveID) -> dict[str, Any]:
        """Get run state and counters.

        Contacts, logs, URLs and provider configuration are omitted.
        """
        return await invoke(service.get_run_status, run_id)

    @sdk.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=True,
            openWorldHint=False,
        )
    )
    async def stop_campaign(campaign_id: PositiveID) -> dict[str, Any]:
        """Request that the campaign and its active child jobs stop.

        Poll status until workers acknowledge cancellation.
        """
        return await invoke(service.stop_campaign, campaign_id)

    app = sdk.streamable_http_app()
    # The SDK owns authentication parsing, MCP routing, negotiation and messages.
    # This is a pre-shared-token deployment, with no OAuth discovery/login server.
    app.user_middleware = [
        Middleware(
            AuthenticationMiddleware,
            backend=BearerAuthBackend(_StaticTokenVerifier(settings)),
        ),
        *app.user_middleware,
    ]
    for route in app.routes:
        route.app = RequireAuthMiddleware(route.app, required_scopes=["campaigns"])
    return RemoteMCP(sdk=sdk, app=app)
