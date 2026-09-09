"""Remote MCP integration, with no worker startup or database access on import."""

from .contracts import CampaignInput, ServiceHooks
from .drafts import CampaignService, init_schema
from .server import MCPSettings, RemoteMCP, create_mcp_server

__all__ = [
    "CampaignInput",
    "CampaignService",
    "MCPSettings",
    "RemoteMCP",
    "ServiceHooks",
    "create_mcp_server",
    "init_schema",
]
