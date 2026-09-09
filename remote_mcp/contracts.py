"""Synchronous service hooks, called in a worker thread with a transaction cursor."""

from dataclasses import dataclass
from typing import Annotated, Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

TemplateKind = Literal["source", "enrichment", "funnel", "export", "verification"]
PositiveID = Annotated[int, Field(strict=True, gt=0)]
RequestText = Annotated[str, Field(strict=True, min_length=1, max_length=2000)]


class CampaignInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    requests: list[RequestText] = Field(min_length=1, max_length=10000)
    source_template_id: PositiveID | None = None
    funnel_template_id: PositiveID
    export_template_id: PositiveID | None = None
    execution_mode: Literal["batch", "streaming"] | None = None
    sendread_ab_list_id: str | None = Field(default=None, min_length=1, max_length=200)
    maps_scrape_mode: Literal["fast", "slow"] = "slow"
    scrape_maps_only: StrictBool = False

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, value: str) -> str:
        if not value.strip() or "\x00" in value:
            raise ValueError("Name must be nonblank and contain no NUL")
        return value

    @field_validator("requests")
    @classmethod
    def literal_requests(cls, values: list[str]) -> list[str]:
        if any(
            not value.strip() or any(c in value for c in "\r\n\x00") for value in values
        ):
            raise ValueError("Each request must be one nonblank line without NUL")
        # Preserve spelling, order, duplicates and whitespace exactly as submitted.
        return values


class TemplateSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: TemplateKind
    id: PositiveID | None
    name: str
    configuration: dict[str, Any]


class LaunchResult(BaseModel):
    # Extra server-only values (e.g. worker IDs) are persisted for after_commit.
    model_config = ConfigDict(extra="allow")

    campaign_id: PositiveID
    run_id: PositiveID | None = None


@dataclass(frozen=True)
class ServiceHooks:
    """Hooks must not commit, start workers, or perform network IO with a cursor.

    prepare returns the JSON plan from streaming_campaigns.prepare (see docs).
    launch receives that exact object and inserts from its frozen configurations.
    after_commit must be idempotent; it is also called on launch retries.
    list/status/stop return dicts; the facade projects only safe metadata.
    """

    prepare: Callable[[Any, dict], dict]
    launch: Callable[[Any, dict], dict]
    after_commit: Callable[[dict], None]
    list_templates: Callable[[Any, TemplateKind], list[dict]]
    get_campaign_status: Callable[[Any, int], dict]
    get_run_status: Callable[[Any, int], dict]
    stop_campaign: Callable[[Any, int], dict]


class DraftError(Exception):
    """A fixed, client-safe error. Never use provider exceptions as its message."""
