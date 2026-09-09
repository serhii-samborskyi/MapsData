# Remote MCP for MapsData

This integration exposes the hosted app over the official MCP Python SDK's
Streamable HTTP transport. Codex CLI and Claude Code connect directly to the
hosted URL. No local server, shell script, stdio bridge, or template editing tool
is involved. The host wiring is installed through `mcp_integration.install` at
the end of `main.py`, and database initialization creates the draft schema.
The isolated MCP package does not import `main`.

## Deploy the wired host

No further mount or startup changes are needed. Set `MAPSDATA_MCP_TOKEN` and
`MAPSDATA_MCP_PUBLIC_URL` in the host environment, then restart the app. Without
a token, the host leaves MCP unmounted. With a token, the endpoint is `/mcp/`;
the existing default-template startup handler still runs. Drafts share the app's
PostgreSQL database and survive application restarts.

The lock is generated with Poetry 1.7.1 in lock format 2.0, matching the existing
Python 3.11 Dockerfile. Keep the production install command
`poetry install --no-root --only main`; the `test` dependency group is optional.
The mount/lifespan example later in this document describes the installed
integration contract; do not add a second mount or lifespan wrapper.

## Connect a client

After the integration below is deployed:

1. Generate a random token with your password manager (at least 32 characters),
   or run `openssl rand -hex 32` once.
2. In your hosting provider's environment/secrets settings, set
   `MAPSDATA_MCP_TOKEN` to that token and `MAPSDATA_MCP_PUBLIC_URL` to your HTTPS
   endpoint, for example `https://maps.example.com/mcp/`. Keep the trailing slash.
3. Restart the hosted app. Make the same token available as
   `MAPSDATA_MCP_TOKEN` in the environment where you open your MCP client.

For Codex CLI, add the hosted server once:

```bash
codex mcp add mapsdata --url https://maps.example.com/mcp/ --bearer-token-env-var MAPSDATA_MCP_TOKEN
```

Equivalent Codex configuration:

```toml
[mcp_servers.mapsdata]
url = "https://maps.example.com/mcp/"
bearer_token_env_var = "MAPSDATA_MCP_TOKEN"
```

For Claude Code, the following `.mcp.json` entry references the environment
variable without storing the token in the project:

```json
{
  "mcpServers": {
    "mapsdata": {
      "type": "http",
      "url": "https://maps.example.com/mcp/",
      "headers": {"Authorization": "Bearer ${MAPSDATA_MCP_TOKEN}"}
    }
  }
}
```

Open the client's MCP server list to verify the connection. First ask it to list
your source and funnel templates, then prepare a campaign. Review the returned
requests, steps, execution mode and export destination before explicitly asking
it to launch that preview.

These settings follow [Codex's MCP documentation](https://developers.openai.com/codex/mcp/)
and [Claude Code's HTTP transport and environment expansion documentation](https://code.claude.com/docs/en/mcp).

## Host integration

Install the updated Poetry dependencies. `mcp>=1.30,<2` selects the maintained
v1 SDK API. Its Uvicorn minimum requires the accompanying Uvicorn pin change;
FastAPI's existing version constraint is retained. Protocol handling comes from
the [official Python SDK](https://github.com/modelcontextprotocol/python-sdk/tree/v1.x).

Call `remote_mcp.init_schema(cursor)` at the end of the host database initializer,
before its normal commit. This creates only `mcp_campaign_drafts`. It does not
modify campaign, request, template or worker tables.

Build the service using synchronous hooks. Each cursor hook runs in a thread,
with a connection created by the supplied `get_db` context manager. That context
manager must close/rollback uncommitted connections, as the existing one does.

```python
from contextlib import asynccontextmanager
import sys
import streaming_campaigns
from remote_mcp import CampaignService, MCPSettings, ServiceHooks, create_mcp_server

host = sys.modules[__name__]  # In the main integration module.
hooks = ServiceHooks(
    prepare=lambda cursor, payload: streaming_campaigns.prepare(cursor, host, payload),
    launch=lambda cursor, plan: streaming_campaigns.launch(cursor, host, plan),
    after_commit=wake_campaign_workers,
    list_templates=list_mcp_template_rows,
    get_campaign_status=load_mcp_campaign_status,
    get_run_status=load_mcp_run_status,
    stop_campaign=request_mcp_campaign_stop,
)
remote = create_mcp_server(CampaignService(get_db, hooks), MCPSettings.from_env())

# Preserve the existing startup/shutdown handlers and lifespan.
previous_lifespan = app.router.lifespan_context

@asynccontextmanager
async def lifespan(app):
    async with previous_lifespan(app):
        async with remote.lifespan():
            yield

app.router.lifespan_context = lifespan
app.mount("/mcp", remote.app)
```

The named host callbacks in this example must be supplied by main integration.
The mounted child serves `/`, so the public URL is `/mcp/`. FastAPI does not run
mounted application lifespans automatically: enter `remote.lifespan()` exactly
once for each server instance. Preserve the other startup handlers.

Keep the hosted endpoint behind HTTPS. The proxy must preserve the configured
Host and Authorization headers; Origin, when supplied, must match the public
origin. The SDK limits request bodies to 4 MiB by default. Existing UI login
middleware must allow requests to reach the MCP mount; the MCP SDK enforces its
own bearer authentication, independently of UI cookies. Do not make other API
routes public as part of that exception.

The token is a deployment-wide credential: holders can see template metadata and
operate campaigns in this app. There is no tenant isolation or per-user scope
model. Invalid/missing tokens receive HTTP 401, including GET and DELETE. Tokens
in query parameters or cookies cannot authenticate. This is a pre-shared-token
deployment with manual header configuration, not an OAuth authorization server.
Rotating the environment token and restarting invalidates the old token while
preserving all drafts and launched campaign IDs.

## Hook contract

| Hook | Input | Return / responsibility |
| --- | --- | --- |
| `prepare` | `cursor, payload` | JSON plan from `streaming_campaigns.prepare`; validate and freeze only |
| `launch` | `cursor, frozen_plan` | Insert campaign, requests and frozen automation run; return `campaign_id`, `run_id`, optional `execution_mode` and private worker fields |
| `after_commit` | Saved launch result | Idempotently wake the relevant batch/streaming/source workers; no open draft transaction |
| `list_templates` | `cursor, kind` | List of dictionaries with integer `id` (null for built-in Maps), safe display `name`, optional `enabled`, `source_type`, `service`, `execution_mode` |
| `get_campaign_status` | `cursor, campaign_id` | Flat dictionary of state and counters |
| `get_run_status` | `cursor, run_id` | Flat dictionary of state and counters; `run_id` means automation run ID |
| `stop_campaign` | `cursor, campaign_id` | Persist cancellation flags for campaign and all child jobs; return state and `stop_requested` |

Cursor hooks must **never commit, roll back, start workers, or perform runtime
network IO**. Preparation and reads use read-only transactions. Preparation also
uses repeatable read so all template snapshots come from one database view.
The facade persists the finished draft in a separate write transaction. Launch
and stop hooks may write, but the facade owns their commits. Hooks must validate
local templates without calling provider schema, account, or destination APIs.

`prepare` receives the validated fields of `CampaignInput`: `name`, `requests`,
required `funnel_template_id`, optional `source_template_id`, `export_template_id`,
`sendread_ab_list_id`, `execution_mode` (`batch`/`streaming`, or null to use the
funnel default), `maps_scrape_mode`, and `scrape_maps_only`. Limits are 10,000
requests, 2,000 characters per request, and one nonblank line per request.
Enrichment/verification templates are selected by the funnel, not independent
MCP overrides.

New campaigns require a funnel with an enabled `pipeline` step to start sourcing
the submitted requests. Preparation rejects a missing or disabled pipeline in
both execution modes, before saving a draft or creating any campaign work.
This restriction does not apply to ordinary UI funnel runs on existing contacts.

The frozen plan uses the existing `streaming_campaigns` shape:

```text
name, requests, source_template_id, source_snapshot,
funnel_template_id, funnel_name, execution_mode, default_retry_count,
maps_scrape_mode, scrape_maps_only, steps, export
```

Every enabled non-pipeline step has `config.template_snapshot`. The facade hashes
the source snapshot, the effective funnel settings/steps, and each referenced
enrichment, verification or export template. A SHA-256 preview hash covers the
whole JSON plan and original submitted payload, including private configuration.
The plan must be JSON-serializable; convert database timestamps/decimals in the
host helper. Launch receives this exact persisted plan, without re-resolving
mutable template rows. `_create_automation_run(..., frozen_plan=plan)` must use
the frozen steps for both batch and streaming execution.

The original submitted requests remain in `submitted_requests`. The effective
`campaign.requests` list is exactly the helper's trim-and-deduplicate result.
The facade rejects newly generated or expanded requests from a preparation
hook. To expand niche keywords, submit each desired literal request explicitly
and review the resulting preview. Both lists are covered by its hash.

Public output includes selected IDs, names, hashes, step types/order, execution
mode, export destination and request counts. The export view resolves the actual
default campaign/A-B list from the frozen template when no override is provided.
It also includes effective filter toggles, field mappings and the requirement for
a separate export confirmation. Mapping values containing known frozen credentials,
bearer tokens or URLs are redacted. It omits API configs, API URLs/queries,
contacts, provider responses, logs and raw errors. Host-provided display names and destination identifiers must themselves
be safe metadata. Frozen plans and saved private worker fields contain secrets
and stay in PostgreSQL; apply the same database/backup access controls used for
the app's existing credential-bearing template tables.

Status projection accepts `campaign_id`, `run_id`, `status`, `execution_mode`,
`stop_requested`, `source_closed`, and
these integer counters: `total_requests`, `pending_requests`, `completed_requests`,
`failed_requests`, `total_contacts`, `processed_contacts`, `enriched_contacts`,
`failed_contacts`, `skipped_contacts`, `exported_contacts`, `total_steps`, `completed_steps`. States
include active/pending/queued/running/paused/completed/failed, canceled/cancelled,
stopping/stopped, waiting_confirmation, idle, inactive, not_started and
completed_with_errors; other strings become `unknown`.
`stream_progress` preserves up to 32 step rows, with `step_type`, `step_order` and
nonnegative integer counts no greater than `2**53-1` for pending/queued/running,
retry/blocked/uncertain, completed/failed/skipped/cancelled/canceled/exported/total.
In `streaming.progress` output, the export step's `completed` count indicates
completed export tasks. Unknown row fields, errors and contact data are omitted.
Return a flat status dictionary rather than a nested API response. Raw hook
errors become a fixed MCP error so provider credentials cannot escape.

## Approval, retries and stopping

`prepare_campaign` never inserts a live campaign. `get_prepared_campaign` can
retrieve a persisted preview after a process restart. Drafts expire after 24
hours by default (configurable on `CampaignService`).

The separate `launch_prepared_campaign` tool requires all three arguments:
`preview_id`, `preview_hash`, and boolean `confirmed`. Missing, false, string or
numeric confirmation cannot launch. Clients must show the complete preview and
obtain explicit human confirmation before setting `confirmed: true`. The boolean
is the client's acknowledgement of that confirmation; the server cannot prove
that a human clicked a client UI. No MCP tool issues or edits approvals/templates.

Launch locks the draft row with `SELECT FOR UPDATE`, creates the campaign/run,
and records the result in the same PostgreSQL transaction. Concurrent or repeated
launches of the same draft return the original campaign/run IDs, including after
draft expiry once it has launched. A failed launch rolls back all its inserts.
Changed or corrupt plans fail integrity checks. Changing a saved template later
does not change an already prepared plan; prepare a new preview to use updates.
Disabling a funnel also leaves previously prepared plans launchable: the frozen
steps, snapshots, execution mode, retry count and funnel name are used. Disabled
funnels cannot be selected for new previews or ordinary, non-frozen starts.
The original funnel row must still exist because runs retain its foreign key.
Deleting it before launch returns a safe error and rolls back the campaign,
requests and run; prepare a new preview using an existing funnel. The current
schema cascades funnel deletion to existing runs, so do not delete funnels whose
runs must be retained. Disabling a funnel is not a cancellation mechanism; use
`stop_campaign` to stop launched work.

`after_commit` runs only after commit and is called again on launch retries. If
waking workers fails, launch still returns the saved IDs with
`worker_wake_pending: true`; retry the same launch to wake them. Main's normal
startup recovery should also scan durable queued runs, covering a process crash
between commit and wakeup. Never create a second campaign to retry a wakeup.

`stop_campaign` persists a stop request; workers must poll and honor cancellation
for both batch and streaming jobs. Return `stopping` until they acknowledge it.
It cannot undo already completed provider calls or exports. The MCP facade does
not call the export-confirmation endpoint or independently approve an export.
The preview's effective export destination and the host funnel's separate export
confirmation behavior both remain relevant.

## Verification

Run MCP tests separately from the legacy tests that install global FastAPI/DB
stubs in `sys.modules`:

```bash
poetry run python -m pytest tests/remote_mcp -q
```

For real PostgreSQL persistence/transaction tests, set `MAPSDATA_MCP_TEST_DSN` to
a disposable PostgreSQL database and run the same command. Tests create and
remove uniquely named schemas; they never use `DATABASE_URL`. Without the test
DSN, PostgreSQL cases skip and authentication/protocol/validation tests still run.

Actual host integration tests live in `tests/test_mcp_host_integration.py`.
Run that file separately with `MAPSDATA_MCP_TEST_DSN` set to a dedicated test
database such as `mapsdata_mcp_test`. The tests refuse the `postgres` database
because an independently running app scheduler could claim queued test runs.
They create unique schemas, import the real host, exercise its lifespan, and
intercept worker startup and external HTTP. Run host imports serially.

## Geographic scope

`generate_city_requests(state, request_template)` delegates to the separately
maintained `geographic_requests.generate_city_requests` helper and returns its
literal requests, request count, provenance and coverage metadata unchanged.
The tool has `readOnlyHint=true` and `openWorldHint=true`: a cache miss may fetch
official Census data, outside any campaign database transaction. It does not
prepare or launch anything. Submit the chosen generated requests explicitly to
`prepare_campaign`, then review and confirm that persisted preview. No tool
claims national business or advertising completeness.
The [official US Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
provide maintained place identifiers/names and coordinates. The loader's cache
should identify an explicitly selected release and record the Census download URL,
release year, retrieval time, archive/content SHA-256, selected states/place types,
parser version and record count. Keep that provenance with generated literal
requests. Census places are geographic entities, not an exhaustive inventory of
businesses or ads, and Gazetteer files do not include population counts. A future
population filter needs a separately identified population dataset.
