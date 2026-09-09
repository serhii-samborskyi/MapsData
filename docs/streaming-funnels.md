# Streaming funnels

## Enable and run

Deploy the updated web app and its dependencies. Database initialization adds
the queue tables and execution-mode columns automatically; existing funnels and
runs default to `batch`.

In Funnel Manager, select **Streaming** when saving a funnel, or override the
mode for a single campaign launch. Select the existing source and funnel, then
optionally override the export template and SendRead A/B list ID. The list must
already exist; an assistant can create it through the cold-email app's own MCP
before preparing the MapsData campaign.

For remote campaign planning and client configuration, see
[Remote MCP](remote-mcp.md). MCP is disabled until its environment token is set.

## Processing

- New contacts enter durable PostgreSQL queues continuously while scraping runs.
- Each contact follows the enabled steps in order: sourcing, enrichment, DNS,
  email verification, export. Different contacts can occupy different stages at
  the same time. The UI displays counts for each stage.
- HTTP sources execute in the web app. Browser/XPath and Google Maps sources
  still require the daemon. Update the daemon too when using streaming mode:
  website email extraction is leased per contact rather than run campaign-wide.
- Source-only campaigns proceed as soon as their contact rows are saved.
  Campaigns that also collect website emails wait for each contact's extraction
  result before enrichment. CSV/XLSX imports do not start scraping.
- Streaming skips the legacy campaign-wide destructive cleanup stage. Running
  cleanup or manually editing contacts during an active funnel is discouraged.
  A changed enrichment input is retried; changed email/domain inputs invalidate
  their old verification results before export.
- An existing active batch scraping pipeline is not converted in place. A
  streaming funnel attached to it waits for review; let that pipeline finish,
  then resume the streaming funnel.
- Each run keeps frozen source and step template settings. Editing a saved
  template does not change a running streaming funnel.

Export batch size is a **per-request maximum**, not a total-contact limit. A
partially filled batch becomes eligible within about 10 seconds. Provider rate
limits, backoff and any required confirmation can delay dispatch. Shared endpoint
limits coordinate streaming runs, and prompt enrichment shares its existing
limiter with batch enrichment. Small batch timing is not a delivery-time SLA.

Only contacts matching the saved export filters are sent. Saved mappings,
including fields explicitly set to Ignore, are preserved. Disabling confirmation
allows automatic export; otherwise use **Confirm Export** for that run first.

## Stop and recovery

**Stop Funnel** stops new work and leaves completed contact values intact. It
also stops further sourcing for a streaming campaign. Calls already dispatched
cannot be undone, and their final delivery receipts can still arrive after Stop.

Each contact step defaults to two retries after its first attempt, using the
funnel's retry setting. A permanently rejected contact or exhausted retry budget
does not prevent other contacts from progressing. Export authentication failures
can pause the run for review instead of repeatedly sending bad requests.

Workers use leases and a durable queue. After a restart, the web scheduler
recovers unfinished work. A lost worker cannot commit stale contact values or
dispatch an export with an expired lease.

An export timeout is not proof that the destination rejected the contacts.
Uncertain deliveries pause the run and appear under **Review Deliveries**.
Check the destination, then confirm Delivered or Not Delivered for each item.
Resume processing separately once review is complete. Stopped runs remain
stopped after review. Do not choose Not Delivered without checking: a provider
may have accepted a request even when its response was lost.

Delivery receipts prevent automatic re-export of the same contact to the same
destination. They do not deduplicate different contact rows or guarantee
exactly-once delivery across external systems without provider idempotency.
Pending queues, receipts and frozen templates are retained in the app database;
protect that database and backups like the existing credential-bearing templates.

## Verification

The tests use mocked external providers and disposable PostgreSQL databases.
No real leads are scraped, verified or exported by the test suite. Run the
database integration files separately from legacy tests that install import
stubs, using `STREAM_TEST_DATABASE_URL` for streaming tests and the MCP test DSN
documented in the remote MCP guide. Do not point test variables at production.
