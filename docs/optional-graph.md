# Graphiti / FalkorDB extension (retired)

The optional Graphiti extension — FalkorDB, Graphiti MCP, the Graphiti projector and their
graph-only model downloads — was removed from Gbuzz on 29 Oct 2026. It had been disabled on the
lab since September and depended on a second database and an external graph runtime.

What replaces it, all in PostgreSQL:

- **Graph:** `gcor.nodes` / `gcor.edges` with the relationship projector (Concept and Inference
  nodes) and graph expansion during retrieval.
- **Temporal facts:** bi-temporal `gcor.facts` (migration 0018) with supersession, contradiction
  and retraction, queryable as of any valid time or record time (`/api/facts/history`).

## What remains

- Migrations 0005 and 0006 and the `gcor.graphiti_projection` table are kept, because applied
  migrations are never rewritten. The proxy still records one row per knowledge entry there and
  reports them in `/api/sessions` and `/api/graphiti/status`; nothing consumes them. Retiring that
  bookkeeping changes the session API and is a separate change.
- `proxy/main.py` keeps an empty `graph_candidates` hook in retrieval. A future PostgreSQL
  shared-concept leg plugs in there; the integration suite already proves that whatever it returns
  is re-authorised by channel, access level, approval, evidence and reader filters.

## Existing installations

No action is needed when the graph was not enabled. A deployment that ran it can stop and remove
the containers (`falkordb`, `graphiti-mcp`, `graphiti-projector`) and, once a backup cycle has
completed, delete the `buzz-falkordb-data` volume: it only held a rebuildable projection.
`scripts/update-stack.ps1`, `scripts/production-preflight.ps1` and
`scripts/verify-production-pins.ps1` refuse `-IncludeGraph`, and `scripts/ComposePlan.psm1`
already refuses graph overlays and the `graph` profile.
