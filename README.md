# Qingflow MCP (CRUD)

This MCP server exposes a canonical, agent-native public surface:

- `qf_tool_spec_get`
- `qf_form_get`
- `qf_field_resolve`
- `qf_value_probe`
- `qf.query.plan`
- `qf.query.rows`
- `qf.query.record`
- `qf.query.aggregate`
- `qf.query.export`
- `qf.records.mutate`

Legacy tools still exist internally for compatibility logic, but they are no longer advertised via `listTools()`.

It intentionally excludes delete for now.

## Setup

Runtime requirement:

- Node.js `>=18`

1. Install dependencies:

```bash
npm install
```

2. Set environment variables:

```bash
export QINGFLOW_BASE_URL="https://api.qingflow.com"
export QINGFLOW_ACCESS_TOKEN="your_access_token"
```

Optional:

```bash
export QINGFLOW_FORM_CACHE_TTL_MS=300000
export QINGFLOW_REQUEST_TIMEOUT_MS=18000
export QINGFLOW_EXECUTION_BUDGET_MS=20000
export QINGFLOW_ADAPTIVE_PAGING=1
export QINGFLOW_ADAPTIVE_MIN_PAGE_SIZE=20
export QINGFLOW_ADAPTIVE_TARGET_PAGE_MS=1200
export QINGFLOW_EXPORT_MAX_ROWS=10000
export QINGFLOW_EXPORT_DIR="/tmp/qingflow-mcp-exports"
```

## Run

Development:

```bash
npm run dev
```

Build and run:

```bash
npm run build
npm start
```

Run tests:

```bash
npm test
```

## Command Line Usage

`qingflow-mcp` still defaults to MCP stdio mode:

```bash
qingflow-mcp
```

Use CLI mode for quick local invocation:

```bash
# list all available tools
qingflow-mcp cli tools

# machine-readable tool list
qingflow-mcp cli tools --json

# canonical plan -> execute
qingflow-mcp cli call qf.query.plan --args '{
  "kind":"rows",
  "query":{
    "app_key":"your_app_key",
    "select":[1001,1002],
    "where":[{"field":1003,"op":"between","from":"2026-01-01","to":"2026-01-31"}],
    "limit":20
  }
}'

# then execute with returned plan_id
qingflow-mcp cli call qf.query.rows --args '{"plan_id":"plan_xxx"}'
```

## CLI Install

Global install from GitHub:

```bash
npm i -g git+https://github.com/853046310/qingflow-mcp.git
```

Install from npm (pinned version):

```bash
npm i -g qingflow-mcp@0.7.0
```

Or one-click installer:

```bash
curl -fsSL https://raw.githubusercontent.com/853046310/qingflow-mcp/main/install.sh | bash
```

Safer (review script before execution):

```bash
curl -fsSL https://raw.githubusercontent.com/853046310/qingflow-mcp/main/install.sh -o install.sh
less install.sh
bash install.sh
```

MCP client config example:

```json
{
  "mcpServers": {
    "qingflow": {
      "command": "qingflow-mcp",
      "env": {
        "QINGFLOW_BASE_URL": "https://api.qingflow.com",
        "QINGFLOW_ACCESS_TOKEN": "your_access_token"
      }
    }
  }
}
```

## Recommended Flow

1. `qf_apps_list` to pick app.
2. `qf_form_get` to inspect field ids/titles.
3. `qf_field_resolve` for field-name to `que_id` mapping.
4. `qf_value_probe` when the agent needs candidate field values and explicit match evidence.
5. `qf_record_create` or `qf_record_update`.
6. If create/update returns only `request_id`, call `qf_operation_get` to resolve async result.

Full calling contract (Chinese):

- [MCP 调用规范](./docs/MCP_CALLING_SPEC.md)
- [vNext Agent-Native 设计稿](./docs/VNEXT_AGENT_NATIVE_DESIGN.md)

## Canonical Usage

Public agent flow is now:

1. `qf_form_get` / `qf_field_resolve` when field mapping is unclear
2. `qf_value_probe` when field value candidates are unclear
3. `qf.query.plan`
4. Execute the returned `plan_id` with:
   - `qf.query.rows`
   - `qf.query.record`
   - `qf.query.aggregate`
   - `qf.query.export`
   - `qf.records.mutate`

### Planner Example

```json
{
  "kind": "aggregate",
  "query": {
    "app_key": "your_app_key",
    "where": [
      { "field": 1003, "op": "between", "from": "2026-01-01", "to": "2026-01-31" }
    ],
    "group_by": [1003],
    "metrics": [
      { "op": "count" },
      { "column": 1002, "op": "sum" }
    ],
    "strict_full": true
  }
}
```

### Execute Example

```json
{
  "plan_id": "plan_xxx"
}
```

Rules:

1. Public execute tools require `plan_id`.
2. Optional `query` / `action` echo is only used for drift checking.
3. If execute input drifts from the planned canonical query, the server returns `PLAN_DRIFT`.
4. Public filtering uses canonical `where[]`; legacy `filters` are no longer part of the public contract.

### Aggregate Business Counts

Aggregate business summaries use one canonical count contract:

```json
{
  "summary": {
    "counts": {
      "source_record_count": 370,
      "group_assignment_count": 405,
      "metric_nonnull_record_count": 395
    },
    "primary_metric_total": 12272931.75,
    "primary_metric_missing_count": 10
  }
}
```

Default answer for “多少单/多少条” must read `summary.counts.source_record_count`.

### Completeness

Canonical `completeness` is technical-only:

- `is_complete`
- `raw_scan_complete`
- `scan_limit_hit`
- `fetched_pages`
- `requested_pages`
- `actual_scanned_pages`
- `scanned_pages`
- `scan_limit`
- `has_more`
- `next_page_token`
- `stop_reason`
- `output_truncated`
- `omitted_items`
- `omitted_chars`

When `strict_full=true`, any incomplete result fails with `INCOMPLETE_RESULT`.

### Error Protocol

Failures return structured JSON with a machine-readable `error.code`, for example:

```json
{
  "ok": false,
  "error": {
    "code": "PLAN_REQUIRED",
    "message": "...",
    "fix_hint": "...",
    "retryable": true
  }
}
```

Common codes:

- `PLAN_REQUIRED`
- `PLAN_NOT_READY`
- `PLAN_DRIFT`
- `FORBIDDEN_RUNTIME_ALIAS`
- `VALIDATION_ERROR`
- `INCOMPLETE_RESULT`
- `UPSTREAM_TIMEOUT`
- `UPSTREAM_API_ERROR`

## Troubleshooting

If you see runtime errors around `Headers` or missing web APIs:

1. Upgrade Node to `>=18`.
2. Upgrade package to latest:

```bash
npm i -g qingflow-mcp@latest
```

3. Verify runtime:

```bash
node -e "console.log(process.version, typeof fetch, typeof Headers)"
```

## Publish

```bash
npm login
npm publish
```

If you publish under an npm scope, use:

```bash
npm publish --access public
```

## Security Notes

1. Keep `QINGFLOW_ACCESS_TOKEN` only in runtime env vars; do not commit `.env`.
2. Rotate token immediately if it appears in screenshots, logs, or chat history.

## Community

- Contributing: [CONTRIBUTING.md](./CONTRIBUTING.md)
- Security: [SECURITY.md](./SECURITY.md)
- Conduct: [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)
