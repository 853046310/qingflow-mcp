# Qingflow MCP (CRUD)

This MCP server wraps Qingflow OpenAPI for:

- `qf_apps_list`
- `qf_form_get`
- `qf_field_resolve`
- `qf_query_plan`
- `qf_records_list`
- `qf_record_get`
- `qf_records_batch_get`
- `qf_export_csv`
- `qf_export_json`
- `qf_query` (unified read entry: list / record / summary)
- `qf_records_aggregate` (deterministic grouped metrics)
- `qf_record_create`
- `qf_record_update`
- `qf_operation_get`

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

# call one tool with JSON args
qingflow-mcp cli call qf_apps_list --args '{"limit":5}'

# call from stdin
echo '{"app_key":"your_app_key","mode":"all","select_columns":[1001]}' \
  | qingflow-mcp cli call qf_query
```

## CLI Install

Global install from GitHub:

```bash
npm i -g git+https://github.com/853046310/qingflow-mcp.git
```

Install from npm (pinned version):

```bash
npm i -g qingflow-mcp@0.3.13
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
3. `qf_record_create` or `qf_record_update`.
4. If create/update returns only `request_id`, call `qf_operation_get` to resolve async result.

Full calling contract (Chinese):

- [MCP 调用规范](./docs/MCP_CALLING_SPEC.md)

## Unified Query (`qf_query`)

`qf_query` is the recommended read entry for agents.

1. `query_mode=auto`:
   - if `apply_id` is set, route to single-record query.
   - if summary params are set (`amount_column` / `time_range` / `stat_policy` / `scan_max_pages`), route to summary query.
   - otherwise route to list query.
2. `query_mode=list|record|summary` forces explicit behavior.
3. In `list` mode, `time_range` is translated to list filters when `from` or `to` is provided.
4. In `list` mode, `select_columns` is required.
5. In `list` mode, row cap defaults to 200 when `max_rows` and `max_items` are omitted.
6. In `record` mode, `select_columns` is required.
7. In `summary` mode, `select_columns` is required (`max_rows` defaults to 200 when omitted).

Summary mode output:

1. `summary`: aggregated stats (`total_count`, `total_amount`, `by_day`, `missing_count`).
2. `rows`: strict column rows (only requested `select_columns`).
3. `meta`: field mapping, filter scope, stat policy, execution limits (`output_profile=verbose` only).

Return shape:

1. success: structured payload `{ "ok": true, "data": ... }` (`meta` only in `output_profile=verbose`)
2. failure: MCP `isError=true`, and text content is JSON payload like `{ "ok": false, "message": ..., ... }`
3. incomplete strict queries fail with `{ "code": "NEED_MORE_DATA", "status": "need_more_data", ... }`

Deterministic read protocol (list/summary/aggregate):

1. output profile:
   - default `output_profile=compact`: return core data only (`rows/row/groups/summary` + `next_page_token`)
   - `output_profile=verbose`: include full contract (`completeness` + `evidence` + `meta`)
   - exception: `qf_query(summary)` and `qf_records_aggregate` always return `completeness`, even in `compact`, so agents can block on incomplete statistics
2. when `output_profile=verbose`, `completeness` fields are:
   - `result_amount`
   - `returned_items`
   - `fetched_pages`
   - `requested_pages`
   - `actual_scanned_pages`
   - `has_more`
   - `next_page_token`
   - `is_complete`
   - `partial`
   - `omitted_items`
   - `omitted_chars`
3. when `output_profile=verbose`, `evidence` fields are:
   - `query_id`
   - `app_key`
   - `filters`
   - `selected_columns`
   - `time_range`
   - `source_pages`
4. `strict_full=true` makes incomplete results fail fast with `NEED_MORE_DATA`.
   - for `qf_query(summary)`, `strict_full` enforces raw source scan completeness; sample rows may still be capped by `max_rows`, which is reflected by `output_page_complete=false`
5. Error payloads expose `error_code` and `fix_hint` for actionable retries.
6. Parameter tolerance supports stringified JSON and numeric/boolean strings for key query fields.

For `qf_query(summary)` and `qf_records_aggregate`, read `data.summary.completeness` / `data.completeness` before concluding:

1. `raw_scan_complete=false`: source data is not fully scanned, do not produce a final conclusion.
2. `scan_limit_hit=true`: query stopped because scan budget was hit.
3. `output_page_complete=false`: source may be complete, but output was truncated by `max_rows` or `max_groups`.
4. `raw_next_page_token`: use this token to continue raw scan pagination (`next_page_token` remains as a backward-compatible alias).

## List Query Tips

Strict mode (`qf_records_list`):

1. `select_columns` is required.
2. `include_answers=false` is not allowed.
3. Output is flat `rows[]` (no raw `answers` payload).

1. For `qf_records_list.sort[].que_id`, use a real field `que_id` (numeric) or exact field title from `qf_form_get`.
2. Avoid aliases like `create_time`; Qingflow often rejects them.
3. Use `max_rows` (or `max_items`) to cap returned rows. Default row cap is 200.
4. Use `max_columns` to cap returned columns per row.
5. Use `select_columns` to return only specific columns (supports `que_id` or exact field title).
6. The server may still trim by response-size guardrail (`QINGFLOW_LIST_MAX_ITEMS_BYTES`) when payload is too large.
7. Use `requested_pages` and `scan_max_pages` for deterministic page scan.
8. Continue with `page_token` from previous `next_page_token`.
9. Column limits: `select_columns <= 2`, `max_columns <= 2`.

Example:

```json
{
  "app_key": "your_app_key",
  "mode": "all",
  "page_size": 50,
  "requested_pages": 1,
  "scan_max_pages": 1,
  "include_answers": true,
  "max_rows": 10,
  "max_columns": 2,
  "select_columns": [1, "客户名称"],
  "output_profile": "compact",
  "strict_full": false
}
```

For single record details (`qf_record_get`), the same column controls are supported:

```json
{
  "apply_id": "497600278750478338",
  "max_columns": 2,
  "select_columns": [1, "客户名称"],
  "output_profile": "compact"
}
```

`qf_record_get` requires `select_columns`.

Aggregate example (`qf_records_aggregate`):

```json
{
  "app_key": "your_app_key",
  "group_by": ["归属部门", "归属销售"],
  "amount_columns": ["报价总金额"],
  "metrics": ["count", "sum", "avg", "min", "max"],
  "time_bucket": "day",
  "requested_pages": 10,
  "scan_max_pages": 10,
  "strict_full": true
}
```

Batch detail example (`qf_records_batch_get`):

```json
{
  "app_key": "your_app_key",
  "apply_ids": ["497600278750478338", "497600278750478339"],
  "select_columns": [1, "客户名称"],
  "max_columns": 2
}
```

Export example (`qf_export_json`):

```json
{
  "app_key": "your_app_key",
  "mode": "all",
  "page_size": 50,
  "requested_pages": 5,
  "max_rows": 500,
  "select_columns": [1, "客户名称"],
  "file_name": "报价单导出.json"
}
```

Optional env vars:

```bash
export QINGFLOW_LIST_MAX_ITEMS_BYTES=400000
```

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
