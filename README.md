# Qingflow MCP (CRUD)

This MCP server wraps Qingflow OpenAPI for:

- `qf_apps_list`
- `qf_form_get`
- `qf_records_list`
- `qf_record_get`
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

## CLI Install

Global install from GitHub:

```bash
npm i -g git+https://github.com/853046310/qingflow-mcp.git
```

Install latest from npm:

```bash
npm i -g qingflow-mcp@latest
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
3. `meta`: field mapping, filter scope, stat policy, execution limits.

Return shape:

1. success: structured payload `{ "ok": true, "data": ..., "meta": ... }`
2. failure: MCP `isError=true`, and text content is JSON payload like `{ "ok": false, "message": ..., ... }`
3. incomplete strict queries fail with `{ "code": "NEED_MORE_DATA", "status": "need_more_data", ... }`

Deterministic read protocol (list/summary/aggregate):

1. `completeness` is always returned:
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
2. `evidence` is always returned:
   - `query_id`
   - `app_key`
   - `filters`
   - `selected_columns`
   - `time_range`
   - `source_pages`
3. `strict_full=true` makes incomplete results fail fast with `NEED_MORE_DATA`.

## List Query Tips

Strict mode (`qf_records_list`):

1. `select_columns` is required.
2. `include_answers=false` is not allowed.
3. Output `items[].answers` contains only selected columns, not full answers.

1. For `qf_records_list.sort[].que_id`, use a real field `que_id` (numeric) or exact field title from `qf_form_get`.
2. Avoid aliases like `create_time`; Qingflow often rejects them.
3. Use `max_rows` (or `max_items`) to cap returned rows. Default row cap is 200.
4. Use `max_columns` to cap answers per row when `include_answers=true`.
5. Use `select_columns` to return only specific columns (supports `que_id` or exact field title).
6. The server may still trim by response-size guardrail (`QINGFLOW_LIST_MAX_ITEMS_BYTES`) when payload is too large.
7. Use `requested_pages` and `scan_max_pages` for deterministic page scan.
8. Continue with `page_token` from previous `next_page_token`.
9. Column limits: `select_columns <= 10`, `max_columns <= 10`.

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
  "max_columns": 5,
  "select_columns": [1, "客户名称", "1003"],
  "strict_full": false
}
```

For single record details (`qf_record_get`), the same column controls are supported:

```json
{
  "apply_id": "497600278750478338",
  "max_columns": 5,
  "select_columns": [1, "客户名称"]
}
```

`qf_record_get` requires `select_columns`.

Aggregate example (`qf_records_aggregate`):

```json
{
  "app_key": "your_app_key",
  "group_by": ["归属部门", "归属销售"],
  "amount_column": "报价总金额",
  "requested_pages": 50,
  "scan_max_pages": 50,
  "strict_full": true
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
