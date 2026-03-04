# Qingflow MCP (CRUD)

This MCP server wraps Qingflow OpenAPI for:

- `qf_apps_list`
- `qf_form_get`
- `qf_records_list`
- `qf_record_get`
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

## List Query Tips

1. For `qf_records_list.sort[].que_id`, use a real field `que_id` (numeric) or exact field title from `qf_form_get`.
2. Avoid aliases like `create_time`; Qingflow often rejects them.
3. Use `max_rows` (or `max_items`) to cap returned rows.
4. Use `max_columns` to cap answers per row when `include_answers=true`.
5. Use `select_columns` to return only specific columns (supports `que_id` or exact field title).
6. When `include_answers=true`, the server still auto-limits by response size to protect MCP context.

Example:

```json
{
  "app_key": "your_app_key",
  "mode": "all",
  "page_size": 50,
  "include_answers": true,
  "max_rows": 10,
  "max_columns": 5,
  "select_columns": [1, "客户名称", "1003"]
}
```

Optional env vars:

```bash
export QINGFLOW_LIST_MAX_ITEMS_WITH_ANSWERS=5
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
