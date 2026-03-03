# Qingflow MCP (CRUL)

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

Global install:

```bash
npm i -g qingflow-mcp
```

Or use without install:

```bash
npx -y qingflow-mcp
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
