# Security Policy

## Reporting a Vulnerability

Please report suspected vulnerabilities privately before opening a public issue.

- Preferred channel: email the maintainer directly.
- Include reproduction steps, impact, and affected versions.

## Secret Handling

1. Never commit `QINGFLOW_ACCESS_TOKEN` or any production credential.
2. Store secrets only in runtime environment variables.
3. Rotate tokens immediately if exposed in terminal logs, screenshots, chat, or commits.

## Scope

This project is an MCP wrapper around Qingflow OpenAPI and handles:

- request/response transformation
- tool argument validation
- error mapping

Security reports related to upstream Qingflow platform behavior should also be reported to Qingflow support.
