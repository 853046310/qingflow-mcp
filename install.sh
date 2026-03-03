#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${QINGFLOW_MCP_REPO_URL:-https://github.com/853046310/qingflow-mcp.git}"
GIT_REF="${QINGFLOW_MCP_GIT_REF:-main}"

if ! command -v node >/dev/null 2>&1; then
  echo "Error: node is not installed."
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "Error: npm is not installed."
  exit 1
fi

PACKAGE_SPEC="git+${REPO_URL}#${GIT_REF}"

echo "Installing qingflow-mcp from ${PACKAGE_SPEC}"
npm install -g "${PACKAGE_SPEC}"

if ! command -v qingflow-mcp >/dev/null 2>&1; then
  echo "Error: installation finished but 'qingflow-mcp' is not on PATH."
  echo "Try reopening your terminal or checking your npm global bin path."
  exit 1
fi

cat <<'EOF'
Installed qingflow-mcp successfully.

Run with:
QINGFLOW_BASE_URL="https://api.qingflow.com" \
QINGFLOW_ACCESS_TOKEN="your_access_token" \
qingflow-mcp
EOF
