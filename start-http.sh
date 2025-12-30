#!/bin/bash

echo "╔════════════════════════════════════════╗"
echo "║  TrendRadar MCP Server (HTTP Mode)     ║"
echo "╚════════════════════════════════════════╝"
echo ""

# Check for virtual environment
if [ ! -d ".venv" ]; then
    echo "❌ [Error] Virtual environment not found"
    echo "Please run ./setup-mac.sh first to set up the project"
    echo ""
    exit 1
fi

echo "[Mode] HTTP (for remote access)"
echo "[URL] http://localhost:3333/mcp"
echo "[Tip] Press Ctrl+C to stop the server"
echo ""

uv run python -m mcp_server.server --transport http --host 0.0.0.0 --port 3333
