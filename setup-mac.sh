#!/bin/bash

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BOLD}╔════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  TrendRadar MCP One-Click Setup (Mac) ║${NC}"
echo -e "${BOLD}╚════════════════════════════════════════╝${NC}"
echo ""

# Get project root directory
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

echo -e "📍 Project directory: ${BLUE}${PROJECT_ROOT}${NC}"
echo ""

# Check if UV is installed
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}[1/3] 🔧 UV not installed, installing automatically...${NC}"
    echo "Note: UV is a fast Python package manager, only needs to be installed once"
    echo ""
    curl -LsSf https://astral.sh/uv/install.sh | sh

    echo ""
    echo "Refreshing PATH environment variable..."
    echo ""

    # Add UV to PATH
    export PATH="$HOME/.cargo/bin:$PATH"

    # Verify UV is available
    if ! command -v uv &> /dev/null; then
        echo -e "${RED}❌ [Error] UV installation failed${NC}"
        echo ""
        echo "Possible causes:"
        echo "  1. Network connection issue, unable to download install script"
        echo "  2. Insufficient permissions for install path"
        echo "  3. Install script execution error"
        echo ""
        echo "Solutions:"
        echo "  1. Check your network connection"
        echo "  2. Manual install: https://docs.astral.sh/uv/getting-started/installation/"
        echo "  3. Or run: curl -LsSf https://astral.sh/uv/install.sh | sh"
        exit 1
    fi

    echo -e "${GREEN}✅ [Success] UV installed${NC}"
    echo -e "${YELLOW}⚠️  Please re-run this script to continue${NC}"
    exit 0
else
    echo -e "${GREEN}[1/3] ✅ UV is installed${NC}"
    uv --version
fi

echo ""
echo "[2/3] 📦 Installing project dependencies..."
echo "Note: This may take 1-2 minutes, please wait"
echo ""

# Create virtual environment and install dependencies
uv sync

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}❌ [Error] Dependency installation failed${NC}"
    echo "Please check your network connection and try again"
    exit 1
fi

echo ""
echo -e "${GREEN}[3/3] ✅ Checking configuration files...${NC}"
echo ""

# Check config file
if [ ! -f "config/config.yaml" ]; then
    echo -e "${YELLOW}⚠️  [Warning] Config file not found: config/config.yaml${NC}"
    echo "Please ensure the config file exists"
    echo ""
fi

# Add execute permission
chmod +x start-http.sh 2>/dev/null || true

# Get UV path
UV_PATH=$(which uv)

echo ""
echo -e "${BOLD}╔════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║           Setup Complete!              ║${NC}"
echo -e "${BOLD}╚════════════════════════════════════════╝${NC}"
echo ""
echo "📋 Next steps:"
echo ""
echo "  1️⃣  Open Cherry Studio (or your MCP client)"
echo "  2️⃣  Go to Settings > MCP Servers > Add Server"
echo "  3️⃣  Enter the following configuration:"
echo ""
echo "      Name: TrendRadar"
echo "      Description: Trending News Aggregation Tool"
echo "      Type: STDIO"
echo -e "      Command: ${BLUE}${UV_PATH}${NC}"
echo "      Arguments (one per line):"
echo -e "        ${BLUE}--directory${NC}"
echo -e "        ${BLUE}${PROJECT_ROOT}${NC}"
echo -e "        ${BLUE}run${NC}"
echo -e "        ${BLUE}python${NC}"
echo -e "        ${BLUE}-m${NC}"
echo -e "        ${BLUE}mcp_server.server${NC}"
echo ""
echo "  4️⃣  Save and enable the MCP toggle"
echo ""
echo "📖 For detailed instructions, see: README-EN.md"
echo "   Keep this window open for reference when entering parameters"
echo ""
