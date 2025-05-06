#!/bin/bash
# Doris MCP Server Start Script
# Ensures the service runs in SSE mode

# Set colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========== Doris MCP Server Start Script ==========${NC}"

# Check virtual environment
if [ -d "venv" ]; then
    echo -e "${CYAN}Virtual environment found, activating...${NC}" # Found virtual environment, activating...
    source venv/bin/activate
fi

# Clean cache files
echo -e "${CYAN}Cleaning cache files...${NC}" # Cleaning cache files...
echo -e "${CYAN}Cleaning Python cache files...${NC}" # Cleaning Python cache files...
find . -type d -name "__pycache__" -exec rm -rf {} +  2>/dev/null || true
echo -e "${CYAN}Cleaning temporary files...${NC}" # Cleaning temporary files...
rm -rf .pytest_cache 2>/dev/null || true
echo -e "${CYAN}Cleaning log files...${NC}" # Cleaning log files...
find ./log -type f -name "*.log" -delete 2>/dev/null || true

# Reload environment variables
if [ -f .env ]; then
    echo -e "${CYAN}Loading environment variables from .env file...${NC}" # Loading environment variables from .env file...
    source .env
fi

# Output key environment variables before starting
echo -e "${CYAN}Database settings:${NC}" # Database settings:
echo "DB_HOST=${DB_HOST}"
echo "DB_PORT=${DB_PORT}"
echo "DB_DATABASE=${DB_DATABASE}"
echo "FORCE_REFRESH_METADATA=${FORCE_REFRESH_METADATA}"

# Start the server (using -m and new package path)
python -m doris_mcp_server.main --sse

# Clean cache files (This section seems redundant and possibly misplaced after the server starts)
echo -e "${YELLOW}Cleaning cache files...${NC}" # Cleaning cache files...

# Backend cache cleanup
echo -e "${GREEN}Cleaning Python cache files...${NC}" # Cleaning Python cache files...
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
rm -rf ./.pytest_cache

# Clean temporary files
echo -e "${GREEN}Cleaning temporary files...${NC}" # Cleaning temporary files...
rm -rf ./tmp
mkdir -p tmp

# Clean log files
echo -e "${GREEN}Cleaning log files...${NC}" # Cleaning log files...
rm -rf ./logs/*.log
mkdir -p logs

# Set environment variables, force SSE mode (This section also seems redundant if variables are set in .env and the command uses --sse)
export MCP_PORT=3000
export ALLOWED_ORIGINS="*"
export LOG_LEVEL="info"
export MCP_ALLOW_CREDENTIALS="false"

# Add adapter debug support
export MCP_DEBUG_ADAPTER="true"
export PYTHONPATH="$(pwd):$PYTHONPATH"  # Ensure modules can be imported

# Create log directory
mkdir -p logs

# Debug info
echo -e "${GREEN}Environment Variables:${NC}" # Environment Variables:
echo -e "MCP_TRANSPORT_TYPE=${MCP_TRANSPORT_TYPE}"
echo -e "MCP_PORT=${MCP_PORT}"
echo -e "ALLOWED_ORIGINS=${ALLOWED_ORIGINS}"
echo -e "LOG_LEVEL=${LOG_LEVEL}"
echo -e "MCP_ALLOW_CREDENTIALS=${MCP_ALLOW_CREDENTIALS}"
echo -e "MCP_DEBUG_ADAPTER=${MCP_DEBUG_ADAPTER}"

echo -e "${GREEN}Starting MCP server (SSE mode)...${NC}" # Starting MCP server (SSE mode)...
echo -e "${YELLOW}Service will run on http://localhost:3000/mcp${NC}" # Service will run on http://localhost:3000/mcp
echo -e "${YELLOW}Health Check: http://localhost:3000/health${NC}" # Health Check: http://localhost:3000/health
echo -e "${YELLOW}SSE Test: http://localhost:3000/sse${NC}" # SSE Test: http://localhost:3000/sse
echo -e "${YELLOW}Use Ctrl+C to stop the service${NC}" # Use Ctrl+C to stop the service

# If the server exits abnormally, output error message
if [ $? -ne 0 ]; then
    echo -e "${RED}Server exited abnormally! Check logs for more information${NC}" # Server exited abnormally! Check logs for more information
    exit 1
fi

# Show browser cache clearing prompt
echo -e "${YELLOW}Tip: If the page displays abnormally, please clear your browser cache or use incognito mode${NC}" # Tip: If the page displays abnormally, please clear your browser cache or use incognito mode
echo -e "${YELLOW}Chrome browser clear cache shortcut: Ctrl+Shift+Del (Windows) or Cmd+Shift+Del (Mac)${NC}" # Chrome browser clear cache shortcut: Ctrl+Shift+Del (Windows) or Cmd+Shift+Del (Mac) 