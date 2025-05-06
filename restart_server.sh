#!/bin/bash
# Doris MCP Server Restart Script
# Detects port and process usage, terminates existing processes, then restarts the server

# Set terminal colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Server configuration
MCP_PORT=3000
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SCRIPT="${SCRIPT_DIR}/start_server.sh"

echo -e "${GREEN}========== Doris MCP Server Restart Script ==========${NC}"

# Check if start_server.sh exists
if [ ! -f "$START_SCRIPT" ]; then
    echo -e "${RED}Error: Start script $START_SCRIPT does not exist${NC}"
    exit 1
fi

# Check port usage
check_port() {
    echo -e "${YELLOW}Checking port $MCP_PORT usage...${NC}"
    PORT_PID=$(lsof -ti:$MCP_PORT)
    if [ -n "$PORT_PID" ]; then
        echo -e "${YELLOW}Port $MCP_PORT is used by process $PORT_PID${NC}"
        return 0
    else
        echo -e "${GREEN}Port $MCP_PORT is not in use${NC}"
        return 1
    fi
}

# Check if Python process is running
check_python_process() {
    echo -e "${YELLOW}Checking if Python process is running doris_mcp_server.main...${NC}"
    PYTHON_PID=$(ps aux | grep "[p]ython.*-m doris_mcp_server.main --sse" | awk '{print $2}')
    if [ -n "$PYTHON_PID" ]; then
        echo -e "${YELLOW}Detected Python process $PYTHON_PID running doris_mcp_server.main --sse${NC}"
        return 0
    else
        echo -e "${GREEN}No Python process running doris_mcp_server.main detected${NC}"
        return 1
    fi
}

# Kill process
kill_process() {
    local PID=$1
    echo -e "${YELLOW}Terminating process $PID...${NC}"
    kill $PID 2>/dev/null
    
    # Wait for process termination
    for i in {1..5}; do
        if ! ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}Process $PID has terminated${NC}"
            return 0
        fi
        echo -e "${YELLOW}Waiting for process termination (${i}/5)...${NC}"
        sleep 1
    done
    
    # If process is still running, force kill
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${YELLOW}Process still running, force killing process $PID...${NC}"
        kill -9 $PID 2>/dev/null
        sleep 1
        if ! ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}Process $PID has been force killed${NC}"
            return 0
        else
            echo -e "${RED}Failed to terminate process $PID${NC}"
            return 1
        fi
    fi
    
    return 0
}

# Clean up all process and port usage
cleanup() {
    # Check and terminate process using the port
    check_port
    if [ $? -eq 0 ]; then
        kill_process $PORT_PID
    fi
    
    # Check and terminate Python process
    check_python_process
    if [ $? -eq 0 ]; then
        kill_process $PYTHON_PID
    fi
    
    # Check port usage again to ensure it's released
    check_port
    if [ $? -eq 0 ]; then
        echo -e "${RED}Warning: Failed to release port $MCP_PORT, please check the process manually${NC}"
        return 1
    fi
    
    # Clean up possible Python bytecode cache
    echo -e "${YELLOW}Cleaning Python bytecode cache...${NC}"
    find "$SCRIPT_DIR" -name "*.pyc" -delete
    find "$SCRIPT_DIR" -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    
    echo -e "${GREEN}Cleanup complete${NC}"
    return 0
}

# Start server
start_server() {
    echo -e "${YELLOW}Stopping existing Doris MCP server process (SSE mode)...${NC}"
    pkill -f "python -m doris_mcp_server.main --sse" || true

    # Wait for the process to stop completely
    sleep 2

    echo -e "${YELLOW}Starting Doris MCP server (SSE mode)...${NC}"
    nohup python -m doris_mcp_server.main --sse >> logs/doris_mcp.log 2>> logs/doris_mcp.error &
    
    # Wait for server startup
    sleep 5

    echo -e "${YELLOW}Checking if the server started successfully (SSE mode)...${NC}"
    if pgrep -f "python -m doris_mcp_server.main --sse" > /dev/null; then
        echo -e "${GREEN}Doris MCP server (SSE mode) started successfully${NC}"
        echo -e "${GREEN}Service address: http://localhost:$MCP_PORT/${NC}"
        return 0
    else
        echo -e "${RED}Server startup failed, please check the log files${NC}"
        tail -n 20 logs/doris_mcp.error
        return 1
    fi
}

# Main function
main() {
    echo -e "${YELLOW}Starting Doris MCP server restart...${NC}"
    
    # Clean up existing processes
    cleanup
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to clean up existing processes, restart aborted${NC}"
        exit 1
    fi
    
    # Wait for port to be fully released
    sleep 2
    
    # Start the server
    start_server
    if [ $? -ne 0 ]; then
        echo -e "${RED}Server startup failed${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Server restarted successfully${NC}"
    echo -e "${YELLOW}Service running at: http://localhost:$MCP_PORT${NC}"
    echo -e "${YELLOW}Health check: http://localhost:$MCP_PORT/health${NC}"
    echo -e "${YELLOW}SSE test endpoint: http://localhost:$MCP_PORT/sse"
}

# Run main function
main 