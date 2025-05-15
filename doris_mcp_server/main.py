#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Apache Doris MCP Server Main Entry - Primarily handles SSE mode

Stdio mode is handled by doris_mcp_server.mcp_core:run_stdio.
"""

import os
import sys
import argparse
import asyncio
import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Dict, Any
import uvicorn
from uvicorn import Config, Server
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# SSE related imports
from mcp.server.fastmcp import FastMCP
from doris_mcp_server.sse_server import DorisMCPSseServer
from doris_mcp_server.streamable_server import DorisMCPStreamableServer

# Stdio related imports (only needed for tools now, maybe move tool init?)
# from mcp.server.stdio import stdio_server -> No longer used here

# Config and Tool Initializer
from doris_mcp_server.config import load_config # LOG_LEVEL might not be needed here directly
from doris_mcp_server.tools.tool_initializer import register_mcp_tools

# Load environment variables (load early for all modes)
load_dotenv(override=True)

# Get logger
logger = logging.getLogger("doris-mcp-main") # Changed logger name slightly

# --- Configuration Loading and Logging Setup ---
load_config() # Loads .env

# --- Create FastAPI App (Global Scope for SSE Mode) ---
# This 'app' object is targeted by 'mcp run doris_mcp_server/main.py:app --transport sse'
# And used when running directly with --sse
app = FastAPI(
    title="Doris MCP Server (SSE Mode)",
    # Lifespan will be added in start_sse_server
)

# --- Removed StdioServerWrapper --- 

# --- Command Line Argument Parsing ---
def parse_args():
    parser = argparse.ArgumentParser(description="Apache Doris MCP Server (SSE Mode Entry)")
    # Only keep SSE related args here
    parser.add_argument('--sse', action='store_true', help='Start SSE Web server mode (required)')
    parser.add_argument('--host', type=str, default=os.getenv('SERVER_HOST', '0.0.0.0'), help='Host address')
    parser.add_argument('--port', type=int, default=int(os.getenv('SERVER_PORT', os.getenv('MCP_PORT', '3000'))), help='Port number')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    return parser.parse_args()

# --- SSE Mode Specific Code ---
@dataclass
class AppContext:
    config: Dict[str, Any]

@asynccontextmanager
async def app_lifespan(app_instance: FastAPI) -> AsyncIterator[None]:
    logger.info("SSE application lifecycle start...")
    config = {
        # Simplified config - maybe get from elsewhere?
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "9030")),
        "db_user": os.getenv("DB_USER", "root"),
        "db_password": os.getenv("DB_PASSWORD", ""),
        "db_database": os.getenv("DB_DATABASE", "test"),
    }
    app_instance.state.config = config
    try:
        # Yield None implicitly or explicitly None
        yield 
    finally:
        logger.info("Cleaning up SSE application resources...")

async def start_sse_server(args):
    """Start SSE Web server mode (Configures the global 'app')"""
    logger.info("Starting SSE Web server mode...")
    global app

    # --- Initialize MCP and Tools for SSE ---
    # Create a *separate* MCP instance for SSE mode
    sse_mcp = FastMCP(
        name="doris-mcp-sse",
        description="Apache Doris MCP Server (SSE)",
        lifespan=None, # Managed by FastAPI
        dependencies=["fastapi", "uvicorn", "openai", "sse_starlette"]
    )
    logger.info("Registering MCP tools for SSE mode...")
    await register_mcp_tools(sse_mcp) # Register tools for the SSE instance
    logger.info("MCP tools registered for SSE.")

    # --- Configure Lifespan and CORS for the global app ---
    app.router.lifespan_context = app_lifespan
    origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    allow_credentials = os.getenv("MCP_ALLOW_CREDENTIALS", "false").lower() == "true"
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )

    # --- Initialize Handlers and Register Routes (Pass sse_mcp instance) ---
    logger.info("Initializing SSE server handlers and registering routes...")
    sse_server_handler = DorisMCPSseServer(sse_mcp, app)
    streamable_server_handler = DorisMCPStreamableServer(sse_mcp, app)
    logger.info("SSE Server handlers initialized and routes registered.")

    # --- Print Configuration and Endpoints ---
    print("--- SSE Mode Configuration ---")
    print(f"Server Host: {args.host}")
    print(f"Server Port: {args.port}")
    print(f"Allowed Origins: {origins}")
    print(f"Allow Credentials: {allow_credentials}")
    print(f"Log Level: {os.getenv('LOG_LEVEL', 'info')}")
    print(f"Debug Mode: {args.debug}")
    print(f"Reload Mode: {args.reload}")
    print(f"DB Host: {os.getenv('DB_HOST')}")
    print(f"DB Port: {os.getenv('DB_PORT')}")
    print(f"DB User: {os.getenv('DB_USER')}")
    print(f"DB Database: {os.getenv('DB_DATABASE')}")
    print(f"DB Mysql Host: {os.getenv('DB_MYSQL_HOST')}")
    print(f"DB Mysql Port: {os.getenv('DB_MYSQL_PORT')}")
    print(f"DB Mysql User: {os.getenv('DB_MYSQL_USER')}")
    print(f"DB Mysql Database: {os.getenv('DB_MYSQL_DATABASE')}")
    print(f"Force Refresh Metadata: {os.getenv('FORCE_REFRESH_METADATA', 'false')}")
    print("------------------------------")
    base_url = f"http://{args.host}:{args.port}"
    print(f"Service running at: {base_url}")
    print(f"  Health Check: GET {base_url}/health")
    print(f"  Status Check: GET {base_url}/status")
    print(f"  SSE Init: GET {base_url}/sse")
    print(f"  SSE/Legacy Messages: POST {base_url}/mcp/messages")
    print(f"  Streamable HTTP: GET/POST/DELETE/OPTIONS {base_url}/mcp")
    print("------------------------------")
    print("Use Ctrl+C to stop the service")

    # --- Start Uvicorn Server ---
    config = Config(
        app=app,
        host=args.host,
        port=args.port,
        log_level="debug" if args.debug else "info",
        reload=args.reload
    )
    server = Server(config=config)
    await server.serve()

# --- Main Execution Logic (Simplified) ---

def run_main_sync():
    """Synchronous wrapper, primarily for SSE mode now."""
    sync_logger = logging.getLogger("run_main_sync")
    sync_logger.info("Entering run_main_sync (SSE focus)...")
    print("DEBUG: Entering run_main_sync (SSE focus)...", file=sys.stderr, flush=True)
    args = parse_args()

    if args.sse:
        try:
            # Run the async SSE server setup and Uvicorn loop
            asyncio.run(start_sse_server(args))
            sync_logger.info("asyncio.run(start_sse_server) completed.")
            print("DEBUG: asyncio.run(start_sse_server) completed.", file=sys.stderr, flush=True)
        except KeyboardInterrupt:
            sync_logger.info("SSE server stopped by KeyboardInterrupt.")
        except Exception as e:
            sync_logger.critical(f"Error during asyncio.run(start_sse_server): {e}", exc_info=True)
            print(f"DEBUG: Error during asyncio.run(start_sse_server): {e}", file=sys.stderr, flush=True)
            raise
    else:
        # If run without --sse, print help/error
        message = "Error: This entry point requires --sse flag. For stdio mode, use 'uv run mcp-doris' or the appropriate command for your stdio setup."
        sync_logger.error(message)
        print(message, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run_main_sync()