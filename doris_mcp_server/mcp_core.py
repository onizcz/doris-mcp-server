#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Core MCP instance and startup logic for stdio mode.
"""

import asyncio
import logging
import sys
import traceback
import json
from typing import Dict, Any

# Import necessary components from mcp and our project
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger("doris-mcp-core")

# --- Global MCP Instance for Stdio ---
# Create the instance when the module is imported.
# Tools will be registered synchronously(?) before running.
stdio_mcp = FastMCP(
    name="doris-mcp-stdio-core",
    description="Apache Doris MCP Server (stdio via core)",
)

# --- Removed async setup functions ---
def run_stdio():
    """
    Synchronous entry point for running the stdio server.
    Mimics the mcp-doris example by calling .run() on the instance.
    Handles tool registration beforehand.
    """
    logger.info("Executing run_stdio (synchronous entry point)...")

    # --- Run the stdio server using the instance's run() method ---
    logger.info("Calling stdio_mcp.run()...")
    try:
        # Assuming stdio_mcp has a synchronous run() method for stdio
        stdio_mcp.run()
        logger.info("stdio_mcp.run() completed.")
    except KeyboardInterrupt:
        logger.info("Stdio server stopped by KeyboardInterrupt.")
    except AttributeError:
        logger.critical("Error: stdio_mcp object does not have a '.run()' method suitable for stdio.", exc_info=False)
        print("ERROR: stdio_mcp object does not have a '.run()' method.", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"run_stdio encountered an error during stdio_mcp.run(): {e}", exc_info=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

# Register Tool: Execute SQL Query
@stdio_mcp.tool("exec_query", description="""[Function Description]: Execute SQL query and return result command (executed by the client).\n
[Parameter Content]:\n
- sql (string) [Required] - SQL statement to execute\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n
- max_rows (integer) [Optional] - Maximum number of rows to return, default 100\n
- timeout (integer) [Optional] - Query timeout in seconds, default 30\n""")
async def exec_query_tool(sql: str, db_name: str = None, max_rows: int = 100, timeout: int = 30) -> Dict[str, Any]:
    """Wrapper: Execute SQL query and return result command"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_exec_query
    return await mcp_doris_exec_query(sql=sql, db_name=db_name, max_rows=max_rows, timeout=timeout)

# Register Tool: Get Table Schema
@stdio_mcp.tool("get_table_schema", description="""[Function Description]: Get detailed structure information of the specified table (columns, types, comments, etc.).\n
[Parameter Content]:\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
async def get_table_schema_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
    """Wrapper: Get table schema"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_table_schema
    if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
    return await mcp_doris_get_table_schema(table_name=table_name, db_name=db_name)

# Register Tool: Get Database Table List
@stdio_mcp.tool("get_db_table_list", description="""[Function Description]: Get a list of all table names in the specified database.\n
[Parameter Content]:\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
async def get_db_table_list_tool(db_name: str = None) -> Dict[str, Any]:
    """Wrapper: Get database table list"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_db_table_list
    return await mcp_doris_get_db_table_list(db_name=db_name)

# Register Tool: Get Database List
@stdio_mcp.tool("get_db_list", description="""[Function Description]: Get a list of all database names on the server.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n""")
async def get_db_list_tool() -> Dict[str, Any]:
    """Wrapper: Get database list"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_db_list
    return await mcp_doris_get_db_list()

# Register Tool: Get Table Comment
@stdio_mcp.tool("get_table_comment", description="""[Function Description]: Get the comment information for the specified table.\n
[Parameter Content]:\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
async def get_table_comment_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
    """Wrapper: Get table comment"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_table_comment
    if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
    return await mcp_doris_get_table_comment(table_name=table_name, db_name=db_name)

# Register Tool: Get Table Column Comments
@stdio_mcp.tool("get_table_column_comments", description="""[Function Description]: Get comment information for all columns in the specified table.\n
[Parameter Content]:\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
async def get_table_column_comments_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
    """Wrapper: Get table column comments"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_table_column_comments
    if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
    return await mcp_doris_get_table_column_comments(table_name=table_name, db_name=db_name)

# Register Tool: Get Table Indexes
@stdio_mcp.tool("get_table_indexes", description="""[Function Description]: Get index information for the specified table.
[Parameter Content]:\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
async def get_table_indexes_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
    """Wrapper: Get table indexes"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_table_indexes
    if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
    return await mcp_doris_get_table_indexes(table_name=table_name, db_name=db_name)

# Register Tool: Get Recent Audit Logs
@stdio_mcp.tool("get_recent_audit_logs", description="""[Function Description]: Get audit log records for a recent period.\n
[Parameter Content]:\n
- days (integer) [Optional] - Number of recent days of logs to retrieve, default is 7\n
- limit (integer) [Optional] - Maximum number of records to return, default is 100\n""")
async def get_recent_audit_logs_tool(days: int = 7, limit: int = 100) -> Dict[str, Any]:
    """Wrapper: Get recent audit logs"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_recent_audit_logs
    try:
        days = int(days)
        limit = int(limit)
    except (ValueError, TypeError):
            return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "days and limit parameters must be integers"})}]}
    return await mcp_doris_get_recent_audit_logs(days=days, limit=limit)

# --- Register Tools ---
