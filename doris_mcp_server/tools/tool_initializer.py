#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tool Initialization Module

Centralized initialization of all tools, ensuring they are correctly registered with MCP
"""

import logging
import os
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import traceback

# Import Context
from mcp.server.fastmcp import Context

# Import doris mcp tools
from doris_mcp_server.tools.mcp_doris_tools import (
    mcp_doris_exec_query,
    mcp_doris_get_table_schema,
    mcp_doris_get_db_table_list,
    mcp_doris_get_db_list,
    mcp_doris_get_table_comment,
    mcp_doris_get_table_column_comments,
    mcp_doris_get_table_indexes,
    mcp_doris_get_recent_audit_logs
)

# Get logger
logger = logging.getLogger("doris-mcp-tools-initializer")

async def register_mcp_tools(mcp):
    """Register MCP tool functions
    
    Args:
        mcp: FastMCP instance
    """
    logger.info("Starting to register MCP tools...")
    
    try:
        # Register Tool: Execute SQL Query (Using long description string including parameters)
        @mcp.tool("exec_query", description="""[Function Description]: Execute SQL query and return result command (executed by the client).\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- sql (string) [Required] - SQL statement to execute\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n
- max_rows (integer) [Optional] - Maximum number of rows to return, default 100
- timeout (integer) [Optional] - Query timeout in seconds, default 30""")
        async def exec_query_tool(sql: str, db_name: str = None, max_rows: int = 100, timeout: int = 30) -> Dict[str, Any]:
            """Wrapper: Execute SQL query and return result command"""
            # Note: ctx parameter is no longer needed here as we receive named parameters directly
            return await mcp_doris_exec_query(sql=sql, db_name=db_name, max_rows=max_rows, timeout=timeout)
        
        # Register Tool: Get Table Schema (Keep long description string including parameters)
        @mcp.tool("get_table_schema", description="""[Function Description]: Get detailed structure information of the specified table (columns, types, comments, etc.).\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
        async def get_table_schema_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
            """Wrapper: Get table schema"""
            if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
            return await mcp_doris_get_table_schema(table_name=table_name, db_name=db_name)
        
        # Register Tool: Get Database Table List (Keep long description string including parameters)
        @mcp.tool("get_db_table_list", description="""[Function Description]: Get a list of all table names in the specified database.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
        async def get_db_table_list_tool(db_name: str = None) -> Dict[str, Any]:
            """Wrapper: Get database table list"""
            return await mcp_doris_get_db_table_list(db_name=db_name)
        
        # Register Tool: Get Database List (Keep long description string including parameters)
        # Note: Although the description mentions random_string, the wrapper function signature does not. See how mcp handles this.
        @mcp.tool("get_db_list", description="""[Function Description]: Get a list of all database names on the server.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n""")
        async def get_db_list_tool() -> Dict[str, Any]: # Function signature has no parameters
            """Wrapper: Get database list"""
            return await mcp_doris_get_db_list()
        
        # Register Tool: Get Table Comment (Keep long description string including parameters)
        @mcp.tool("get_table_comment", description="""[Function Description]: Get the comment information for the specified table.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
        async def get_table_comment_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
            """Wrapper: Get table comment"""
            if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
            return await mcp_doris_get_table_comment(table_name=table_name, db_name=db_name)
        
        # Register Tool: Get Table Column Comments (Keep long description string including parameters)
        @mcp.tool("get_table_column_comments", description="""[Function Description]: Get comment information for all columns in the specified table.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
        async def get_table_column_comments_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
            """Wrapper: Get table column comments"""
            if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
            return await mcp_doris_get_table_column_comments(table_name=table_name, db_name=db_name)
        
        # Register Tool: Get Table Indexes (Keep long description string including parameters)
        @mcp.tool("get_table_indexes", description="""[Function Description]: Get index information for the specified table.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- table_name (string) [Required] - Name of the table to query\n
- db_name (string) [Optional] - Target database name, defaults to the current database\n""")
        async def get_table_indexes_tool(table_name: str, db_name: str = None) -> Dict[str, Any]:
            """Wrapper: Get table indexes"""
            if not table_name: return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "Missing table_name parameter"})}]}
            return await mcp_doris_get_table_indexes(table_name=table_name, db_name=db_name)
        
        # Register Tool: Get Recent Audit Logs (Keep long description string including parameters)
        @mcp.tool("get_recent_audit_logs", description="""[Function Description]: Get audit log records for a recent period.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n
- days (integer) [Optional] - Number of recent days of logs to retrieve, default is 7\n
- limit (integer) [Optional] - Maximum number of records to return, default is 100\n""")
        async def get_recent_audit_logs_tool(days: int = 7, limit: int = 100) -> Dict[str, Any]:
            """Wrapper: Get recent audit logs"""
            try:
                days = int(days)
                limit = int(limit)
            except (ValueError, TypeError):
                 return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": "days and limit parameters must be integers"})}]}
            return await mcp_doris_get_recent_audit_logs(days=days, limit=limit)
        
        # Get tool count
        tools_count = len(await mcp.list_tools())
        logger.info(f"Registered all MCP tools, total {tools_count} tools")
        return True
    except Exception as e:
        logger.error(f"Error registering MCP tools: {str(e)}")
        logger.error(traceback.format_exc())
        return False