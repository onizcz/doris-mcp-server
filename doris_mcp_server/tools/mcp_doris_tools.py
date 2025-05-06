#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Doris MCP Tool Implementations

Includes exec_query and new tools based on schema_extractor.
"""

import os
import time
import json
import logging
from typing import Dict, Any
import pandas as pd

# --- Use absolute imports ---
from doris_mcp_server.utils.schema_extractor import MetadataExtractor
from doris_mcp_server.utils.sql_executor_tools import execute_sql_query

# Get logger
logger = logging.getLogger("doris-mcp-tools")

# --- Helper Function to format response ---
def _format_response(success: bool, result: Any = None, error: str = None, message: str = "") -> Dict[str, Any]:
    response_data = {
        "success": success,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    if success and result is not None:
        # Handle DataFrame serialization
        if isinstance(result, pd.DataFrame):
            try:
                # Convert DataFrame to JSON records format
                response_data["result"] = json.loads(result.to_json(orient='records', date_format='iso'))
            except Exception as df_err:
                logger.error(f"DataFrame to JSON conversion failed: {df_err}")
                # Fallback or specific error handling for DataFrame
                response_data["result"] = {"error": "Failed to serialize DataFrame result"}
                response_data["success"] = False # Mark as failed if serialization fails
                response_data["error"] = f"DataFrame serialization error: {str(df_err)}"
        else:
            response_data["result"] = result
        response_data["message"] = message or "Operation successful" # Translated: Operation successful
    elif not success:
        response_data["error"] = error or "Unknown error" # Translated: Unknown error
        response_data["message"] = message or "Operation failed" # Translated: Operation failed

    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(response_data, ensure_ascii=False, default=str) # Use default=str for non-serializable types
            }
        ]
    }

async def mcp_doris_exec_query(sql: str = None, db_name: str = None, max_rows: int = 100, timeout: int = 30) -> Dict[str, Any]:
    """
    Executes an SQL query and returns the result.

    Args:
        sql (str): The SQL query to execute.
        db_name (str, optional): Target database name. Defaults to the configured default database.
        max_rows (int, optional): Maximum number of rows to return. Defaults to 100.
        timeout (int, optional): Query timeout in seconds. Defaults to 30.

    Returns:
        Dict[str, Any]: A dictionary containing the query result or an error.
    """
    logger.info(f"MCP Tool Call: mcp_doris_exec_query, SQL: {sql}, DB: {db_name}, MaxRows: {max_rows}, Timeout: {timeout}")
    try:
        if not sql:
            return _format_response(success=False, error="SQL statement not provided", message="Please provide the SQL statement to execute")

        # Build parameters to pass to execute_sql_query
        exec_ctx = {
            "params": {
                "sql": sql,
                "db_name": db_name,
                "max_rows": max_rows,
                "timeout": timeout
            }
        }

        # Directly call execute_sql_query to execute the query
        exec_result = await execute_sql_query(exec_ctx)

        # The format returned by execute_sql_query is {'content': [{'type': 'text', 'text': json_string}]}
        # Need to parse the internal JSON string
        if exec_result and 'content' in exec_result and len(exec_result['content']) > 0 and 'text' in exec_result['content'][0]:
            try:
                # Parse JSON string
                result_data = json.loads(exec_result['content'][0]['text'])

                # Directly return the parsed result obtained from execute_sql_query
                # This result is already in the format {"success": ..., "data": ..., "columns": ...} or {"success": false, "error": ...}
                # _format_response would wrap it again, but here we directly use the parsed data
                # Note: This changes the original return structure of this function; it now directly returns the output of sql_executor
                # If the _format_response wrapper needs to be maintained, the code below needs adjustment
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result_data, ensure_ascii=False, default=str)
                        }
                    ]
                }
            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to parse execute_sql_query result: {json_err}")
                return _format_response(success=False, error=str(json_err), message="Error parsing SQL execution result")
            except Exception as parse_err:
                logger.error(f"Unexpected error occurred while processing execute_sql_query result: {parse_err}", exc_info=True)
                return _format_response(success=False, error=str(parse_err), message="Unknown error occurred while processing SQL execution result")
        else:
            logger.error(f"execute_sql_query returned an unexpected format: {exec_result}")
            return _format_response(success=False, error="SQL executor returned invalid format", message="Internal error executing SQL query")

    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_exec_query: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error executing SQL query")


async def mcp_doris_get_table_schema(table_name: str, db_name: str = None) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_table_schema, Table: {table_name}, DB: {db_name}")
    if not table_name:
         return _format_response(success=False, error="Missing table_name parameter")
    try:
        extractor = MetadataExtractor(db_name=db_name)
        schema = extractor.get_table_schema(table_name=table_name, db_name=db_name)
        if not schema:
             return _format_response(success=False, error="Table not found or has no columns", message=f"Could not get schema for table {db_name or extractor.db_name}.{table_name}")
        return _format_response(success=True, result=schema)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_table_schema: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting table schema")

async def mcp_doris_get_db_table_list(db_name: str = None) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_db_table_list, DB: {db_name}")
    try:
        extractor = MetadataExtractor(db_name=db_name)
        tables = extractor.get_database_tables(db_name=db_name)
        return _format_response(success=True, result=tables)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_db_table_list: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting database table list")

async def mcp_doris_get_db_list() -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_db_list")
    try:
        extractor = MetadataExtractor()
        databases = extractor.get_all_databases()
        return _format_response(success=True, result=databases)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_db_list: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting database list")

async def mcp_doris_get_table_comment(table_name: str, db_name: str = None) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_table_comment, Table: {table_name}, DB: {db_name}")
    if not table_name:
         return _format_response(success=False, error="Missing table_name parameter")
    try:
        extractor = MetadataExtractor(db_name=db_name)
        comment = extractor.get_table_comment(table_name=table_name, db_name=db_name)
        return _format_response(success=True, result=comment)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_table_comment: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting table comment")

async def mcp_doris_get_table_column_comments(table_name: str, db_name: str = None) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_table_column_comments, Table: {table_name}, DB: {db_name}")
    if not table_name:
         return _format_response(success=False, error="Missing table_name parameter")
    try:
        extractor = MetadataExtractor(db_name=db_name)
        comments = extractor.get_column_comments(table_name=table_name, db_name=db_name)
        return _format_response(success=True, result=comments)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_table_column_comments: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting column comments")

async def mcp_doris_get_table_indexes(table_name: str, db_name: str = None) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_table_indexes, Table: {table_name}, DB: {db_name}")
    if not table_name:
         return _format_response(success=False, error="Missing table_name parameter")
    try:
        extractor = MetadataExtractor(db_name=db_name)
        indexes = extractor.get_table_indexes(table_name=table_name, db_name=db_name)
        return _format_response(success=True, result=indexes)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_table_indexes: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting table indexes")

async def mcp_doris_get_recent_audit_logs(days: int = 7, limit: int = 100) -> Dict[str, Any]:
    logger.info(f"MCP Tool Call: mcp_doris_get_recent_audit_logs, Days: {days}, Limit: {limit}")
    try:
        extractor = MetadataExtractor()
        logs_df = extractor.get_recent_audit_logs(days=days, limit=limit)
        return _format_response(success=True, result=logs_df)
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_recent_audit_logs: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting audit logs")
