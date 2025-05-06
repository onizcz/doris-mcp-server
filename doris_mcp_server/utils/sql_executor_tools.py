#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL Execution Tool

Responsible for executing SQL queries and handling results
"""

import os
import json
import logging
import traceback
import time
from typing import Dict, Any
import re
import datetime
from decimal import Decimal

# Get logger
logger = logging.getLogger("doris-mcp.sql-executor")

# Add environment variable control for whether to perform SQL security checks
ENABLE_SQL_SECURITY_CHECK = os.environ.get('ENABLE_SQL_SECURITY_CHECK', 'true').lower() == 'true'

async def execute_sql_query(ctx) -> Dict[str, Any]:
    """
    Execute SQL query and return results
    
    Args:
        ctx: Context object or dictionary containing request parameters
        
    Returns:
        Dict[str, Any]: Execution result
    """
    try:
        # Support the case where the passed argument is a dictionary
        if isinstance(ctx, dict) and 'params' in ctx:
            params = ctx['params']
        else:
            params = ctx.params

        sql = params.get("sql")
        db_name = params.get("db_name", os.getenv("DB_DATABASE", ""))
        max_rows = params.get("max_rows", 1000)  # Maximum number of rows to return
        timeout = params.get("timeout", 30)  # Timeout in seconds
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "Missing SQL parameter",
                            "message": "Please provide the SQL query to execute"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # First check SQL security
        security_result = await _check_sql_security(sql)
        if not security_result.get("is_safe", False):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "SQL security check failed",
                            "message": "Query contains unsafe operations and cannot be executed",
                            "security_issues": security_result.get("security_issues", [])
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # Import database connection tool
        from doris_mcp_server.utils.db import execute_query
        
        if not sql:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": "Missing SQL parameter",
                            "message": "Please provide the SQL query to execute"
                        }, ensure_ascii=False)
                    }
                ]
            }
        
        # Ensure SELECT statements include a LIMIT clause
        sql_lower = sql.lower().strip()
        if sql_lower.startswith("select") and "limit" not in sql_lower:
            sql = sql.rstrip(";") + f" LIMIT {max_rows};"
        
        # Start timer
        start_time = time.time()
        
        # Execute query
        try:
            result = execute_query(sql, db_name)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Build return result
            if isinstance(result, list):
                # Handle list of query results
                row_count = len(result)
                
                # Extract column names
                if hasattr(result[0], "_fields"):
                    # If it's a named tuple
                    columns = list(result[0]._fields)
                else:
                    # Otherwise, assume it's a dictionary
                    columns = list(result[0].keys()) if isinstance(result[0], dict) else []
                
                # Convert results to serializable format
                data = []
                for row in result:
                    row_dict = {}
                    if hasattr(row, "_asdict"):
                        # If it's a named tuple
                        row_dict = row._asdict()
                    elif isinstance(row, dict):
                        # If it's a dictionary
                        row_dict = row
                    else:
                        # If it's a list or tuple
                        row_dict = dict(zip(columns, row)) if columns else row
                    
                    # Handle special types to make them JSON serializable
                    serialized_row = _serialize_row_data(row_dict)
                    data.append(serialized_row)
                
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps({
                                "success": True,
                                "sql": sql,
                                "row_count": row_count,
                                "columns": columns,
                                "data": data[:max_rows],  # Limit returned rows
                                "execution_time": execution_time,
                                "truncated": row_count > max_rows
                            }, ensure_ascii=False)
                        }
                    ]
                }
            else:
                # Handle other types of results
                other_response = {
                    "success": True,
                    "sql": sql,
                    "result": str(result),
                    "execution_time": execution_time
                }
                other_response = _serialize_row_data(other_response)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(other_response, ensure_ascii=False)
                        }
                    ]
                }
                
        except Exception as db_error:
            error_message = str(db_error)
            
            # Try to get more detailed error information
            error_details = {}
            if "timeout" in error_message.lower():
                error_details["type"] = "timeout"
                error_details["suggestion"] = "Query timed out, please optimize SQL or increase timeout"
            elif "syntax" in error_message.lower():
                error_details["type"] = "syntax"
                error_details["suggestion"] = "SQL syntax error, please check syntax"
            elif "not found" in error_message.lower() or "doesn't exist" in error_message.lower():
                error_details["type"] = "not_found"
                error_details["suggestion"] = "Table or column not found, please check table and column names"
            else:
                error_details["type"] = "unknown"
                error_details["suggestion"] = "Please check the SQL statement and try simplifying the query"
            
            # Create error response
            error_response = {
                "success": False,
                "error": error_message,
                "error_details": error_details,
                "sql": sql,
                "db_name": db_name
            }
            
            # Ensure error response is also serializable
            error_response = _serialize_row_data(error_response)
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(error_response, ensure_ascii=False)
                    }
                ]
            }
        
    except Exception as e:
        logger.error(f"Failed to execute SQL query: {str(e)}")
        logger.error(traceback.format_exc())
        
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error occurred while executing SQL query"
        }
        
        # Ensure error response is also serializable
        error_response = _serialize_row_data(error_response)
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(error_response, ensure_ascii=False)
                }
            ]
        }

# Helper function
async def _check_sql_security(sql: str) -> Dict[str, Any]:
    """Check SQL security"""
    # If environment variable is set to disable security check, return safe immediately
    if not ENABLE_SQL_SECURITY_CHECK:
        return {
            "is_safe": True,
            "security_issues": []
        }
        
    # Check if SQL contains dangerous operations
    sql_lower = sql.lower()
    
    # Check if it's a read-only query type
    is_read_only = sql_lower.strip().startswith(("select ", "show ", "desc ", "describe ", "explain "))
    
    # Define list of dangerous operations (checked for both read-only and non-read-only queries)
    dangerous_operations = [
        (r'\bdelete\b', "DELETE operation"),
        (r'\bdrop\b', "DROP TABLE/DATABASE operation"),
        (r'\btruncate\b', "TRUNCATE TABLE operation"),
        (r'\bupdate\b', "UPDATE operation"),
        (r'\binsert\b', "INSERT operation"),
        (r'\balter\b', "ALTER TABLE structure operation"),
        (r'\bcreate\b', "CREATE TABLE/DATABASE operation"),
        (r'\bgrant\b', "GRANT operation"),
        (r'\brevoke\b', "REVOKE permission operation"),
        (r'\bexec\b', "EXECUTE stored procedure"),
        (r'\bxp_', "Extended stored procedure, potential security risk"),
        (r'\bshutdown\b', "SHUTDOWN database operation"),
        (r'\bunion\s+all\s+select\b', "UNION statement, potential SQL injection"),
        (r'\bunion\s+select\b', "UNION statement, potential SQL injection"),
        (r'\binto\s+outfile\b', "Write to file operation"),
        (r'\bload_file\b', "Load file operation")
    ]
    
    # Dangerous operations checked only for non-read-only queries
    non_readonly_operations = []
    if not is_read_only:
        non_readonly_operations = [
            (r'--', "SQL comment, potential SQL injection"),
            (r'/\*', "SQL block comment, potential SQL injection")
        ]
    
    # Check if dangerous operations are included
    security_issues = []
    
    # Check dangerous operations applicable to all queries
    for operation, description in dangerous_operations:
        if re.search(operation, sql_lower):
            # For specific keywords in read-only queries, differentiate if used as independent operations
            if is_read_only and operation in [r'\bcreate\b', r'\bdrop\b', r'\bdelete\b', r'\binsert\b', r'\bupdate\b', r'\balter\b']:
                # Check if used as DDL/DML keyword, e.g., CREATE TABLE, DROP DATABASE
                pattern = operation + r'\s+(?:table|database|view|index|procedure|function|trigger|event)'
                if re.search(pattern, sql_lower):
                    security_issues.append({
                        "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                        "description": description,
                        "severity": "High"
                    })
            else:
                security_issues.append({
                    "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                    "description": description,
                    "severity": "High"
                })
    
    # Check dangerous operations specific to non-read-only queries
    for operation, description in non_readonly_operations:
        if re.search(operation, sql_lower):
            security_issues.append({
                "operation": operation.replace(r'\b', '').replace(r'\s+', ' '),
                "description": description,
                "severity": "Medium"
            })
    
    return {
        "is_safe": len(security_issues) == 0,
        "security_issues": security_issues
    }

def _serialize_row_data(row_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert special types in row data (like date, time, Decimal) to JSON serializable format
    
    Args:
        row_data: Row data dictionary
        
    Returns:
        Dict[str, Any]: Processed serializable dictionary
    """
    serialized_data = {}
    for key, value in row_data.items():
        if value is None:
            serialized_data[key] = None
        elif isinstance(value, (datetime.date, datetime.datetime)):
            # Convert date and time types to ISO format string
            serialized_data[key] = value.isoformat()
        elif isinstance(value, Decimal):
            # Convert Decimal type to float
            serialized_data[key] = float(value)
        elif isinstance(value, (list, tuple)):
            # Recursively process elements in list or tuple
            serialized_data[key] = [
                _serialize_row_data(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif isinstance(value, dict):
            # Recursively process nested dictionaries
            serialized_data[key] = _serialize_row_data(value)
        else:
            serialized_data[key] = value
    return serialized_data 