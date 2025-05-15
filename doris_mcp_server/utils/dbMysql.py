import os
import json
import pymysql
import pandas as pd
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv(override=True)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_MYSQL_HOST", "192.168.91.245"),
    "port": int(os.getenv("DB_MYSQL_PORT", "3306")),
    "user": os.getenv("DB_MYSQL_USER", "root"),
    "password": os.getenv("DB_MYSQL_PASSWORD", "tRstOpwalkroot0823"),
    "database": os.getenv("DB_MYSQL_DATABASE", "announce"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db_mysql_connection(db_name: Optional[str] = None):
    """
    Get database connection
    
    Args:
        db_name: Specify the database name to connect to, use default config if None
    
    Returns:
        Database connection
    """
    if db_name:
        # Use default config but override database name
        config = DB_CONFIG.copy()
        config["database"] = db_name
        return pymysql.connect(**config)
    else:
        # Use default config
        return pymysql.connect(**DB_CONFIG)

def get_db_mysql_name() -> str:
    """Get the currently configured default database name"""
    return DB_CONFIG["database"] or os.getenv("DB_DATABASE", "")

def execute_query_mysql(sql, db_name: Optional[str] = None):
    """
    Execute SQL query and return results
    
    Args:
        sql: SQL query statement
        db_name: Specify the database name to connect to, use default config if None
    
    Returns:
        Query results
    """
    conn = get_db_mysql_connection(db_name)
    try:
        with conn.cursor() as cursor:
            # Set connection character set to utf8 before executing query
            cursor.execute("SET NAMES utf8")
            
            # Execute the actual query
            cursor.execute(sql)
            result = cursor.fetchall()
        return result
    finally:
        conn.close()

def execute_query_df_mysql(sql, db_name: Optional[str] = None):
    """
    Execute SQL query and return pandas DataFrame
    
    Args:
        sql: SQL query statement
        db_name: Specify the database name to connect to, use default config if None
    
    Returns:
        pandas DataFrame
    """
    conn = get_db_mysql_connection(db_name)
    try:
        # Use a temporary cursor to execute the query and get results
        with conn.cursor() as cursor:
            # Set connection character set to utf8 before executing query
            cursor.execute("SET NAMES utf8")
            
            # Execute the actual query
            cursor.execute(sql)
            result = cursor.fetchall()
        
        # If no results, return empty DataFrame
        if not result:
            return pd.DataFrame()
            
        # Manually convert dict results to DataFrame
        df = pd.DataFrame(result)
        return df
    finally:
        conn.close() 