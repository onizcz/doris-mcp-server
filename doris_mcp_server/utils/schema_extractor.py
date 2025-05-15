"""
Metadata Extraction Tool

Responsible for extracting table structures, relationships, and other metadata from the database.
"""

import os
import json
import pandas as pd
import re
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Import unified logging configuration
from doris_mcp_server.utils.logger import get_logger

# Configure logging
logger = get_logger(__name__)

# Load environment variables
load_dotenv(override=True)

METADATA_DB_NAME="information_schema"
ENABLE_MULTI_DATABASE=os.getenv("ENABLE_MULTI_DATABASE",True)
MULTI_DATABASE_NAMES=os.getenv("MULTI_DATABASE_NAMES","")

# Import local modules
from doris_mcp_server.utils.db import execute_query_df, execute_query
from doris_mcp_server.utils.dbMysql import execute_query_df_mysql, execute_query_mysql

class MetadataExtractor:
    """Apache Doris Metadata Extractor"""
    
    def __init__(self, db_name: str = None):
        """
        Initialize the metadata extractor
        
        Args:
            db_name: Default database name, uses the currently connected database if not specified
        """
        # Get configuration from environment variables
        self.db_name = db_name or os.getenv("DB_DATABASE", "")
        self.metadata_db = METADATA_DB_NAME  # Use constant
        
        # Caching system
        self.metadata_cache = {}
        self.metadata_cache_time = {}
        self.cache_ttl = int(os.getenv("METADATA_CACHE_TTL", "3600"))  # Default cache 1 hour
        
        # Refresh time
        self.last_refresh_time = None
        
        # Enable multi-database support - use variable imported from db.py
        self.enable_multi_database = ENABLE_MULTI_DATABASE
        
        # Load table hierarchy matching configuration
        self.enable_table_hierarchy = os.getenv("ENABLE_TABLE_HIERARCHY", "false").lower() == "true"
        if self.enable_table_hierarchy:
            self.table_hierarchy_patterns = self._load_table_hierarchy_patterns()
        else:
            self.table_hierarchy_patterns = []
        
        # List of excluded system databases
        self.excluded_databases = self._load_excluded_databases()
        
    def _load_excluded_databases(self) -> List[str]:
        """
        Load the list of excluded databases configuration
        
        Returns:
            List of excluded databases
        """
        excluded_dbs_str = os.getenv("EXCLUDED_DATABASES", 
                               '["information_schema", "mysql", "performance_schema", "sys", "doris_metadata"]')
        try:
            excluded_dbs = json.loads(excluded_dbs_str)
            if isinstance(excluded_dbs, list):
                logger.info(f"Loaded excluded database list: {excluded_dbs}")
                return excluded_dbs
            else:
                logger.warning("Excluded database list configuration is not in list format, using default value")
        except json.JSONDecodeError:
            logger.warning("Error parsing excluded database list JSON, using default value")
        
        # Default value
        default_excluded_dbs = ["information_schema", "mysql", "performance_schema", "sys", "doris_metadata"]
        return default_excluded_dbs
        
    def _load_table_hierarchy_patterns(self) -> List[str]:
        """
        Load table hierarchy matching pattern configuration
        
        Returns:
            List of table hierarchy matching regular expressions
        """
        patterns_str = os.getenv("TABLE_HIERARCHY_PATTERNS", 
                               '["^ads_.*$","^dim_.*$","^dws_.*$","^dwd_.*$","^ods_.*$","^tmp_.*$","^stg_.*$","^.*$"]')
        try:
            patterns = json.loads(patterns_str)
            if isinstance(patterns, list):
                # Ensure all patterns are valid regular expressions
                validated_patterns = []
                for pattern in patterns:
                    try:
                        re.compile(pattern)
                        validated_patterns.append(pattern)
                    except re.error:
                        logger.warning(f"Invalid regular expression pattern: {pattern}")
                
                logger.info(f"Loaded table hierarchy matching patterns: {validated_patterns}")
                return validated_patterns
            else:
                logger.warning("Table hierarchy matching pattern configuration is not in list format, using default value")
        except json.JSONDecodeError:
            logger.warning("Error parsing table hierarchy matching pattern JSON, using default value")
        
        # Default value
        default_patterns = ["^ads_.*$", "^dim_.*$", "^dws_.*$", "^dwd_.*$", "^ods_.*$", "^.*$"]
        return default_patterns
        
    def get_all_databases(self) -> List[str]:
        """
        Get a list of all databases
        
        Returns:
            List of database names
        """
        cache_key = "databases"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Use information_schema.schemata table to get database list
            query = """
            SELECT 
                SCHEMA_NAME 
            FROM 
                information_schema.schemata 
            WHERE 
                SCHEMA_NAME IN ('topwalk_dw', 'announce', 'asset-center', 'emergency', 'situation', 'vulnerability', 'base')
            ORDER BY 
                SCHEMA_NAME
            """
            
            result = execute_query(query)
            databases = []
            if result:
                databases = [db["SCHEMA_NAME"] for db in result]
                logger.info(f"Retrieved database list: {databases}")

            result2 = execute_query_mysql(query)
            if result2:
                databases2 = ["mysql_catalog_bigdata." + db["SCHEMA_NAME"] for db in result2]
                databases.extend(databases2)
                logger.info(f"Retrieved mysql database list: {databases}")
            
            # Update cache
            self.metadata_cache[cache_key] = databases
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return databases
        except Exception as e:
            logger.error(f"Error getting database list: {str(e)}")
            return []

    def get_all_target_databases(self) -> List[str]:
        """
        Get all target databases
        
        If multi-database support is enabled, returns all databases from the configuration;
        Otherwise, returns the current database
        
        Returns:
            List of target databases
        """
        if self.enable_multi_database:
            # Get multi-database list from configuration
            from doris_mcp_server.utils.db import MULTI_DATABASE_NAMES
            
            # If configuration is empty, return current database and all databases in the system
            if not MULTI_DATABASE_NAMES:
                all_dbs = self.get_all_databases()
                # Put the current database at the front
                if self.db_name in all_dbs:
                    all_dbs.remove(self.db_name)
                    all_dbs = [self.db_name] + all_dbs
                
                # Filter out excluded databases
                all_dbs = [db for db in all_dbs if db not in self.excluded_databases]
                logger.info(f"Multi-database list not configured, getting database list from system: {all_dbs}")
                return all_dbs
            else:
                # Ensure the current database is in the list and at the front
                db_names = list(MULTI_DATABASE_NAMES)  # Copy to avoid modifying the original list
                if self.db_name and self.db_name not in db_names:
                    db_names.insert(0, self.db_name)
                elif self.db_name and self.db_name in db_names:
                    # If current database is in the list but not first, adjust position
                    db_names.remove(self.db_name)
                    db_names.insert(0, self.db_name)
                
                # Filter out excluded databases
                db_names = [db for db in db_names if db not in self.excluded_databases]
                logger.info(f"Using configured multi-database list: {db_names}")
                return db_names
        else:
            # Return only the current database
            if self.db_name in self.excluded_databases:
                logger.warning(f"Current database {self.db_name} is in the excluded list, metadata retrieval might not work properly")
            return [self.db_name] if self.db_name else []
    
    def get_database_tables(self, db_name: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get a list of all tables in the database
        
        Args:
            db_name: Database name, uses current database if None
            
        Returns:
            List of table names
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("Database name not specified")
            return []
        
        cache_key = f"tables_{db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Use information_schema.tables table to get table list
            query = f"""
            SELECT 
                TABLE_NAME,TABLE_COMMENT  
            FROM 
                information_schema.tables 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_TYPE = 'BASE TABLE'
            """


            if "mysql_catalog_bigdata." in db_name:
                db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                query2 = f"""
                SELECT
                    TABLE_NAME,TABLE_COMMENT 
                FROM
                    information_schema.tables
                WHERE
                    TABLE_SCHEMA = '{db_name2}'
                    AND TABLE_TYPE = 'BASE TABLE'
                """
                result = execute_query_mysql(query2, db_name2)
            else:
                result = execute_query(query, db_name)

            logger.info(f"{db_name}.information_schema.tables query result: {result}")



            if not result:
                tables_info = []
            else:
                # 构造包含表名和表注释的字典列表
                tables_info = [{
                    'table_name': table['TABLE_NAME'],
                    'table_comment': table['TABLE_COMMENT'] if table['TABLE_COMMENT'] else ''  # 处理可能的None值
                } for table in result]
                logger.info(f"Table names retrieved from {db_name}.information_schema.tables: {tables_info}")

            # 如果需要按层次结构排序
            if self.enable_table_hierarchy and tables_info:
                # 注意：这里需要调整排序逻辑，因为现在处理的是字典而非单纯的表名
                tables_info = self._sort_tables_by_hierarchy([table['table_name'] for table in tables_info])
                # 或者修改 _sort_tables_by_hierarchy 方法以处理字典列表
            
            # Update cache
            self.metadata_cache[cache_key] = tables_info
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return tables_info
        except Exception as e:
            logger.error(f"Error getting table list: {str(e)}")
            return []
    
    def get_all_tables_and_columns(self) -> Dict[str, Any]:
        """
        Get information for all tables and columns
        
        Returns:
            Dict[str, Any]: Dictionary containing information for all tables and columns
        """
        cache_key = f"all_tables_columns_{self.db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            result = {}
            tables = self.get_database_tables(self.db_name)

            for table_info in tables:
                table_name = table_info['table_name']
                schema = self.get_table_schema(table_name, self.db_name)
                if schema:
                    columns = schema.get("columns", [])
                    column_names = [col.get("name") for col in columns if col.get("name")]
                    column_types = {col.get("name"): col.get("type") for col in columns if col.get("name") and col.get("type")}
                    column_comments = {col.get("name"): col.get("comment") for col in columns if col.get("name")}
                    
                    result[table_name] = {
                        "comment": schema.get("comment", ""),
                        "columns": column_names,
                        "column_types": column_types,
                        "column_comments": column_comments
                    }
            
            # Update cache
            self.metadata_cache[cache_key] = result
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return result
        except Exception as e:
            logger.error(f"Error getting all tables and columns information: {str(e)}")
            return {}
    
    def _sort_tables_by_hierarchy(self, tables: List[str]) -> List[str]:
        """
        Sort tables based on hierarchy matching patterns
        
        Args:
            tables: List of table names
            
        Returns:
            Sorted list of table names
        """
        if not self.enable_table_hierarchy or not self.table_hierarchy_patterns:
            return tables
        
        # Group tables by pattern priority
        table_groups = []
        remaining_tables = set(tables)
        
        for pattern in self.table_hierarchy_patterns:
            matching_tables = []
            regex = re.compile(pattern)
            
            for table in list(remaining_tables):
                if regex.match(table):
                    matching_tables.append(table)
                    remaining_tables.remove(table)
            
            if matching_tables:
                # Within each group, sort alphabetically
                matching_tables.sort()
                table_groups.append(matching_tables)
        
        # Add remaining tables to the end
        if remaining_tables:
            table_groups.append(sorted(list(remaining_tables)))
        
        # Flatten the groups
        return [table for group in table_groups for table in group]
    
    def get_all_tables_from_all_databases(self) -> Dict[str, List[str]]:
        """
        Get all tables from all target databases
        
        Returns:
            Mapping from database name to list of table names
        """
        all_tables = {}
        target_dbs = self.get_all_target_databases()
        
        for db_name in target_dbs:
            tables = self.get_database_tables(db_name)
            if tables:
                all_tables[db_name] = tables
        
        return all_tables
    
    def find_tables_by_pattern(self, pattern: str, db_name: Optional[str] = None) -> List[Tuple[str, str]]:
        """
        Find matching tables in the database based on a pattern
        
        Args:
            pattern: Table name pattern (regular expression)
            db_name: Database name, searches all target databases if None
            
        Returns:
            List of matching (database_name, table_name) tuples
        """
        try:
            regex = re.compile(pattern)
        except re.error:
            logger.error(f"Invalid regular expression pattern: {pattern}")
            return []
        
        matches = []
        
        if db_name:
            # Search only in the specified database
            tables = self.get_database_tables(db_name)
            matches = [(db_name, table['table_name']) for table in tables if regex.match(table['table_name'])]
        else:
            # Search in all target databases
            all_tables = self.get_all_tables_from_all_databases()
            
            for db, tables in all_tables.items():
                db_matches = [(db, table['table_name']) for table in tables if regex.match(table['table_name'])]
                matches.extend(db_matches)
        
        return matches
    
    def get_table_schema(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the schema information for a table
        
        Args:
            table_name: Table name
            db_name: Database name, uses current database if None
            
        Returns:
            Table schema information, including column names, types, nullability, defaults, comments, etc.
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("Database name not specified")
            return {}
        
        cache_key = f"schema_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Use information_schema.columns table to get table schema
            query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                IS_NULLABLE, 
                COLUMN_DEFAULT, 
                COLUMN_COMMENT,
                ORDINAL_POSITION,
                COLUMN_KEY,
                EXTRA
            FROM 
                information_schema.columns 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                ORDINAL_POSITION
            """

            if "mysql_catalog_bigdata." in db_name:
                db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                query2 = f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_COMMENT,
                    ORDINAL_POSITION,
                    COLUMN_KEY,
                    EXTRA
                FROM
                    information_schema.columns
                WHERE
                    TABLE_SCHEMA = '{db_name2}'
                    AND TABLE_NAME = '{table_name}'
                ORDER BY
                   ORDINAL_POSITION
                """
                result = execute_query_mysql(query2)
            else:
                result = execute_query(query)



            if not result:
                logger.warning(f"Table {db_name}.{table_name} does not exist or has no columns")
                return {}

            # Create structured table schema information
            columns = []
            for col in result:
                # Ensure using actual column values, not column names
                column_info = {
                    "name": col.get("COLUMN_NAME", ""),
                    "type": col.get("DATA_TYPE", ""),
                    "nullable": col.get("IS_NULLABLE", "") == "YES",
                    "default": col.get("COLUMN_DEFAULT", ""),
                    "comment": col.get("COLUMN_COMMENT", "") or "",
                    "position": col.get("ORDINAL_POSITION", ""),
                    "key": col.get("COLUMN_KEY", "") or "",
                    "extra": col.get("EXTRA", "") or ""
                }
                columns.append(column_info)

            # Get table comment
            table_comment = self.get_table_comment(table_name, db_name)
            
            # Build complete structure
            schema = {
                "name": table_name,
                "database": db_name,
                "comment": table_comment,
                "columns": columns,
                "create_time": datetime.now().isoformat()
            }
            
            # Get table type information
            try:
                table_type_query = f"""
                SELECT 
                    TABLE_TYPE,
                    ENGINE 
                FROM 
                    information_schema.tables 
                WHERE 
                    TABLE_SCHEMA = '{db_name}' 
                    AND TABLE_NAME = '{table_name}'
                """
                if "mysql_catalog_bigdata." in db_name:
                    db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                    table_type_query2 = f"""
                    SELECT
                        TABLE_TYPE,
                        ENGINE
                    FROM
                        information_schema.tables
                    WHERE
                        TABLE_SCHEMA = '{db_name2}'
                        AND TABLE_NAME = '{table_name}'
                    """
                    table_type_result = execute_query_df_mysql(table_type_query2)
                else:
                    table_type_result = execute_query(table_type_query)
                if table_type_result:
                    schema["table_type"] = table_type_result[0].get("TABLE_TYPE", "")
                    schema["engine"] = table_type_result[0].get("ENGINE", "")
            except Exception as e:
                logger.warning(f"Error getting table type information: {str(e)}")
            
            # Update cache
            self.metadata_cache[cache_key] = schema
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return schema
        except Exception as e:
            logger.error(f"Error getting table schema: {str(e)}")
            return {}
    
    def get_table_comment(self, table_name: str, db_name: Optional[str] = None) -> str:
        """
        Get the comment for a table
        
        Args:
            table_name: Table name
            db_name: Database name, uses current database if None
            
        Returns:
            Table comment
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("Database name not specified")
            return ""
        
        cache_key = f"table_comment_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Use information_schema.tables table to get table comment
            query = f"""
            SELECT 
                TABLE_COMMENT 
            FROM 
                information_schema.tables 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            """
            if "mysql_catalog_bigdata." in db_name:
                db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                query2 = f"""
                SELECT
                    TABLE_COMMENT
                FROM
                    information_schema.tables
                WHERE
                    TABLE_SCHEMA = '{db_name2}'
                    AND TABLE_NAME = '{table_name}'
                """
                result = execute_query_mysql(query2)
            else:
                result = execute_query(query)

            if not result or not result[0]:
                comment = ""
            else:
                comment = result[0].get("TABLE_COMMENT", "")
            
            # Update cache
            self.metadata_cache[cache_key] = comment
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return comment
        except Exception as e:
            logger.error(f"Error getting table comment: {str(e)}")
            return ""
    
    def get_column_comments(self, table_name: str, db_name: Optional[str] = None) -> Dict[str, str]:
        """
        Get comments for all columns in a table
        
        Args:
            table_name: Table name
            db_name: Database name, uses current database if None
            
        Returns:
            Dictionary of column names and comments
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.warning("Database name not specified")
            return {}
        
        cache_key = f"column_comments_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Use information_schema.columns table to get column comments
            query = f"""
            SELECT 
                COLUMN_NAME, 
                COLUMN_COMMENT 
            FROM 
                information_schema.columns 
            WHERE 
                TABLE_SCHEMA = '{db_name}' 
                AND TABLE_NAME = '{table_name}'
            ORDER BY 
                ORDINAL_POSITION
            """
            if "mysql_catalog_bigdata." in db_name:
                db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                query2 = f"""
                SELECT
                    COLUMN_NAME,
                    COLUMN_COMMENT
                FROM
                    information_schema.columns
                WHERE
                    TABLE_SCHEMA = '{db_name2}'
                    AND TABLE_NAME = '{table_name}'
                ORDER BY
                    ORDINAL_POSITION
                """
                result = execute_query_mysql(query2)
            else:
                result = execute_query(query)

            comments = {}
            for col in result:
                column_name = col.get("COLUMN_NAME", "")
                column_comment = col.get("COLUMN_COMMENT", "")
                if column_name:
                    comments[column_name] = column_comment
            
            # Update cache
            self.metadata_cache[cache_key] = comments
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return comments
        except Exception as e:
            logger.error(f"Error getting column comments: {str(e)}")
            return {}
    
    def get_table_indexes(self, table_name: str, db_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the index information for a table
        
        Args:
            table_name: Table name
            db_name: Database name, uses the database specified during initialization if None
            
        Returns:
            List[Dict[str, Any]]: List of index information
        """
        db_name = db_name or self.db_name
        if not db_name:
            logger.error("Database name not specified")
            return []
        
        cache_key = f"indexes_{db_name}_{table_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            query = f"SHOW INDEX FROM `{db_name}`.`{table_name}`"
            if "mysql_catalog_bigdata." in db_name:
                db_name2 = db_name.removeprefix("mysql_catalog_bigdata.")
                query2 = f"SHOW INDEX FROM `{db_name2}`.`{table_name}`"
                df = execute_query_df_mysql(query2)
            else:
                df = execute_query_df(query)
            
            # Process results
            indexes = []
            current_index = None
            
            for _, row in df.iterrows():
                index_name = row['Key_name']
                column_name = row['Column_name']
                
                if current_index is None or current_index['name'] != index_name:
                    if current_index is not None:
                        indexes.append(current_index)
                    
                    current_index = {
                        'name': index_name,
                        'columns': [column_name],
                        'unique': row['Non_unique'] == 0,
                        'type': row['Index_type']
                    }
                else:
                    current_index['columns'].append(column_name)
            
            if current_index is not None:
                indexes.append(current_index)
            
            # Update cache
            self.metadata_cache[cache_key] = indexes
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return indexes
        except Exception as e:
            logger.error(f"Error getting index information: {str(e)}")
            return []
    
    def get_table_relationships(self) -> List[Dict[str, Any]]:
        """
        Infer table relationships from table comments and naming patterns
        
        Returns:
            List[Dict[str, Any]]: List of table relationship information
        """
        cache_key = f"relationships_{self.db_name}"
        if cache_key in self.metadata_cache and (datetime.now() - self.metadata_cache_time.get(cache_key, datetime.min)).total_seconds() < self.cache_ttl:
            return self.metadata_cache[cache_key]
        
        try:
            # Get all tables
            tables = self.get_database_tables(self.db_name)
            relationships = []
            
            # Simple foreign key naming convention detection
            # Example: If a table has a column named xxx_id and another table named xxx exists, it might be a foreign key relationship
            for table_info in tables:
                table_name = table_info['table_name']
                schema = self.get_table_schema(table_name, self.db_name)
                columns = schema.get("columns", [])
                
                for column in columns:
                    column_name = column["name"]
                    if column_name.endswith('_id'):
                        # Possible foreign key table name
                        ref_table_name = column_name[:-3]  # Remove _id suffix
                        
                        # Check if the possible table exists
                        for ref_table_info in tables:
                            ref_table_name = ref_table_info['table_name']
                            # Find possible primary key column
                            ref_schema = self.get_table_schema(ref_table_name, self.db_name)
                            ref_columns = ref_schema.get("columns", [])
                            
                            # Assume primary key column name is id
                            if any(col["name"] == "id" for col in ref_columns):
                                relationships.append({
                                    "table": table_name,
                                    "column": column_name,
                                    "references_table": ref_table_name,
                                    "references_column": "id",
                                    "relationship_type": "many-to-one",
                                    "confidence": "medium"  # Low confidence, based on naming convention
                                })
            
            # Update cache
            self.metadata_cache[cache_key] = relationships
            self.metadata_cache_time[cache_key] = datetime.now()
            
            return relationships
        except Exception as e:
            logger.error(f"Error inferring table relationships: {str(e)}")
            return []
    
    def get_recent_audit_logs(self, days: int = 7, limit: int = 100) -> pd.DataFrame:
        """
        Get recent audit logs
        
        Args:
            days: Get audit logs for the last N days
            limit: Maximum number of records to return
            
        Returns:
            pd.DataFrame: Audit log DataFrame
        """
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            query = f"""
            SELECT client_ip, user, db, time, stmt_id, stmt, state, error_code
            FROM `__internal_schema`.`audit_log`
            WHERE `time` >= '{start_date}'
            AND state = 'EOF' AND error_code = 0
            AND `stmt` NOT LIKE 'SHOW%'
            AND `stmt` NOT LIKE 'DESC%'
            AND `stmt` NOT LIKE 'EXPLAIN%'
            AND `stmt` NOT LIKE 'SELECT 1%'
            ORDER BY time DESC
            LIMIT {limit}
            """
            df = execute_query_df(query)
            return df
        except Exception as e:
            logger.error(f"Error getting audit logs: {str(e)}")
            return pd.DataFrame()
    
    def extract_sql_comments(self, sql: str) -> str:
        """
        Extract comments from SQL
        
        Args:
            sql: SQL query
            
        Returns:
            str: Extracted comments
        """
        # Extract single-line comments
        single_line_comments = re.findall(r'--\s*(.*?)(?:\n|$)', sql)
        
        # Extract multi-line comments
        multi_line_comments = re.findall(r'/\*(.*?)\*/', sql, re.DOTALL)
        
        # Merge all comments
        all_comments = single_line_comments + multi_line_comments
        return '\n'.join(comment.strip() for comment in all_comments if comment.strip())
    
    def extract_common_sql_patterns(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Extract common SQL patterns
        
        Args:
            limit: Maximum number of audit logs to retrieve
            
        Returns:
            List[Dict[str, Any]]: List of SQL pattern information, including pattern, type, frequency, etc.
        """
        try:
            # Get audit logs
            audit_logs = self.get_recent_audit_logs(days=30, limit=limit)
            if audit_logs.empty:
                # If audit logs cannot be retrieved, return some default patterns
                default_patterns = [
                    {
                        "pattern": "SELECT * FROM {table} WHERE {condition}",
                        "type": "SELECT",
                        "frequency": 1
                    },
                    {
                        "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                        "type": "SELECT",
                        "frequency": 1
                    }
                ]
                return default_patterns
            
            # Group and process by SQL type
            patterns_by_type = {}
            for _, row in audit_logs.iterrows():
                sql = row['stmt']
                if not sql:
                    continue
                
                # Determine SQL type
                sql_type = self._get_sql_type(sql)
                if not sql_type:
                    continue
                
                # Simplify SQL
                simplified_sql = self._simplify_sql(sql)
                
                # Extract involved tables
                tables = self._extract_tables_from_sql(sql)
                
                # Extract SQL comments
                comments = self.extract_sql_comments(sql)
                
                # Initialize if it's a new pattern
                if sql_type not in patterns_by_type:
                    patterns_by_type[sql_type] = []
                    
                # Check if a similar pattern exists
                found_similar = False
                for pattern in patterns_by_type[sql_type]:
                    if self._are_sqls_similar(simplified_sql, pattern['simplified_sql']):
                        pattern['count'] += 1
                        pattern['examples'].append(sql)
                        if comments:
                            pattern['comments'].append(comments)
                        found_similar = True
                        break
                        
                # If no similar pattern found, add new pattern
                if not found_similar:
                    patterns_by_type[sql_type].append({
                        'simplified_sql': simplified_sql,
                        'examples': [sql],
                        'comments': [comments] if comments else [],
                        'count': 1,
                        'tables': tables
                    })
                    
            # Convert grouped patterns to the required output format
            result_patterns = []
            
            # Sort by frequency and convert format
            for sql_type, type_patterns in patterns_by_type.items():
                sorted_patterns = sorted(type_patterns, key=lambda x: x['count'], reverse=True)
                
                # Extract top 3 patterns and convert to expected format
                for pattern in sorted_patterns[:3]:
                    # Create output consistent with the format used in _update_sql_patterns_for_all_databases
                    result_patterns.append({
                        "pattern": pattern['simplified_sql'],
                        "type": sql_type,
                        "frequency": pattern['count'],
                        "examples": json.dumps(pattern['examples'][:3], ensure_ascii=False),
                        "comments": json.dumps(pattern['comments'][:3], ensure_ascii=False) if pattern['comments'] else "[]",
                        "tables": json.dumps(pattern['tables'], ensure_ascii=False)
                    })
            
            # If no patterns found, return default values
            if not result_patterns:
                default_patterns = [
                    {
                        "pattern": "SELECT * FROM {table} WHERE {condition}",
                        "type": "SELECT",
                        "frequency": 1,
                        "examples": "[]",
                        "comments": "[]",
                        "tables": "[]"
                    },
                    {
                        "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                        "type": "SELECT",
                        "frequency": 1,
                        "examples": "[]",
                        "comments": "[]",
                        "tables": "[]"
                    }
                ]
                return default_patterns
            
            return result_patterns
            
        except Exception as e:
            logger.error(f"Error extracting SQL patterns: {str(e)}")
            # Return some default patterns to ensure subsequent processing doesn't fail
            default_patterns = [
                {
                    "pattern": "SELECT * FROM {table} WHERE {condition}",
                    "type": "SELECT",
                    "frequency": 1,
                    "examples": "[]",
                    "comments": "[]",
                    "tables": "[]"
                },
                {
                    "pattern": "SELECT {columns} FROM {table} GROUP BY {group_by} ORDER BY {order_by} LIMIT {limit}",
                    "type": "SELECT",
                    "frequency": 1,
                    "examples": "[]",
                    "comments": "[]",
                    "tables": "[]"
                }
            ]
            return default_patterns
    
    def _simplify_sql(self, sql: str) -> str:
        """
        Simplify SQL for better pattern recognition
        
        Args:
            sql: SQL query
            
        Returns:
            str: Simplified SQL
        """
        # Remove comments
        sql = re.sub(r'--.*?(\n|$)', ' ', sql)
        sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
        
        # Replace string and numeric constants
        sql = re.sub(r"'[^']*'", "'?'", sql)
        sql = re.sub(r'\b\d+\b', '?', sql)
        
        # Replace contents of IN clauses
        sql = re.sub(r'IN\s*\([^)]+\)', 'IN (?)', sql, flags=re.IGNORECASE)
        
        # Remove excess whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        return sql
    
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """
        Extract table names from SQL
        
        Args:
            sql: SQL query
            
        Returns:
            List[str]: List of table names
        """
        # This is a very simplified implementation
        # Real applications require more complex SQL parsing
        tables = set()
        
        # Find table names after FROM clause
        from_matches = re.finditer(r'\bFROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in from_matches:
            tables.add(match.group(1))
        
        # Find table names after JOIN clause
        join_matches = re.finditer(r'\bJOIN\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in join_matches:
            tables.add(match.group(1))
        
        # Find table names after INSERT INTO
        insert_matches = re.finditer(r'\bINSERT\s+INTO\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in insert_matches:
            tables.add(match.group(1))
        
        # Find table names after UPDATE
        update_matches = re.finditer(r'\bUPDATE\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in update_matches:
            tables.add(match.group(1))
        
        # Find table names after DELETE FROM
        delete_matches = re.finditer(r'\bDELETE\s+FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
        for match in delete_matches:
            tables.add(match.group(1))
        
        return list(tables)
    
    
    
    def get_table_partition_info(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get partition information for a table
        
        Args:
            db_name: Database name
            table_name: Table name
            
        Returns:
            Dict: Partition information
        """
        try:
            # Get partition information
            query = f"""
            SELECT 
                PARTITION_NAME,
                PARTITION_EXPRESSION,
                PARTITION_DESCRIPTION,
                TABLE_ROWS
            FROM 
                information_schema.partitions
            WHERE 
                TABLE_SCHEMA = '{db_name}'
                AND TABLE_NAME = '{table_name}'
            """
            
            partitions = execute_query(query)
            
            if not partitions:
                return {}
                
            partition_info = {
                "has_partitions": True,
                "partitions": []
            }
            
            for part in partitions:
                partition_info["partitions"].append({
                    "name": part.get("PARTITION_NAME", ""),
                    "expression": part.get("PARTITION_EXPRESSION", ""),
                    "description": part.get("PARTITION_DESCRIPTION", ""),
                    "rows": part.get("TABLE_ROWS", 0)
                })
                
            return partition_info
        except Exception as e:
            logger.error(f"Error getting partition information for table {db_name}.{table_name}: {str(e)}")
            return {}