"""
TPC-DS data generation using DuckDB extension
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional
import duckdb
import structlog

logger = structlog.get_logger(__name__)


class TPCDSGenerator:
    """
    Generates TPC-DS data using DuckDB's tpcds extension directly to S3/MinIO
    """
    
    def __init__(self, s3_path: str, s3_config: Dict, warehouse_path: str = "/tmp"):
        """
        Initialize TPC-DS data generator for direct S3/MinIO write
        
        Args:
            s3_path: S3 URL path to store generated data (e.g., s3://bucket/path)
            s3_config: S3/MinIO configuration dictionary
            warehouse_path: Temporary local path for DuckDB operations
        """
        self.s3_path = s3_path
        self.s3_config = s3_config
        self.warehouse_path = Path(warehouse_path)
        self.conn = None
        
        # Ensure local temp directory exists for DuckDB operations
        self.warehouse_path.mkdir(parents=True, exist_ok=True)
        
        if not self._is_s3_path(s3_path):
            raise ValueError(f"Invalid S3 path: {s3_path}. Must start with s3:// or s3a://")
        
    def _is_s3_path(self, path: str) -> bool:
        """Check if path is an S3 URL"""
        return isinstance(path, str) and path.startswith(('s3://', 's3a://'))
        
    def _setup_s3_credentials(self):
        """Setup S3 credentials for DuckDB httpfs extension"""
        if self.s3_config:
            logger.info("Setting up S3 credentials for direct write")
            
            # Set S3 credentials for DuckDB
            if 'access_key' in self.s3_config and 'secret_key' in self.s3_config:
                self.conn.execute(f"SET s3_access_key_id = '{self.s3_config['access_key']}';")
                self.conn.execute(f"SET s3_secret_access_key = '{self.s3_config['secret_key']}';")
            
            # Set S3 endpoint if using MinIO
            if 'endpoint_url' in self.s3_config:
                # Extract host:port from endpoint URL
                endpoint = self.s3_config['endpoint_url'].replace('http://', '').replace('https://', '')
                self.conn.execute(f"SET s3_endpoint = '{endpoint}';")
                
            # Set other S3 options
            self.conn.execute("SET s3_use_ssl = false;")  # Usually false for local MinIO
            self.conn.execute("SET s3_url_style = 'path';")  # Path-style for MinIO compatibility
            
            logger.info("S3 credentials configured for DuckDB")
    
    def __enter__(self):
        """Context manager entry"""
        self.conn = duckdb.connect()
        self._setup_extensions()
        self._setup_s3_credentials()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.conn:
            self.conn.close()
            
    def _setup_extensions(self):
        """Install and load required DuckDB extensions"""
        logger.info("Setting up DuckDB extensions")
        
        # Install and load tpcds extension
        self.conn.execute("INSTALL tpcds;")
        self.conn.execute("LOAD tpcds;")
        
        # Install httpfs for potential remote data access
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        
    def generate_schema(self) -> Dict[str, str]:
        """
        Generate TPC-DS schema without data (scale factor 0)
        
        Returns:
            Dictionary mapping table names to their DDL statements
        """
        logger.info("Generating TPC-DS schema")
        
        # Generate schema with scale factor 0 (no data)
        self.conn.execute("CALL dsdgen(sf = 0);")
        
        # Get list of tables
        tables_result = self.conn.execute("SHOW TABLES;").fetchall()
        table_names = [row[0] for row in tables_result]
        
        # Get schema for each table
        schemas = {}
        for table_name in table_names:
            schema_result = self.conn.execute(f"DESCRIBE {table_name};").fetchall()
            ddl = self._build_ddl(table_name, schema_result)
            schemas[table_name] = ddl
            
        logger.info("Generated schema for tables", table_count=len(schemas))
        return schemas
        
    def _build_ddl(self, table_name: str, schema_info: List) -> str:
        """Build DDL statement from schema information"""
        columns = []
        for row in schema_info:
            col_name, col_type, null, key, default, extra = row
            columns.append(f"{col_name} {col_type}")
            
        return f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n);"
        
    def _export_table(self, table_name: str, output_file: str, format: str):
        """Export a single table to specified format (local or S3)"""
        logger.info("Exporting table", table=table_name, file=output_file)
        
        if format.lower() == "parquet":
            self.conn.execute(f"COPY {table_name} TO '{output_file}' (FORMAT PARQUET);")
        elif format.lower() == "csv":
            self.conn.execute(f"COPY {table_name} TO '{output_file}' (FORMAT CSV, HEADER);")
        elif format.lower() == "json":
            self.conn.execute(f"COPY {table_name} TO '{output_file}' (FORMAT JSON);")
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def generate_data(self, scale_factor: float = 1.0, format: str = "parquet") -> Dict[str, str]:
        """
        Generate TPC-DS data with specified scale factor directly to S3/MinIO
        
        Args:
            scale_factor: TPC-DS scale factor (1.0 = 1GB)
            format: Output format (parquet, csv, json)
            
        Returns:
            Dictionary mapping table names to their S3 file paths
        """
        logger.info("Generating TPC-DS data directly to S3", 
                   scale_factor=scale_factor, 
                   format=format,
                   s3_path=self.s3_path)
        
        # Generate data with specified scale factor
        self.conn.execute(f"CALL dsdgen(sf = {scale_factor});")
        
        # Get list of tables
        tables_result = self.conn.execute("SHOW TABLES;").fetchall()
        table_names = [row[0] for row in tables_result]
        
        # Export each table directly to S3
        exported_files = {}
        for table_name in table_names:
            output_file = f"{self.s3_path.rstrip('/')}/{table_name}.{format}"
            self._export_table(table_name, output_file, format)
            exported_files[table_name] = output_file
            
        logger.info("Generated data files directly to S3", 
                   table_count=len(exported_files),
                   s3_path=self.s3_path)
        return exported_files
            
    def get_table_info(self) -> Dict[str, Dict]:
        """
        Get information about generated tables
        
        Returns:
            Dictionary with table metadata including row counts and sizes
        """
        logger.info("Collecting table information")
        
        tables_result = self.conn.execute("SHOW TABLES;").fetchall()
        table_names = [row[0] for row in tables_result]
        
        table_info = {}
        for table_name in table_names:
            # Get row count
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()
            row_count = count_result[0] if count_result else 0
            
            # Get table schema
            schema_result = self.conn.execute(f"DESCRIBE {table_name};").fetchall()
            
            table_info[table_name] = {
                "row_count": row_count,
                "columns": len(schema_result),
                "schema": schema_result
            }
            
        return table_info
        
    def run_query(self, query_id: int) -> List:
        """
        Run a TPC-DS query using the built-in queries
        
        Args:
            query_id: TPC-DS query number (1-99)
            
        Returns:
            Query results
        """
        logger.info("Running TPC-DS query", query_id=query_id)
        
        if not (1 <= query_id <= 99):
            raise ValueError(f"Query ID must be between 1 and 99, got {query_id}")
            
        result = self.conn.execute(f"PRAGMA tpcds({query_id});").fetchall()
        logger.info("Query completed", query_id=query_id, result_rows=len(result))
        
        return result
        
    def get_query_sql(self, query_id: int) -> str:
        """
        Get the SQL text for a TPC-DS query
        
        Args:
            query_id: TPC-DS query number (1-99)
            
        Returns:
            SQL query string
        """
        if not (1 <= query_id <= 99):
            raise ValueError(f"Query ID must be between 1 and 99, got {query_id}")
            
        # This is a simplified approach - in practice, you might want to
        # extract the actual SQL from DuckDB's tpcds extension
        return f"-- TPC-DS Query {query_id}\nPRAGMA tpcds({query_id});"