"""
Apache Iceberg engine implementation
"""
import logging
from typing import Dict, List, Optional, Any
import structlog
from .base_engine import BaseLakehouseEngine

logger = structlog.get_logger(__name__)


class IcebergEngine(BaseLakehouseEngine):
    """
    Apache Iceberg engine implementation
    """
    
    def __init__(self, spark_session, config: Dict):
        """
        Initialize Iceberg engine
        
        Args:
            spark_session: Active Spark session with Iceberg configured
            config: Engine configuration
        """
        super().__init__(spark_session, config)
        self.catalog_name = config.get('catalog_name', 'lakehouse_catalog')
        self.warehouse_path = config.get('warehouse_path', 's3a://lakehouse/iceberg')
        
    def create_table(self, table_name: str, data_path: str, schema: Optional[str] = None, 
                    partitions: Optional[List[str]] = None, **kwargs) -> bool:
        """
        Create an Iceberg table from data
        
        Args:
            table_name: Name of the Iceberg table to create
            data_path: Path to source data (Parquet files)
            schema: Optional table schema
            partitions: Optional partition columns
            **kwargs: Iceberg-specific parameters
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            # Build full table name with catalog
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            # Create Iceberg table writer
            writer = df.write.format("iceberg")
            
            # Add partitioning if specified
            if partitions:
                writer = writer.partitionBy(*partitions)
                
            # Add Iceberg table properties
            table_properties = kwargs.get('table_properties', {})
            for key, value in table_properties.items():
                writer = writer.option(f"write.{key}", value)
                
            # Write the table
            writer.saveAsTable(full_table_name)
            
            logger.info("Created Iceberg table",
                       table=table_name,
                       full_name=full_table_name,
                       partitions=partitions,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to create Iceberg table",
                        table=table_name,
                        error=str(e))
            return False
            
    def load_data(self, table_name: str, data_path: str, mode: str = "append", **kwargs) -> bool:
        """
        Load data into an Iceberg table
        
        Args:
            table_name: Target Iceberg table name
            data_path: Path to source data
            mode: Load mode (append, overwrite)
            **kwargs: Iceberg-specific parameters
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            # Build full table name
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            # Write to Iceberg table
            writer = df.write.format("iceberg").mode(mode)
            
            # Add any additional options
            for key, value in kwargs.items():
                writer = writer.option(f"write.{key}", value)
                
            writer.saveAsTable(full_table_name)
            
            logger.info("Loaded data into Iceberg table",
                       table=table_name,
                       mode=mode,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to load data into Iceberg table",
                        table=table_name,
                        mode=mode,
                        error=str(e))
            return False
            
    def run_query(self, sql: str, collect_results: bool = False) -> Optional[Any]:
        """
        Execute SQL query on Iceberg tables
        
        Args:
            sql: SQL query string
            collect_results: Whether to collect and return results
            
        Returns:
            Query results if collect_results=True, None otherwise
        """
        try:
            result_df = self.spark.sql(sql)
            
            if collect_results:
                results = result_df.collect()
                logger.info("Executed Iceberg query with results",
                           query_preview=sql[:100],
                           result_count=len(results))
                return results
            else:
                # For write operations, just execute
                result_df.count()  # Force execution
                logger.info("Executed Iceberg query",
                           query_preview=sql[:100])
                return None
                
        except Exception as e:
            logger.error("Failed to execute Iceberg query",
                        query_preview=sql[:100],
                        error=str(e))
            return None
            
    def merge_data(self, target_table: str, source_data: str, 
                  merge_condition: str, when_matched: str, when_not_matched: str) -> bool:
        """
        Perform Iceberg merge operation using MERGE SQL
        
        Args:
            target_table: Target Iceberg table name
            source_data: Source data path or table
            merge_condition: Condition for matching records
            when_matched: Action when records match
            when_not_matched: Action when records don't match
            
        Returns:
            True if successful
        """
        try:
            # Build full table name
            full_target_table = f"{self.catalog_name}.default.{target_table}"
            
            # Create temporary view for source data if it's a file path
            if source_data.endswith('.parquet') or 's3a://' in source_data:
                source_df = self.spark.read.parquet(source_data)
                source_view = f"temp_source_{target_table}"
                source_df.createOrReplaceTempView(source_view)
                source_table = source_view
            else:
                source_table = source_data
                
            # Build MERGE SQL statement
            merge_sql = f"""
            MERGE INTO {full_target_table} AS target
            USING {source_table} AS source
            ON {merge_condition}
            WHEN MATCHED THEN {when_matched}
            WHEN NOT MATCHED THEN {when_not_matched}
            """
            
            # Execute merge
            self.spark.sql(merge_sql)
            
            logger.info("Executed Iceberg merge operation",
                       target_table=target_table,
                       source=source_data,
                       condition=merge_condition)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to execute Iceberg merge",
                        target_table=target_table,
                        source=source_data,
                        error=str(e))
            return False
            
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get Iceberg table metadata and statistics
        
        Args:
            table_name: Name of the Iceberg table
            
        Returns:
            Dictionary with table information
        """
        try:
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            # Get table details
            details_sql = f"DESCRIBE EXTENDED {full_table_name}"
            details_df = self.spark.sql(details_sql)
            details = details_df.collect()
            
            # Get table snapshots
            snapshots_sql = f"SELECT * FROM {full_table_name}.snapshots ORDER BY committed_at DESC LIMIT 10"
            try:
                snapshots_df = self.spark.sql(snapshots_sql)
                snapshots = snapshots_df.collect()
            except:
                snapshots = []
                
            # Get table files
            files_sql = f"SELECT * FROM {full_table_name}.files LIMIT 10"
            try:
                files_df = self.spark.sql(files_sql)
                files = files_df.collect()
            except:
                files = []
                
            # Parse table details
            table_info = {}
            for row in details:
                if hasattr(row, 'col_name') and hasattr(row, 'data_type'):
                    if row.col_name and row.data_type:
                        table_info[row.col_name] = row.data_type
                        
            info = {
                "table_name": table_name,
                "format": "iceberg",
                "catalog": self.catalog_name,
                "table_details": table_info,
                "snapshots": [
                    {
                        "snapshot_id": s.snapshot_id,
                        "committed_at": s.committed_at,
                        "operation": getattr(s, 'operation', None),
                        "summary": getattr(s, 'summary', {})
                    } for s in snapshots[:5]
                ],
                "sample_files": [
                    {
                        "file_path": f.file_path,
                        "file_format": f.file_format,
                        "record_count": f.record_count,
                        "file_size_in_bytes": f.file_size_in_bytes
                    } for f in files[:5]
                ]
            }
            
            logger.info("Retrieved Iceberg table info",
                       table=table_name,
                       snapshots_count=len(snapshots),
                       files_count=len(files))
                       
            return info
            
        except Exception as e:
            logger.error("Failed to get Iceberg table info",
                        table=table_name,
                        error=str(e))
            return {}
            
    def optimize_table(self, table_name: str, **kwargs) -> bool:
        """
        Optimize Iceberg table (rewrite data files, sort)
        
        Args:
            table_name: Name of the Iceberg table to optimize
            **kwargs: Optimization parameters (rewrite_mode, sort_order, etc.)
            
        Returns:
            True if successful
        """
        try:
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            # Get optimization parameters
            rewrite_mode = kwargs.get('rewrite_mode', 'rewrite-data-files')
            sort_columns = kwargs.get('sort_columns', [])
            target_file_size = kwargs.get('target_file_size', '134217728')  # 128MB
            
            if rewrite_mode == 'rewrite-data-files':
                # Rewrite data files for compaction
                rewrite_sql = f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{full_table_name}',
                    options => map(
                        'target-file-size-bytes', '{target_file_size}'
                    )
                )
                """
                self.spark.sql(rewrite_sql)
                
            elif rewrite_mode == 'sort' and sort_columns:
                # Sort table by specified columns
                sort_sql = f"""
                CALL {self.catalog_name}.system.rewrite_data_files(
                    table => '{full_table_name}',
                    strategy => 'sort',
                    sort_order => '{",".join(sort_columns)}'
                )
                """
                self.spark.sql(sort_sql)
                
            logger.info("Optimized Iceberg table",
                       table=table_name,
                       mode=rewrite_mode,
                       sort_columns=sort_columns)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to optimize Iceberg table",
                        table=table_name,
                        error=str(e))
            return False
            
    def expire_snapshots(self, table_name: str, older_than_timestamp: str) -> bool:
        """
        Expire old Iceberg snapshots
        
        Args:
            table_name: Name of the Iceberg table
            older_than_timestamp: Timestamp string (e.g., '2023-01-01 00:00:00')
            
        Returns:
            True if successful
        """
        try:
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            expire_sql = f"""
            CALL {self.catalog_name}.system.expire_snapshots(
                table => '{full_table_name}',
                older_than => TIMESTAMP '{older_than_timestamp}'
            )
            """
            
            self.spark.sql(expire_sql)
            
            logger.info("Expired Iceberg snapshots",
                       table=table_name,
                       older_than=older_than_timestamp)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to expire Iceberg snapshots",
                        table=table_name,
                        error=str(e))
            return False
            
    def time_travel_query(self, table_name: str, snapshot_id: Optional[str] = None, 
                         timestamp: Optional[str] = None) -> Optional[Any]:
        """
        Query Iceberg table at a specific point in time
        
        Args:
            table_name: Name of the Iceberg table
            snapshot_id: Specific snapshot ID
            timestamp: Specific timestamp
            
        Returns:
            DataFrame for the historical data
        """
        try:
            full_table_name = f"{self.catalog_name}.default.{table_name}"
            
            if snapshot_id:
                query_sql = f"SELECT * FROM {full_table_name} VERSION AS OF {snapshot_id}"
            elif timestamp:
                query_sql = f"SELECT * FROM {full_table_name} TIMESTAMP AS OF '{timestamp}'"
            else:
                raise ValueError("Either snapshot_id or timestamp must be provided")
                
            result_df = self.spark.sql(query_sql)
            
            logger.info("Executed Iceberg time travel query",
                       table=table_name,
                       snapshot_id=snapshot_id,
                       timestamp=timestamp)
                       
            return result_df
            
        except Exception as e:
            logger.error("Failed to execute time travel query",
                        table=table_name,
                        error=str(e))
            return None
            
    def validate_setup(self) -> Dict[str, bool]:
        """
        Validate Iceberg engine setup
        
        Returns:
            Dictionary with validation results
        """
        validation = super().validate_setup()
        
        try:
            # Check if Iceberg catalog is available
            try:
                catalogs = self.spark.sql("SHOW CATALOGS").collect()
                catalog_names = [row.catalog for row in catalogs]
                validation["iceberg_catalog"] = self.catalog_name in catalog_names
            except:
                validation["iceberg_catalog"] = False
                
            # Check Iceberg format support
            try:
                # Try to create a simple test table
                test_df = self.spark.range(1)
                test_table = f"{self.catalog_name}.default._test_iceberg"
                test_df.write.format("iceberg").mode("overwrite").saveAsTable(test_table)
                self.spark.sql(f"DROP TABLE IF EXISTS {test_table}")
                validation["iceberg_format"] = True
            except:
                validation["iceberg_format"] = False
                
            # Check warehouse path access
            try:
                test_path = f"{self.warehouse_path}/_test"
                test_df = self.spark.range(1)
                test_df.write.mode("overwrite").parquet(test_path)
                validation["warehouse_access"] = True
            except:
                validation["warehouse_access"] = False
                
        except Exception as e:
            logger.error("Iceberg validation failed", error=str(e))
            
        return validation
