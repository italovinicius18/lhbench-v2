"""
Delta Lake engine implementation
"""
import logging
from typing import Dict, List, Optional, Any
import structlog
from .base_engine import BaseLakehouseEngine

logger = structlog.get_logger(__name__)


class DeltaEngine(BaseLakehouseEngine):
    """
    Delta Lake engine implementation
    """
    
    def __init__(self, spark_session, config: Dict):
        """
        Initialize Delta engine
        
        Args:
            spark_session: Active Spark session with Delta configured
            config: Engine configuration
        """
        super().__init__(spark_session, config)
        self.storage_path = config.get('storage_path', 's3a://lakehouse/delta')
        
    def create_table(self, table_name: str, data_path: str, schema: Optional[str] = None, 
                    partitions: Optional[List[str]] = None, **kwargs) -> bool:
        """
        Create a Delta table from data
        
        Args:
            table_name: Name of the Delta table to create
            data_path: Path to source data (Parquet files)
            schema: Optional table schema
            partitions: Optional partition columns
            **kwargs: Delta-specific parameters (location, properties, etc.)
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            # Build table location
            table_location = kwargs.get('location', f"{self.storage_path}/{table_name}")
            
            # Create Delta table writer
            writer = df.write.format("delta")
            
            # Set table location
            writer = writer.option("path", table_location)
            
            # Add partitioning if specified
            if partitions:
                writer = writer.partitionBy(*partitions)
                
            # Add Delta table properties
            table_properties = kwargs.get('table_properties', {})
            for key, value in table_properties.items():
                writer = writer.option(key, value)
                
            # Write the table
            writer.saveAsTable(table_name)
            
            logger.info("Created Delta table",
                       table=table_name,
                       location=table_location,
                       partitions=partitions,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to create Delta table",
                        table=table_name,
                        error=str(e))
            return False
            
    def load_data(self, table_name: str, data_path: str, mode: str = "append", **kwargs) -> bool:
        """
        Load data into a Delta table
        
        Args:
            table_name: Target Delta table name
            data_path: Path to source data
            mode: Load mode (append, overwrite, merge)
            **kwargs: Delta-specific parameters
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            if mode == "merge":
                # Handle merge operation separately
                merge_key = kwargs.get('merge_key', 'id')
                return self._merge_data_simple(table_name, df, merge_key)
            else:
                # Standard write operation
                writer = df.write.format("delta").mode(mode)
                
                # Add any additional options
                for key, value in kwargs.items():
                    if key not in ['merge_key']:
                        writer = writer.option(key, value)
                        
                writer.saveAsTable(table_name)
                
            logger.info("Loaded data into Delta table",
                       table=table_name,
                       mode=mode,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to load data into Delta table",
                        table=table_name,
                        mode=mode,
                        error=str(e))
            return False
            
    def run_query(self, sql: str, collect_results: bool = False) -> Optional[Any]:
        """
        Execute SQL query on Delta tables
        
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
                logger.info("Executed Delta query with results",
                           query_preview=sql[:100],
                           result_count=len(results))
                return results
            else:
                # For write operations, just execute
                result_df.count()  # Force execution
                logger.info("Executed Delta query",
                           query_preview=sql[:100])
                return None
                
        except Exception as e:
            logger.error("Failed to execute Delta query",
                        query_preview=sql[:100],
                        error=str(e))
            return None
            
    def merge_data(self, target_table: str, source_data: str, 
                  merge_condition: str, when_matched: str, when_not_matched: str) -> bool:
        """
        Perform Delta merge operation
        
        Args:
            target_table: Target Delta table name
            source_data: Source data path or table
            merge_condition: Condition for matching records
            when_matched: Action when records match (UPDATE SET, DELETE)
            when_not_matched: Action when records don't match (INSERT)
            
        Returns:
            True if successful
        """
        try:
            # Import DeltaTable
            from delta.tables import DeltaTable
            
            # Get target table
            delta_table = DeltaTable.forName(self.spark, target_table)
            
            # Read source data
            if source_data.endswith('.parquet') or 's3a://' in source_data:
                source_df = self.spark.read.parquet(source_data)
                source_alias = "source"
            else:
                # Assume it's a table name
                source_df = self.spark.table(source_data)
                source_alias = source_data
                
            # Build merge query
            merge_builder = delta_table.alias("target").merge(
                source_df.alias(source_alias),
                merge_condition
            )
            
            # Add when matched clause
            if when_matched.upper().startswith("UPDATE"):
                update_exprs = self._parse_update_expressions(when_matched)
                merge_builder = merge_builder.whenMatchedUpdate(set=update_exprs)
            elif when_matched.upper() == "DELETE":
                merge_builder = merge_builder.whenMatchedDelete()
                
            # Add when not matched clause
            if when_not_matched.upper().startswith("INSERT"):
                insert_exprs = self._parse_insert_expressions(when_not_matched)
                merge_builder = merge_builder.whenNotMatchedInsert(values=insert_exprs)
                
            # Execute merge
            merge_builder.execute()
            
            logger.info("Executed Delta merge operation",
                       target_table=target_table,
                       source=source_data,
                       condition=merge_condition)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to execute Delta merge",
                        target_table=target_table,
                        source=source_data,
                        error=str(e))
            return False
            
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get Delta table metadata and statistics
        
        Args:
            table_name: Name of the Delta table
            
        Returns:
            Dictionary with table information
        """
        try:
            from delta.tables import DeltaTable
            
            # Get Delta table
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Get table details
            details = delta_table.detail().collect()[0]
            
            # Get table history (last few operations)
            history = delta_table.history(5).collect()
            
            # Get table statistics
            stats_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
            stats = stats_df.collect()[0]
            
            info = {
                "table_name": table_name,
                "format": "delta",
                "location": details.location,
                "created_at": details.createdAt,
                "last_modified": details.lastModified,
                "version": details.version,
                "num_files": details.numFiles,
                "size_in_bytes": details.sizeInBytes,
                "partition_columns": details.partitionColumns,
                "table_properties": stats.tblProperties,
                "recent_operations": [
                    {
                        "version": h.version,
                        "operation": h.operation,
                        "timestamp": h.timestamp,
                        "user_identity": getattr(h, 'userIdentity', None),
                        "operation_metrics": getattr(h, 'operationMetrics', {})
                    } for h in history
                ]
            }
            
            logger.info("Retrieved Delta table info",
                       table=table_name,
                       version=details.version,
                       size_mb=details.sizeInBytes / (1024 * 1024))
                       
            return info
            
        except Exception as e:
            logger.error("Failed to get Delta table info",
                        table=table_name,
                        error=str(e))
            return {}
            
    def optimize_table(self, table_name: str, **kwargs) -> bool:
        """
        Optimize Delta table (compaction, Z-ordering)
        
        Args:
            table_name: Name of the Delta table to optimize
            **kwargs: Optimization parameters (z_order_cols, max_file_size, etc.)
            
        Returns:
            True if successful
        """
        try:
            from delta.tables import DeltaTable
            
            # Get Delta table
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Basic compaction
            optimize_cmd = delta_table.optimize()
            
            # Z-ordering if columns specified
            z_order_cols = kwargs.get('z_order_cols', [])
            if z_order_cols:
                optimize_cmd = optimize_cmd.executeZOrderBy(*z_order_cols)
            else:
                optimize_cmd.executeCompaction()
                
            logger.info("Optimized Delta table",
                       table=table_name,
                       z_order_cols=z_order_cols)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to optimize Delta table",
                        table=table_name,
                        error=str(e))
            return False
            
    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> bool:
        """
        Vacuum Delta table to remove old files
        
        Args:
            table_name: Name of the Delta table
            retention_hours: Retention period in hours (default 7 days)
            
        Returns:
            True if successful
        """
        try:
            from delta.tables import DeltaTable
            
            # Get Delta table
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Vacuum old files
            delta_table.vacuum(retention_hours)
            
            logger.info("Vacuumed Delta table",
                       table=table_name,
                       retention_hours=retention_hours)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to vacuum Delta table",
                        table=table_name,
                        error=str(e))
            return False
            
    def validate_setup(self) -> Dict[str, bool]:
        """
        Validate Delta engine setup
        
        Returns:
            Dictionary with validation results
        """
        validation = super().validate_setup()
        
        try:
            # Check Delta extensions
            try:
                from delta.tables import DeltaTable
                validation["delta_library"] = True
            except ImportError:
                validation["delta_library"] = False
                
            # Check if Delta SQL extensions are loaded
            try:
                self.spark.sql("SELECT 1").collect()
                # Try a Delta-specific function
                self.spark.sql("SELECT delta_version()").collect()
                validation["delta_extensions"] = True
            except:
                validation["delta_extensions"] = False
                
            # Check storage connectivity
            try:
                test_path = f"{self.storage_path}/_test"
                test_df = self.spark.range(1)
                test_df.write.mode("overwrite").parquet(test_path)
                validation["storage_access"] = True
            except:
                validation["storage_access"] = False
                
        except Exception as e:
            logger.error("Delta validation failed", error=str(e))
            
        return validation
        
    def _merge_data_simple(self, table_name: str, source_df, merge_key: str) -> bool:
        """
        Simple merge operation using merge key
        """
        try:
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Simple merge - update if exists, insert if not
            delta_table.alias("target").merge(
                source_df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            return True
            
        except Exception as e:
            logger.error("Simple merge failed", error=str(e))
            return False
            
    def _parse_update_expressions(self, when_matched: str) -> Dict[str, str]:
        """Parse UPDATE SET expressions from SQL string"""
        # Simplified parser - in production, use proper SQL parsing
        if "SET" in when_matched.upper():
            set_part = when_matched.split("SET", 1)[1].strip()
            expressions = {}
            for expr in set_part.split(","):
                if "=" in expr:
                    key, value = expr.split("=", 1)
                    expressions[key.strip()] = value.strip()
            return expressions
        return {}
        
    def _parse_insert_expressions(self, when_not_matched: str) -> Dict[str, str]:
        """Parse INSERT expressions from SQL string"""
        # Simplified parser - in production, use proper SQL parsing
        if "VALUES" in when_not_matched.upper():
            values_part = when_not_matched.split("VALUES", 1)[1].strip()
            # This is a simplified implementation
            return {}
        return {}
