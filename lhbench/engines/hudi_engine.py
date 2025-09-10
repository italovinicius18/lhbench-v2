"""
Apache Hudi engine implementation
"""
import logging
from typing import Dict, List, Optional, Any
import structlog
from .base_engine import BaseLakehouseEngine

logger = structlog.get_logger(__name__)


class HudiEngine(BaseLakehouseEngine):
    """
    Apache Hudi engine implementation
    """
    
    def __init__(self, spark_session, config: Dict):
        """
        Initialize Hudi engine
        
        Args:
            spark_session: Active Spark session with Hudi configured
            config: Engine configuration
        """
        super().__init__(spark_session, config)
        self.storage_path = config.get('storage_path', 's3a://lakehouse/hudi')
        self.database_name = config.get('database_name', 'default')
        
    def create_table(self, table_name: str, data_path: str, schema: Optional[str] = None, 
                    partitions: Optional[List[str]] = None, **kwargs) -> bool:
        """
        Create a Hudi table from data
        
        Args:
            table_name: Name of the Hudi table to create
            data_path: Path to source data (Parquet files)
            schema: Optional table schema
            partitions: Optional partition columns
            **kwargs: Hudi-specific parameters
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            # Build table location
            table_location = kwargs.get('location', f"{self.storage_path}/{table_name}")
            
            # Get Hudi configuration
            hudi_options = self._get_hudi_options(table_name, "bulk_insert", **kwargs)
            hudi_options['hoodie.table.name'] = table_name
            hudi_options['hoodie.datasource.write.table.type'] = kwargs.get('table_type', 'COPY_ON_WRITE')
            hudi_options['hoodie.datasource.write.operation'] = 'bulk_insert'
            hudi_options['path'] = table_location
            
            # Set record key and precombine field
            record_key = kwargs.get('record_key', 'id')
            precombine_field = kwargs.get('precombine_field', 'ts')
            hudi_options['hoodie.datasource.write.recordkey.field'] = record_key
            hudi_options['hoodie.datasource.write.precombine.field'] = precombine_field
            
            # Set partitioning
            if partitions:
                hudi_options['hoodie.datasource.write.partitionpath.field'] = ','.join(partitions)
                
            # Create Hudi table writer
            writer = df.write.format("hudi").options(**hudi_options)
            
            # Write the table
            writer.mode("overwrite").save(table_location)
            
            # Register table in metastore
            self._register_hudi_table(table_name, table_location)
            
            logger.info("Created Hudi table",
                       table=table_name,
                       location=table_location,
                       table_type=hudi_options['hoodie.datasource.write.table.type'],
                       partitions=partitions,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to create Hudi table",
                        table=table_name,
                        error=str(e))
            return False
            
    def load_data(self, table_name: str, data_path: str, mode: str = "append", **kwargs) -> bool:
        """
        Load data into a Hudi table
        
        Args:
            table_name: Target Hudi table name
            data_path: Path to source data
            mode: Load mode (append, overwrite, upsert)
            **kwargs: Hudi-specific parameters
            
        Returns:
            True if successful
        """
        try:
            # Read source data
            df = self.spark.read.parquet(data_path)
            
            # Get table location
            table_location = f"{self.storage_path}/{table_name}"
            
            # Map mode to Hudi operation
            operation_map = {
                "append": "insert",
                "overwrite": "bulk_insert", 
                "upsert": "upsert"
            }
            hudi_operation = operation_map.get(mode, "upsert")
            
            # Get Hudi configuration
            hudi_options = self._get_hudi_options(table_name, hudi_operation, **kwargs)
            hudi_options['hoodie.datasource.write.operation'] = hudi_operation
            hudi_options['path'] = table_location
            
            # Create writer
            writer = df.write.format("hudi").options(**hudi_options)
            
            # Execute write
            if mode == "overwrite":
                writer.mode("overwrite").save(table_location)
            else:
                writer.mode("append").save(table_location)
                
            logger.info("Loaded data into Hudi table",
                       table=table_name,
                       mode=mode,
                       operation=hudi_operation,
                       rows=df.count())
                       
            return True
            
        except Exception as e:
            logger.error("Failed to load data into Hudi table",
                        table=table_name,
                        mode=mode,
                        error=str(e))
            return False
            
    def run_query(self, sql: str, collect_results: bool = False) -> Optional[Any]:
        """
        Execute SQL query on Hudi tables
        
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
                logger.info("Executed Hudi query with results",
                           query_preview=sql[:100],
                           result_count=len(results))
                return results
            else:
                # For write operations, just execute
                result_df.count()  # Force execution
                logger.info("Executed Hudi query",
                           query_preview=sql[:100])
                return None
                
        except Exception as e:
            logger.error("Failed to execute Hudi query",
                        query_preview=sql[:100],
                        error=str(e))
            return None
            
    def merge_data(self, target_table: str, source_data: str, 
                  merge_condition: str, when_matched: str, when_not_matched: str) -> bool:
        """
        Perform Hudi merge operation (upsert)
        
        Args:
            target_table: Target Hudi table name
            source_data: Source data path or table
            merge_condition: Condition for matching records (not used in Hudi, uses record key)
            when_matched: Action when records match (not used, Hudi handles automatically)
            when_not_matched: Action when records don't match (not used, Hudi handles automatically)
            
        Returns:
            True if successful
        """
        try:
            # Note: Hudi handles merge logic automatically based on record key
            # The merge_condition and when_* parameters are not used but kept for interface compatibility
            
            # Read source data
            if source_data.endswith('.parquet') or 's3a://' in source_data:
                source_df = self.spark.read.parquet(source_data)
            else:
                source_df = self.spark.table(source_data)
                
            # Perform upsert operation
            table_location = f"{self.storage_path}/{target_table}"
            hudi_options = self._get_hudi_options(target_table, "upsert")
            hudi_options['hoodie.datasource.write.operation'] = 'upsert'
            hudi_options['path'] = table_location
            
            # Execute upsert
            source_df.write.format("hudi").options(**hudi_options).mode("append").save(table_location)
            
            logger.info("Executed Hudi merge operation (upsert)",
                       target_table=target_table,
                       source=source_data)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to execute Hudi merge",
                        target_table=target_table,
                        source=source_data,
                        error=str(e))
            return False
            
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get Hudi table metadata and statistics
        
        Args:
            table_name: Name of the Hudi table
            
        Returns:
            Dictionary with table information
        """
        try:
            table_location = f"{self.storage_path}/{table_name}"
            
            # Read Hudi table to get metadata
            hudi_df = self.spark.read.format("hudi").load(table_location)
            
            # Get table schema
            schema_info = hudi_df.schema.jsonValue()
            
            # Get commit timeline info
            commits_df = self.spark.read.format("hudi").load(f"{table_location}/.hoodie/timeline/*.commit")
            commits = []
            try:
                commit_files = commits_df.collect()
                commits = [{"commit": c.commit} for c in commit_files[:10]]
            except:
                pass
                
            # Get partition info
            partitions = []
            try:
                partitions_df = self.spark.sql(f"SHOW PARTITIONS {table_name}")
                partitions = [row.partition for row in partitions_df.collect()]
            except:
                pass
                
            # Get basic statistics
            try:
                row_count = hudi_df.count()
            except:
                row_count = 0
                
            info = {
                "table_name": table_name,
                "format": "hudi",
                "location": table_location,
                "schema": schema_info,
                "row_count": row_count,
                "partitions": partitions[:10],  # Limit to first 10
                "recent_commits": commits[:10],  # Limit to recent 10
                "hudi_columns": [
                    col for col in hudi_df.columns 
                    if col.startswith('_hoodie_')
                ]
            }
            
            logger.info("Retrieved Hudi table info",
                       table=table_name,
                       row_count=row_count,
                       partitions_count=len(partitions))
                       
            return info
            
        except Exception as e:
            logger.error("Failed to get Hudi table info",
                        table=table_name,
                        error=str(e))
            return {}
            
    def optimize_table(self, table_name: str, **kwargs) -> bool:
        """
        Optimize Hudi table (compaction, clustering)
        
        Args:
            table_name: Name of the Hudi table to optimize
            **kwargs: Optimization parameters (operation_type, etc.)
            
        Returns:
            True if successful
        """
        try:
            table_location = f"{self.storage_path}/{table_name}"
            operation_type = kwargs.get('operation_type', 'compaction')
            
            if operation_type == 'compaction':
                # Trigger compaction
                hudi_options = self._get_hudi_options(table_name, "compaction")
                hudi_options['hoodie.compact.inline'] = 'true'
                hudi_options['hoodie.compact.inline.max.delta.commits'] = '1'
                hudi_options['path'] = table_location
                
                # Read and write to trigger compaction
                hudi_df = self.spark.read.format("hudi").load(table_location)
                hudi_df.write.format("hudi").options(**hudi_options).mode("append").save(table_location)
                
            elif operation_type == 'clustering':
                # Trigger clustering (for newer Hudi versions)
                hudi_options = self._get_hudi_options(table_name, "clustering")
                hudi_options['hoodie.clustering.inline'] = 'true'
                hudi_options['path'] = table_location
                
                # Read and write to trigger clustering
                hudi_df = self.spark.read.format("hudi").load(table_location)
                hudi_df.write.format("hudi").options(**hudi_options).mode("append").save(table_location)
                
            logger.info("Optimized Hudi table",
                       table=table_name,
                       operation=operation_type)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to optimize Hudi table",
                        table=table_name,
                        operation_type=operation_type,
                        error=str(e))
            return False
            
    def time_travel_query(self, table_name: str, commit_time: str) -> Optional[Any]:
        """
        Query Hudi table at a specific commit time
        
        Args:
            table_name: Name of the Hudi table
            commit_time: Commit timestamp to query
            
        Returns:
            DataFrame for the historical data
        """
        try:
            table_location = f"{self.storage_path}/{table_name}"
            
            # Read Hudi table as of specific time
            hudi_options = {
                'hoodie.datasource.query.type': 'incremental',
                'hoodie.datasource.read.begin.instanttime': commit_time
            }
            
            result_df = self.spark.read.format("hudi").options(**hudi_options).load(table_location)
            
            logger.info("Executed Hudi time travel query",
                       table=table_name,
                       commit_time=commit_time)
                       
            return result_df
            
        except Exception as e:
            logger.error("Failed to execute Hudi time travel query",
                        table=table_name,
                        commit_time=commit_time,
                        error=str(e))
            return None
            
    def incremental_query(self, table_name: str, begin_instant: str, end_instant: Optional[str] = None) -> Optional[Any]:
        """
        Query incremental changes in Hudi table
        
        Args:
            table_name: Name of the Hudi table
            begin_instant: Start instant time
            end_instant: End instant time (optional)
            
        Returns:
            DataFrame with incremental changes
        """
        try:
            table_location = f"{self.storage_path}/{table_name}"
            
            hudi_options = {
                'hoodie.datasource.query.type': 'incremental',
                'hoodie.datasource.read.begin.instanttime': begin_instant
            }
            
            if end_instant:
                hudi_options['hoodie.datasource.read.end.instanttime'] = end_instant
                
            result_df = self.spark.read.format("hudi").options(**hudi_options).load(table_location)
            
            logger.info("Executed Hudi incremental query",
                       table=table_name,
                       begin_instant=begin_instant,
                       end_instant=end_instant)
                       
            return result_df
            
        except Exception as e:
            logger.error("Failed to execute Hudi incremental query",
                        table=table_name,
                        error=str(e))
            return None
            
    def validate_setup(self) -> Dict[str, bool]:
        """
        Validate Hudi engine setup
        
        Returns:
            Dictionary with validation results
        """
        validation = super().validate_setup()
        
        try:
            # Check Hudi format support
            try:
                test_df = self.spark.range(1).withColumn("ts", self.spark.sql("SELECT current_timestamp()").collect()[0][0])
                test_path = f"{self.storage_path}/_test_hudi"
                
                hudi_options = {
                    'hoodie.table.name': '_test_hudi',
                    'hoodie.datasource.write.recordkey.field': 'id',
                    'hoodie.datasource.write.precombine.field': 'ts',
                    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE'
                }
                
                test_df.write.format("hudi").options(**hudi_options).mode("overwrite").save(test_path)
                validation["hudi_format"] = True
            except:
                validation["hudi_format"] = False
                
            # Check storage path access
            try:
                test_path = f"{self.storage_path}/_test"
                test_df = self.spark.range(1)
                test_df.write.mode("overwrite").parquet(test_path)
                validation["storage_access"] = True
            except:
                validation["storage_access"] = False
                
        except Exception as e:
            logger.error("Hudi validation failed", error=str(e))
            
        return validation
        
    def _get_hudi_options(self, table_name: str, operation: str, **kwargs) -> Dict[str, str]:
        """
        Get default Hudi options for operations
        
        Args:
            table_name: Name of the table
            operation: Hudi operation type
            **kwargs: Additional options
            
        Returns:
            Dictionary of Hudi options
        """
        base_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': kwargs.get('record_key', 'id'),
            'hoodie.datasource.write.precombine.field': kwargs.get('precombine_field', 'ts'),
            'hoodie.datasource.write.table.type': kwargs.get('table_type', 'COPY_ON_WRITE'),
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.hive_style_partitioning': 'true'
        }
        
        # Add partition path if specified
        partitions = kwargs.get('partitions', [])
        if partitions:
            base_options['hoodie.datasource.write.partitionpath.field'] = ','.join(partitions)
        else:
            base_options['hoodie.datasource.write.keygenerator.class'] = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
            
        # Operation-specific options
        if operation == 'compaction':
            base_options.update({
                'hoodie.compact.inline': 'true',
                'hoodie.compact.inline.max.delta.commits': '5'
            })
        elif operation == 'clustering':
            base_options.update({
                'hoodie.clustering.inline': 'true',
                'hoodie.clustering.inline.max.commits': '10'
            })
            
        # Merge with any additional options
        base_options.update(kwargs.get('hudi_options', {}))
        
        return base_options
        
    def _register_hudi_table(self, table_name: str, table_location: str) -> bool:
        """
        Register Hudi table in metastore
        
        Args:
            table_name: Name of the table
            table_location: Location of the table
            
        Returns:
            True if successful
        """
        try:
            # Create table registration SQL
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.database_name}.{table_name}
            USING HUDI
            LOCATION '{table_location}'
            """
            
            self.spark.sql(create_sql)
            return True
            
        except Exception as e:
            logger.warning("Failed to register Hudi table in metastore",
                          table=table_name,
                          error=str(e))
            return False
