"""
Base class for lakehouse storage engines
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import structlog

logger = structlog.get_logger(__name__)


class BaseLakehouseEngine(ABC):
    """
    Abstract base class for lakehouse storage engines
    """
    
    def __init__(self, spark_session, config: Dict):
        """
        Initialize base engine
        
        Args:
            spark_session: Active Spark session
            config: Engine configuration
        """
        self.spark = spark_session
        self.config = config
        self.engine_name = self.__class__.__name__.lower().replace('engine', '')
        
    @abstractmethod
    def create_table(self, table_name: str, data_path: str, schema: Optional[str] = None, 
                    partitions: Optional[List[str]] = None, **kwargs) -> bool:
        """
        Create a table from data
        
        Args:
            table_name: Name of the table to create
            data_path: Path to source data
            schema: Optional table schema
            partitions: Optional partition columns
            **kwargs: Engine-specific parameters
            
        Returns:
            True if successful
        """
        pass
        
    @abstractmethod
    def load_data(self, table_name: str, data_path: str, mode: str = "append", **kwargs) -> bool:
        """
        Load data into a table
        
        Args:
            table_name: Target table name
            data_path: Path to source data
            mode: Load mode (append, overwrite, etc.)
            **kwargs: Engine-specific parameters
            
        Returns:
            True if successful
        """
        pass
        
    @abstractmethod
    def run_query(self, sql: str, collect_results: bool = False) -> Optional[Any]:
        """
        Execute SQL query
        
        Args:
            sql: SQL query string
            collect_results: Whether to collect and return results
            
        Returns:
            Query results if collect_results=True, None otherwise
        """
        pass
        
    @abstractmethod
    def merge_data(self, target_table: str, source_data: str, 
                  merge_condition: str, when_matched: str, when_not_matched: str) -> bool:
        """
        Perform merge/upsert operation
        
        Args:
            target_table: Target table name
            source_data: Source data path or table
            merge_condition: Condition for matching records
            when_matched: Action when records match
            when_not_matched: Action when records don't match
            
        Returns:
            True if successful
        """
        pass
        
    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get table metadata and statistics
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        pass
        
    @abstractmethod
    def optimize_table(self, table_name: str, **kwargs) -> bool:
        """
        Optimize table (compaction, etc.)
        
        Args:
            table_name: Name of the table to optimize
            **kwargs: Engine-specific optimization parameters
            
        Returns:
            True if successful
        """
        pass
        
    def drop_table(self, table_name: str, purge: bool = True) -> bool:
        """
        Drop a table
        
        Args:
            table_name: Name of the table to drop
            purge: Whether to purge table data
            
        Returns:
            True if successful
        """
        try:
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            if purge:
                drop_sql += " PURGE"
                
            self.spark.sql(drop_sql)
            logger.info("Dropped table", engine=self.engine_name, table=table_name)
            return True
            
        except Exception as e:
            logger.error("Failed to drop table",
                        engine=self.engine_name,
                        table=table_name,
                        error=str(e))
            return False
            
    def list_tables(self, database: str = "default") -> List[str]:
        """
        List tables in database
        
        Args:
            database: Database name
            
        Returns:
            List of table names
        """
        try:
            tables_df = self.spark.sql(f"SHOW TABLES IN {database}")
            tables = [row.tableName for row in tables_df.collect()]
            
            logger.info("Listed tables",
                       engine=self.engine_name,
                       database=database,
                       count=len(tables))
                       
            return tables
            
        except Exception as e:
            logger.error("Failed to list tables",
                        engine=self.engine_name,
                        database=database,
                        error=str(e))
            return []
            
    def get_engine_info(self) -> Dict:
        """
        Get engine-specific information
        
        Returns:
            Dictionary with engine information
        """
        return {
            "engine_name": self.engine_name,
            "spark_version": self.spark.version,
            "config": self.config
        }
        
    def validate_setup(self) -> Dict[str, bool]:
        """
        Validate that engine is properly configured
        
        Returns:
            Dictionary with validation results
        """
        validation = {
            "spark_session": False,
            "extensions_loaded": False,
            "catalogs_configured": False
        }
        
        try:
            # Check Spark session
            if self.spark:
                validation["spark_session"] = True
                
            # Check SQL extensions (engine-specific implementations should override)
            validation["extensions_loaded"] = True
            validation["catalogs_configured"] = True
            
        except Exception as e:
            logger.error("Engine validation failed",
                        engine=self.engine_name,
                        error=str(e))
                        
        return validation