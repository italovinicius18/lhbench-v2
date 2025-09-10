"""
Factory for creating lakehouse engines
"""
import logging
from typing import Dict, Optional, Union
import structlog
from .base_engine import BaseLakehouseEngine
from .delta_engine import DeltaEngine
from .iceberg_engine import IcebergEngine
from .hudi_engine import HudiEngine

logger = structlog.get_logger(__name__)


class EngineFactory:
    """
    Factory class for creating lakehouse engines
    """
    
    _engines = {
        'delta': DeltaEngine,
        'iceberg': IcebergEngine,
        'hudi': HudiEngine
    }
    
    @classmethod
    def create_engine(cls, engine_type: str, spark_session, config: Dict) -> Optional[BaseLakehouseEngine]:
        """
        Create a lakehouse engine instance
        
        Args:
            engine_type: Type of engine ('delta', 'iceberg', 'hudi')
            spark_session: Active Spark session
            config: Engine configuration
            
        Returns:
            Engine instance or None if type not supported
        """
        try:
            engine_class = cls._engines.get(engine_type.lower())
            if not engine_class:
                available_engines = list(cls._engines.keys())
                logger.error("Unsupported engine type",
                           engine_type=engine_type,
                           available_engines=available_engines)
                return None
                
            engine = engine_class(spark_session, config)
            
            logger.info("Created lakehouse engine",
                       engine_type=engine_type,
                       engine_class=engine_class.__name__)
                       
            return engine
            
        except Exception as e:
            logger.error("Failed to create engine",
                        engine_type=engine_type,
                        error=str(e))
            return None
            
    @classmethod
    def get_available_engines(cls) -> list:
        """
        Get list of available engine types
        
        Returns:
            List of available engine type names
        """
        return list(cls._engines.keys())
        
    @classmethod
    def validate_engine_type(cls, engine_type: str) -> bool:
        """
        Validate if engine type is supported
        
        Args:
            engine_type: Engine type to validate
            
        Returns:
            True if supported, False otherwise
        """
        return engine_type.lower() in cls._engines


class MultiEngineManager:
    """
    Manager class for working with multiple lakehouse engines
    """
    
    def __init__(self, spark_session, engines_config: Dict[str, Dict]):
        """
        Initialize multi-engine manager
        
        Args:
            spark_session: Active Spark session
            engines_config: Configuration for each engine type
        """
        self.spark = spark_session
        self.engines_config = engines_config
        self.engines = {}
        self._initialize_engines()
        
    def _initialize_engines(self):
        """Initialize all configured engines"""
        for engine_type, config in self.engines_config.items():
            if config.get('enabled', False):
                engine = EngineFactory.create_engine(engine_type, self.spark, config)
                if engine:
                    self.engines[engine_type] = engine
                    logger.info("Initialized engine", engine_type=engine_type)
                else:
                    logger.warning("Failed to initialize engine", engine_type=engine_type)
                    
    def get_engine(self, engine_type: str) -> Optional[BaseLakehouseEngine]:
        """
        Get engine instance by type
        
        Args:
            engine_type: Type of engine to get
            
        Returns:
            Engine instance or None if not available
        """
        return self.engines.get(engine_type.lower())
        
    def get_available_engines(self) -> list:
        """
        Get list of available (initialized) engines
        
        Returns:
            List of available engine names
        """
        return list(self.engines.keys())
        
    def validate_all_engines(self) -> Dict[str, Dict[str, bool]]:
        """
        Validate setup for all engines
        
        Returns:
            Dictionary with validation results for each engine
        """
        validation_results = {}
        
        for engine_type, engine in self.engines.items():
            validation_results[engine_type] = engine.validate_setup()
            
        return validation_results
        
    def create_table_all_engines(self, table_name: str, data_path: str, 
                                 partitions: Optional[list] = None, 
                                 engine_configs: Optional[Dict[str, Dict]] = None) -> Dict[str, bool]:
        """
        Create table in all engines
        
        Args:
            table_name: Name of the table to create
            data_path: Path to source data
            partitions: Optional partition columns
            engine_configs: Optional engine-specific configurations
            
        Returns:
            Dictionary with success status for each engine
        """
        results = {}
        engine_configs = engine_configs or {}
        
        for engine_type, engine in self.engines.items():
            try:
                config = engine_configs.get(engine_type, {})
                success = engine.create_table(
                    table_name=table_name,
                    data_path=data_path,
                    partitions=partitions,
                    **config
                )
                results[engine_type] = success
                
                logger.info("Created table in engine",
                           engine_type=engine_type,
                           table_name=table_name,
                           success=success)
                           
            except Exception as e:
                logger.error("Failed to create table in engine",
                           engine_type=engine_type,
                           table_name=table_name,
                           error=str(e))
                results[engine_type] = False
                
        return results
        
    def run_query_all_engines(self, sql: str, collect_results: bool = False) -> Dict[str, any]:
        """
        Run query on all engines
        
        Args:
            sql: SQL query to execute
            collect_results: Whether to collect results
            
        Returns:
            Dictionary with results from each engine
        """
        results = {}
        
        for engine_type, engine in self.engines.items():
            try:
                result = engine.run_query(sql, collect_results=collect_results)
                results[engine_type] = result
                
                logger.info("Executed query in engine",
                           engine_type=engine_type,
                           query_preview=sql[:50])
                           
            except Exception as e:
                logger.error("Failed to execute query in engine",
                           engine_type=engine_type,
                           query_preview=sql[:50],
                           error=str(e))
                results[engine_type] = None
                
        return results
        
    def get_table_info_all_engines(self, table_name: str) -> Dict[str, Dict]:
        """
        Get table information from all engines
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table info from each engine
        """
        results = {}
        
        for engine_type, engine in self.engines.items():
            try:
                info = engine.get_table_info(table_name)
                results[engine_type] = info
                
                logger.info("Retrieved table info from engine",
                           engine_type=engine_type,
                           table_name=table_name)
                           
            except Exception as e:
                logger.error("Failed to get table info from engine",
                           engine_type=engine_type,
                           table_name=table_name,
                           error=str(e))
                results[engine_type] = {}
                
        return results
        
    def compare_query_performance(self, sql: str, iterations: int = 3) -> Dict[str, Dict]:
        """
        Compare query performance across engines
        
        Args:
            sql: SQL query to benchmark
            iterations: Number of iterations to run
            
        Returns:
            Dictionary with performance metrics for each engine
        """
        import time
        
        results = {}
        
        for engine_type, engine in self.engines.items():
            times = []
            
            for i in range(iterations):
                try:
                    start_time = time.time()
                    engine.run_query(sql, collect_results=False)
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    times.append(execution_time)
                    
                except Exception as e:
                    logger.error("Query failed during performance test",
                               engine_type=engine_type,
                               iteration=i,
                               error=str(e))
                    times.append(None)
                    
            # Calculate metrics
            valid_times = [t for t in times if t is not None]
            if valid_times:
                results[engine_type] = {
                    'avg_time': sum(valid_times) / len(valid_times),
                    'min_time': min(valid_times),
                    'max_time': max(valid_times),
                    'successful_runs': len(valid_times),
                    'total_runs': iterations,
                    'all_times': times
                }
            else:
                results[engine_type] = {
                    'avg_time': None,
                    'min_time': None,
                    'max_time': None,
                    'successful_runs': 0,
                    'total_runs': iterations,
                    'all_times': times
                }
                
            logger.info("Performance test completed for engine",
                       engine_type=engine_type,
                       successful_runs=results[engine_type]['successful_runs'],
                       avg_time=results[engine_type]['avg_time'])
                       
        return results
        
    def close_all_engines(self):
        """Close all engine connections"""
        for engine_type, engine in self.engines.items():
            try:
                # Engines don't have explicit close methods in our implementation
                # But this could be extended for cleanup operations
                logger.info("Closed engine", engine_type=engine_type)
            except Exception as e:
                logger.error("Failed to close engine",
                           engine_type=engine_type,
                           error=str(e))
