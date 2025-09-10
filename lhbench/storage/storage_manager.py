"""
Storage management for benchmark data and results
"""
import logging
from pathlib import Path
from typing import Dict, Optional
import structlog

from .minio_client import MinIOClient

logger = structlog.get_logger(__name__)


class StorageManager:
    """
    Manages storage operations for benchmark data and results
    """
    
    def __init__(self, config: Dict):
        """
        Initialize storage manager
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.local_data_path = Path(config.get("local_data_path", "/tmp/lhbench-data"))
        self.warehouse_path = Path(config.get("warehouse_path", "/mnt/c/Users/italo/WSL_DATA"))
        
        # Initialize MinIO client
        minio_config = config.get("minio", {})
        self.minio_client = MinIOClient(minio_config)
        
        # Bucket names
        self.buckets = {
            "lakehouse": "lakehouse",          # Main lakehouse data storage
            "benchmark_data": "benchmark-data", # Generated benchmark data
            "results": "benchmark-results"      # Benchmark results
        }
        
        # Ensure local directories exist
        self.local_data_path.mkdir(parents=True, exist_ok=True)
        self.warehouse_path.mkdir(parents=True, exist_ok=True)
        
    def initialize_storage(self) -> bool:
        """
        Initialize storage infrastructure (buckets, directories)
        
        Returns:
            True if successful
        """
        logger.info("Initializing storage infrastructure")
        
        try:
            # Create MinIO buckets
            for bucket_name in self.buckets.values():
                if not self.minio_client.create_bucket(bucket_name):
                    logger.error("Failed to create bucket", bucket=bucket_name)
                    return False
                    
            # Verify health
            health = self.minio_client.health_check()
            if not all(health.values()):
                logger.error("Storage health check failed", health=health)
                return False
                
            logger.info("Storage infrastructure initialized successfully")
            return True
            
        except Exception as e:
            logger.error("Failed to initialize storage", error=str(e))
            return False
            
    def upload_benchmark_data(self, local_data_dir: str, dataset_name: str) -> bool:
        """
        Upload benchmark dataset to MinIO
        
        Args:
            local_data_dir: Local directory containing data files
            dataset_name: Name/identifier for the dataset
            
        Returns:
            True if successful
        """
        logger.info("Uploading benchmark data", 
                   local_dir=local_data_dir, 
                   dataset=dataset_name)
        
        s3_prefix = f"datasets/{dataset_name}"
        bucket_name = self.buckets["benchmark_data"]
        
        success = self.minio_client.upload_directory(
            local_data_dir, 
            bucket_name, 
            s3_prefix
        )
        
        if success:
            # Get uploaded data info
            size_info = self.minio_client.get_bucket_size(bucket_name, s3_prefix)
            logger.info("Benchmark data uploaded successfully",
                       dataset=dataset_name,
                       **size_info)
                       
        return success
        
    def create_lakehouse_table_path(self, engine: str, table_name: str, format_version: str = "v1") -> str:
        """
        Create S3 path for lakehouse table storage
        
        Args:
            engine: Lakehouse engine (delta, iceberg, hudi)
            table_name: Name of the table
            format_version: Format version (for engines that support it)
            
        Returns:
            S3 path for the table
        """
        bucket = self.buckets["lakehouse"]
        path = f"s3a://{bucket}/{engine}/{format_version}/{table_name}"
        
        logger.debug("Created lakehouse table path", 
                    engine=engine, 
                    table=table_name, 
                    path=path)
                    
        return path
        
    def create_warehouse_directory(self, engine: str) -> str:
        """
        Create local warehouse directory for an engine
        
        Args:
            engine: Lakehouse engine name
            
        Returns:
            Local warehouse directory path
        """
        warehouse_dir = self.warehouse_path / "warehouse" / engine
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Created warehouse directory", 
                   engine=engine, 
                   path=str(warehouse_dir))
                   
        return str(warehouse_dir)
        
    def store_benchmark_results(self, results_data: Dict, benchmark_name: str, timestamp: str) -> bool:
        """
        Store benchmark results to MinIO
        
        Args:
            results_data: Results dictionary
            benchmark_name: Name of the benchmark
            timestamp: Timestamp for the results
            
        Returns:
            True if successful
        """
        logger.info("Storing benchmark results", 
                   benchmark=benchmark_name, 
                   timestamp=timestamp)
        
        try:
            import json
            
            # Convert results to JSON
            results_json = json.dumps(results_data, indent=2, default=str)
            
            # Create S3 key
            s3_key = f"{benchmark_name}/{timestamp}/results.json"
            bucket_name = self.buckets["results"]
            
            # Upload results
            self.minio_client.s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=results_json.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info("Benchmark results stored successfully",
                       benchmark=benchmark_name,
                       s3_key=s3_key)
                       
            return True
            
        except Exception as e:
            logger.error("Failed to store benchmark results",
                        benchmark=benchmark_name,
                        error=str(e))
            return False
            
    def get_benchmark_results(self, benchmark_name: str, timestamp: Optional[str] = None) -> Optional[Dict]:
        """
        Retrieve benchmark results from MinIO
        
        Args:
            benchmark_name: Name of the benchmark
            timestamp: Specific timestamp (if None, gets latest)
            
        Returns:
            Results dictionary or None if not found
        """
        try:
            bucket_name = self.buckets["results"]
            prefix = f"{benchmark_name}/"
            
            if timestamp:
                s3_key = f"{benchmark_name}/{timestamp}/results.json"
            else:
                # Get latest results
                objects = self.minio_client.list_objects(bucket_name, prefix)
                if not objects:
                    logger.info("No benchmark results found", benchmark=benchmark_name)
                    return None
                    
                # Find latest timestamp
                latest_obj = max(objects, key=lambda x: x['last_modified'])
                s3_key = latest_obj['key']
                
            # Download results
            response = self.minio_client.s3_client.get_object(
                Bucket=bucket_name,
                Key=s3_key
            )
            
            import json
            results_data = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info("Retrieved benchmark results",
                       benchmark=benchmark_name,
                       s3_key=s3_key)
                       
            return results_data
            
        except Exception as e:
            logger.error("Failed to retrieve benchmark results",
                        benchmark=benchmark_name,
                        error=str(e))
            return None
            
    def cleanup_old_data(self, days_to_keep: int = 7) -> Dict[str, int]:
        """
        Clean up old benchmark data and results
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup statistics
        """
        logger.info("Cleaning up old data", days_to_keep=days_to_keep)
        
        from datetime import datetime, timedelta
        
        cleanup_stats = {
            "benchmark_data_deleted": 0,
            "results_deleted": 0,
            "lakehouse_data_deleted": 0
        }
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        try:
            # Clean up results
            bucket_name = self.buckets["results"]
            objects = self.minio_client.list_objects(bucket_name)
            
            for obj in objects:
                if obj['last_modified'] < cutoff_date:
                    if self.minio_client.delete_object(bucket_name, obj['key']):
                        cleanup_stats["results_deleted"] += 1
                        
            logger.info("Data cleanup completed", **cleanup_stats)
            return cleanup_stats
            
        except Exception as e:
            logger.error("Failed to cleanup old data", error=str(e))
            return cleanup_stats
            
    def get_storage_summary(self) -> Dict:
        """
        Get summary of storage usage
        
        Returns:
            Dictionary with storage statistics
        """
        logger.info("Getting storage summary")
        
        summary = {
            "buckets": {},
            "total_size_gb": 0,
            "total_objects": 0
        }
        
        try:
            for bucket_type, bucket_name in self.buckets.items():
                size_info = self.minio_client.get_bucket_size(bucket_name)
                summary["buckets"][bucket_type] = size_info
                summary["total_size_gb"] += size_info["total_size_gb"]
                summary["total_objects"] += size_info["object_count"]
                
            # Add local storage info
            if self.warehouse_path.exists():
                import shutil
                local_usage = shutil.disk_usage(self.warehouse_path)
                summary["local_warehouse"] = {
                    "free_gb": local_usage.free / (1024**3),
                    "used_gb": local_usage.used / (1024**3),
                    "total_gb": local_usage.total / (1024**3)
                }
                
        except Exception as e:
            logger.error("Failed to get storage summary", error=str(e))
            
        return summary