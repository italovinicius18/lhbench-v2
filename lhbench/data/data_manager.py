"""
Data management utilities for the benchmark
"""
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Optional
import structlog

from .tpcds_generator import TPCDSGenerator

logger = structlog.get_logger(__name__)


class DataManager:
    """
    Manages data generation, storage, and lifecycle for benchmark tests
    """
    
    def __init__(self, config: Dict):
        """
        Initialize data manager with configuration
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.scale_factors = config.get("scale_factors", [1.0])
        
        # S3/MinIO configuration for direct write (now the only mode)
        self.s3_config = config.get("s3_config", {
            'endpoint_url': 'http://localhost:9000',
            'access_key': 'minioadmin',
            'secret_key': 'minioadmin',
            'bucket': 'lakehouse'
        })
        
    def prepare_tpcds_data(self, scale_factor: float = 1.0, force_regenerate: bool = False) -> Dict[str, str]:
        """
        Prepare TPC-DS data for benchmark tests (direct S3/MinIO generation)
        
        Args:
            scale_factor: TPC-DS scale factor
            force_regenerate: Force data regeneration even if exists
            
        Returns:
            Dictionary mapping table names to S3 file paths
        """
        bucket = self.s3_config.get('bucket', 'lakehouse')
        s3_path = f"s3://{bucket}/tpcds-data/sf_{scale_factor}"
        
        logger.info("Generating TPC-DS data directly to S3/MinIO", 
                   scale_factor=scale_factor, 
                   s3_path=s3_path)
        
        # TODO: Check if data already exists in S3 (if not force_regenerate)
        
        with TPCDSGenerator(s3_path, s3_config=self.s3_config) as generator:
            # Generate actual data directly to S3
            data_files = generator.generate_data(scale_factor=scale_factor, format="parquet")
            
            logger.info("Data generation complete (direct S3)", 
                       scale_factor=scale_factor,
                       tables=len(data_files),
                       s3_path=s3_path)
                       
        return data_files
        
    def prepare_refresh_data(self, base_scale_factor: float = 1.0, refresh_percentage: float = 3.0) -> Dict[str, str]:
        """
        Prepare refresh data for TPC-DS refresh tests (direct S3/MinIO generation)
        
        Args:
            base_scale_factor: Base dataset scale factor
            refresh_percentage: Percentage of data to use for refresh
            
        Returns:
            Dictionary mapping table names to S3 refresh file paths
        """
        refresh_scale = base_scale_factor * (refresh_percentage / 100.0)
        bucket = self.s3_config.get('bucket', 'lakehouse')
        s3_path = f"s3://{bucket}/tpcds-refresh/sf_{refresh_scale}"
        
        logger.info("Preparing refresh data (direct S3)", 
                   base_sf=base_scale_factor, 
                   refresh_pct=refresh_percentage,
                   refresh_sf=refresh_scale,
                   s3_path=s3_path)
        
        with TPCDSGenerator(s3_path, s3_config=self.s3_config) as generator:
            data_files = generator.generate_data(scale_factor=refresh_scale, format="parquet")
            
        logger.info("Refresh data prepared (direct S3)", files=len(data_files))
        return data_files
        
    def prepare_micro_merge_data(self, data_size_gb: float = 100.0, num_columns: int = 4) -> str:
        """
        Prepare synthetic data for micro merge tests
        
        Args:
            data_size_gb: Size of dataset in GB
            num_columns: Number of columns in the dataset
            
        Returns:
            Path to the generated data file
        """
        micro_dir = self.data_path / f"micro_merge_{data_size_gb}gb"
        micro_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = micro_dir / "micro_merge_data.parquet"
        
        if output_file.exists():
            logger.info("Using existing micro merge data", file=str(output_file))
            return str(output_file)
            
        logger.info("Generating micro merge data", size_gb=data_size_gb, columns=num_columns)
        
        # Use DuckDB to generate synthetic data
        import duckdb
        
        with duckdb.connect() as conn:
            # Calculate approximate rows needed for target size
            # Assuming roughly 40 bytes per row (4 columns * 10 bytes avg)
            target_rows = int((data_size_gb * 1024 * 1024 * 1024) / 40)
            
            # Generate synthetic table
            sql = f"""
            CREATE TABLE micro_merge_data AS 
            SELECT 
                row_number() OVER () as id,
                random() as col1,
                random() * 1000000 as col2,
                'value_' || (random() * 10000)::int as col3
            FROM range({target_rows});
            """
            
            if num_columns > 4:
                # Add additional columns if requested
                extra_cols = [f"random() * 100 as col{i}" for i in range(4, num_columns)]
                sql = sql.replace("'value_' || (random() * 10000)::int as col3",
                                f"'value_' || (random() * 10000)::int as col3,\n                " +
                                ",\n                ".join(extra_cols))
            
            conn.execute(sql)
            conn.execute(f"COPY micro_merge_data TO '{output_file}' (FORMAT PARQUET);")
            
        logger.info("Micro merge data generated", file=str(output_file))
        return str(output_file)
        
    def cleanup_data(self, keep_latest: int = 2):
        """
        Clean up old data files, keeping only the latest versions
        
        Args:
            keep_latest: Number of latest versions to keep for each dataset
        """
        logger.info("Cleaning up old data files", keep_latest=keep_latest)
        
        # Group directories by type
        tpcds_dirs = list(self.data_path.glob("tpcds_sf_*"))
        refresh_dirs = list(self.data_path.glob("tpcds_refresh_sf_*"))
        micro_dirs = list(self.data_path.glob("micro_merge_*"))
        
        for dir_group in [tpcds_dirs, refresh_dirs, micro_dirs]:
            if len(dir_group) > keep_latest:
                # Sort by modification time
                dir_group.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                
                # Remove older directories
                for old_dir in dir_group[keep_latest:]:
                    logger.info("Removing old data directory", path=str(old_dir))
                    shutil.rmtree(old_dir)
                    
    def get_data_summary(self) -> Dict:
        """
        Get summary of available data
        
        Returns:
            Dictionary with data availability summary
        """
        summary = {
            "tpcds_datasets": [],
            "refresh_datasets": [],
            "micro_merge_datasets": [],
            "total_size_mb": 0
        }
        
        # Check TPC-DS datasets
        for tpcds_dir in self.data_path.glob("tpcds_sf_*"):
            if tpcds_dir.is_dir():
                scale_factor = tpcds_dir.name.split("_")[-1]
                files = list(tpcds_dir.glob("*.parquet"))
                size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
                
                summary["tpcds_datasets"].append({
                    "scale_factor": scale_factor,
                    "files": len(files),
                    "size_mb": size_mb
                })
                summary["total_size_mb"] += size_mb
                
        # Check refresh datasets
        for refresh_dir in self.data_path.glob("tpcds_refresh_sf_*"):
            if refresh_dir.is_dir():
                scale_factor = refresh_dir.name.split("_")[-1]
                files = list(refresh_dir.glob("*.parquet"))
                size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
                
                summary["refresh_datasets"].append({
                    "scale_factor": scale_factor,
                    "files": len(files),
                    "size_mb": size_mb
                })
                summary["total_size_mb"] += size_mb
                
        # Check micro merge datasets
        for micro_dir in self.data_path.glob("micro_merge_*"):
            if micro_dir.is_dir():
                size_gb = micro_dir.name.split("_")[-1].replace("gb", "")
                files = list(micro_dir.glob("*.parquet"))
                size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
                
                summary["micro_merge_datasets"].append({
                    "size_gb": size_gb,
                    "files": len(files),
                    "size_mb": size_mb
                })
                summary["total_size_mb"] += size_mb
                
        return summary