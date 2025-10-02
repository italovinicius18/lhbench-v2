"""Cache management for Spark DataFrames."""

from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession


class CacheManager:
    """Manages caching of Spark DataFrames."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._cached_dfs: Dict[str, DataFrame] = {}

    def cache(self, name: str, df: DataFrame, storage_level: str = "MEMORY_AND_DISK") -> DataFrame:
        """
        Cache a DataFrame with a given name.

        Args:
            name: Name to identify the cached DataFrame
            df: DataFrame to cache
            storage_level: Spark storage level

        Returns:
            Cached DataFrame
        """
        if name in self._cached_dfs:
            self.uncache(name)

        # Apply storage level
        if storage_level == "MEMORY_ONLY":
            cached_df = df.persist()
        elif storage_level == "MEMORY_AND_DISK":
            cached_df = df.cache()
        else:
            cached_df = df.persist(storage_level)

        self._cached_dfs[name] = cached_df
        return cached_df

    def get(self, name: str) -> Optional[DataFrame]:
        """Get cached DataFrame by name."""
        return self._cached_dfs.get(name)

    def uncache(self, name: str):
        """Uncache a DataFrame."""
        if name in self._cached_dfs:
            df = self._cached_dfs[name]
            df.unpersist()
            del self._cached_dfs[name]

    def uncache_all(self):
        """Uncache all DataFrames."""
        for name in list(self._cached_dfs.keys()):
            self.uncache(name)

    def clear_spark_cache(self):
        """Clear all Spark cache."""
        self.uncache_all()
        self.spark.catalog.clearCache()
