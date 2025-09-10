"""
Configuration file for lakehouse engines
"""

# Default Spark configuration
DEFAULT_SPARK_CONFIG = {
    'app_name': 'LakehouseBenchmark',
    'executor_memory': '2g',
    'driver_memory': '2g', 
    'executor_cores': 2,
    'executor_instances': 2,
    'driver_max_result_size': '1g',
    'enable_hive_support': True,
    'timezone': 'UTC',
    's3': {
        'endpoint': 'http://localhost:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin'
    },
    'spark_configs': {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true'
    }
}

# Default engine configurations
DEFAULT_ENGINE_CONFIGS = {
    'delta': {
        'enabled': True,
        'storage_path': 's3a://lakehouse/delta',
        'table_properties': {
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        }
    },
    'iceberg': {
        'enabled': True,
        'catalog_name': 'lakehouse_catalog',
        'warehouse_path': 's3a://lakehouse/iceberg',
        'table_properties': {
            'write.merge.mode': 'copy-on-write',
            'write.update.mode': 'copy-on-write'
        }
    },
    'hudi': {
        'enabled': True,
        'storage_path': 's3a://lakehouse/hudi',
        'database_name': 'default',
        'table_type': 'COPY_ON_WRITE',  # or MERGE_ON_READ
        'record_key': 'id',
        'precombine_field': 'ts',
        'hudi_options': {
            'hoodie.compact.inline': 'false',
            'hoodie.clustering.inline': 'false',
            'hoodie.datasource.write.hive_style_partitioning': 'true'
        }
    }
}

# TPC-DS specific configurations
TPCDS_ENGINE_CONFIGS = {
    'delta': {
        'partitions': {
            'store_sales': ['ss_sold_date_sk'],
            'store_returns': ['sr_returned_date_sk'],
            'web_sales': ['ws_sold_date_sk'],
            'web_returns': ['wr_returned_date_sk'],
            'catalog_sales': ['cs_sold_date_sk'],
            'catalog_returns': ['cr_returned_date_sk'],
            'inventory': ['inv_date_sk']
        },
        'table_properties': {
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true',
            'delta.tuneFileSizesForRewrites': 'true'
        }
    },
    'iceberg': {
        'partitions': {
            'store_sales': ['ss_sold_date_sk'],
            'store_returns': ['sr_returned_date_sk'],
            'web_sales': ['ws_sold_date_sk'],
            'web_returns': ['wr_returned_date_sk'],
            'catalog_sales': ['cs_sold_date_sk'],
            'catalog_returns': ['cr_returned_date_sk'],
            'inventory': ['inv_date_sk']
        },
        'table_properties': {
            'write.target-file-size-bytes': '134217728',  # 128MB
            'write.delete.mode': 'copy-on-write',
            'write.update.mode': 'copy-on-write'
        }
    },
    'hudi': {
        'partitions': {
            'store_sales': ['ss_sold_date_sk'],
            'store_returns': ['sr_returned_date_sk'],
            'web_sales': ['ws_sold_date_sk'],
            'web_returns': ['wr_returned_date_sk'],
            'catalog_sales': ['cs_sold_date_sk'],
            'catalog_returns': ['cr_returned_date_sk'],
            'inventory': ['inv_date_sk']
        },
        'table_configs': {
            'store_sales': {
                'record_key': 'ss_item_sk,ss_ticket_number',
                'precombine_field': 'ss_sold_time_sk'
            },
            'store_returns': {
                'record_key': 'sr_item_sk,sr_ticket_number',
                'precombine_field': 'sr_returned_time_sk'
            },
            'web_sales': {
                'record_key': 'ws_item_sk,ws_order_number',
                'precombine_field': 'ws_sold_time_sk'
            },
            'web_returns': {
                'record_key': 'wr_item_sk,wr_order_number',
                'precombine_field': 'wr_returned_time_sk'
            },
            'catalog_sales': {
                'record_key': 'cs_item_sk,cs_order_number',
                'precombine_field': 'cs_sold_time_sk'
            },
            'catalog_returns': {
                'record_key': 'cr_item_sk,cr_order_number',
                'precombine_field': 'cr_returned_time_sk'
            }
        }
    }
}

# Performance test configurations
PERFORMANCE_TEST_CONFIG = {
    'warmup_iterations': 1,
    'test_iterations': 3,
    'timeout_seconds': 3600,  # 1 hour
    'memory_monitoring': True,
    'collect_metrics': True
}

# Benchmark query categories
BENCHMARK_QUERIES = {
    'adhoc': ['q1', 'q2', 'q3', 'q19', 'q27', 'q42', 'q43', 'q52', 'q55', 'q67'],
    'reporting': ['q5', 'q6', 'q12', 'q13', 'q15', 'q18', 'q20', 'q21', 'q22'],
    'iterative': ['q14a', 'q14b', 'q23a', 'q23b', 'q24a', 'q24b', 'q39a', 'q39b'],
    'extraction': ['q7', 'q16', 'q25', 'q29', 'q35', 'q49', 'q64', 'q74'],
    'all': None  # Will include all available queries
}

def get_engine_config(engine_type: str, scale_factor: int = 1) -> dict:
    """
    Get configuration for specific engine type
    
    Args:
        engine_type: Type of engine ('delta', 'iceberg', 'hudi')
        scale_factor: TPC-DS scale factor
        
    Returns:
        Engine configuration dictionary
    """
    base_config = DEFAULT_ENGINE_CONFIGS.get(engine_type, {}).copy()
    tpcds_config = TPCDS_ENGINE_CONFIGS.get(engine_type, {})
    
    # Merge configurations
    for key, value in tpcds_config.items():
        if key in base_config and isinstance(base_config[key], dict) and isinstance(value, dict):
            base_config[key].update(value)
        else:
            base_config[key] = value
            
    # Adjust for scale factor
    if scale_factor > 1:
        # Adjust memory and parallelism for larger datasets
        pass
        
    return base_config

def get_spark_config(scale_factor: int = 1) -> dict:
    """
    Get Spark configuration adjusted for scale factor
    
    Args:
        scale_factor: TPC-DS scale factor
        
    Returns:
        Spark configuration dictionary
    """
    config = DEFAULT_SPARK_CONFIG.copy()
    
    # Adjust memory and cores based on scale factor
    if scale_factor >= 10:
        config['executor_memory'] = '4g'
        config['driver_memory'] = '4g'
        config['executor_cores'] = 4
        config['executor_instances'] = 4
    elif scale_factor >= 100:
        config['executor_memory'] = '8g'
        config['driver_memory'] = '8g'
        config['executor_cores'] = 8
        config['executor_instances'] = 8
        
    return config

def get_all_engines_config(scale_factor: int = 1) -> dict:
    """
    Get configuration for all engines
    
    Args:
        scale_factor: TPC-DS scale factor
        
    Returns:
        Dictionary with configuration for all engines
    """
    return {
        engine: get_engine_config(engine, scale_factor)
        for engine in ['delta', 'iceberg', 'hudi']
    }
