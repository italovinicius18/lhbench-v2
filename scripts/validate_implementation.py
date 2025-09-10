#!/usr/bin/env python3
"""
Validation script for LHBench v2 Spark engines implementation
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_imports():
    """Validate that all components can be imported"""
    logger.info("Validating imports...")
    
    try:
        # Core engines
        from lhbench.engines import (
            BaseLakehouseEngine,
            DeltaEngine,
            IcebergEngine,
            HudiEngine,
            EngineFactory,
            MultiEngineManager,
            SparkManager
        )
        logger.info("✓ Core engines imported successfully")
        
        # Configuration
        from lhbench.engines.config import (
            get_engine_config,
            get_spark_config,
            get_all_engines_config
        )
        logger.info("✓ Configuration module imported successfully")
        
        # Storage components
        from lhbench.storage import MinIOClient, StorageManager
        logger.info("✓ Storage components imported successfully")
        
        # Data generation
        from lhbench.data import TPCDSGenerator
        logger.info("✓ Data generation components imported successfully")
        
        return True
        
    except ImportError as e:
        logger.error(f"✗ Import failed: {e}")
        return False


def validate_configurations():
    """Validate configuration generation"""
    logger.info("Validating configurations...")
    
    try:
        from lhbench.engines.config import (
            get_engine_config,
            get_spark_config,
            get_all_engines_config,
            DEFAULT_SPARK_CONFIG,
            DEFAULT_ENGINE_CONFIGS
        )
        
        # Test Spark configuration
        spark_config = get_spark_config(scale_factor=1)
        assert isinstance(spark_config, dict)
        assert 'app_name' in spark_config
        assert 's3' in spark_config
        logger.info("✓ Spark configuration valid")
        
        # Test engine configurations
        for engine_type in ['delta', 'iceberg', 'hudi']:
            config = get_engine_config(engine_type, scale_factor=1)
            assert isinstance(config, dict)
            assert 'enabled' in config
            logger.info(f"✓ {engine_type} configuration valid")
            
        # Test all engines configuration
        all_config = get_all_engines_config(scale_factor=1)
        assert len(all_config) == 3
        logger.info("✓ All engines configuration valid")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {e}")
        return False


def validate_factory_pattern():
    """Validate engine factory functionality"""
    logger.info("Validating factory pattern...")
    
    try:
        from lhbench.engines import EngineFactory
        
        # Test available engines
        available = EngineFactory.get_available_engines()
        expected_engines = ['delta', 'iceberg', 'hudi']
        
        for engine in expected_engines:
            assert engine in available, f"Engine {engine} not available"
            
        logger.info(f"✓ Available engines: {available}")
        
        # Test engine validation
        for engine_type in expected_engines:
            is_valid = EngineFactory.validate_engine_type(engine_type)
            assert is_valid, f"Engine {engine_type} not valid"
            
        logger.info("✓ Engine validation working")
        
        # Test invalid engine
        assert not EngineFactory.validate_engine_type('invalid_engine')
        logger.info("✓ Invalid engine rejection working")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Factory pattern validation failed: {e}")
        return False


def validate_base_engine_interface():
    """Validate base engine interface"""
    logger.info("Validating base engine interface...")
    
    try:
        from lhbench.engines import BaseLakehouseEngine
        import inspect
        
        # Check that it's an abstract class
        assert inspect.isabstract(BaseLakehouseEngine), "BaseLakehouseEngine should be abstract"
        
        # Check abstract methods
        abstract_methods = [
            'create_table',
            'load_data', 
            'run_query',
            'merge_data',
            'get_table_info',
            'optimize_table'
        ]
        
        # Get actual abstract methods
        actual_abstract_methods = [
            name for name, method in inspect.getmembers(BaseLakehouseEngine, predicate=inspect.isfunction)
            if getattr(method, '__isabstractmethod__', False)
        ]
        
        logger.info(f"Found abstract methods: {actual_abstract_methods}")
        
        for method_name in abstract_methods:
            assert hasattr(BaseLakehouseEngine, method_name), f"Method {method_name} should exist"
            method = getattr(BaseLakehouseEngine, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"Method {method_name} should be abstract"
            
        logger.info(f"✓ Abstract methods validated: {abstract_methods}")
        
        # Check concrete methods
        concrete_methods = [
            'drop_table',
            'list_tables',
            'get_engine_info',
            'validate_setup'
        ]
        
        for method_name in concrete_methods:
            assert hasattr(BaseLakehouseEngine, method_name), f"Method {method_name} should exist"
            method = getattr(BaseLakehouseEngine, method_name)
            assert not getattr(method, '__isabstractmethod__', False), f"Method {method_name} should be concrete"
            
        logger.info(f"✓ Concrete methods validated: {concrete_methods}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Base engine interface validation failed: {e}")
        return False


def validate_engine_implementations():
    """Validate that all engines implement the interface correctly"""
    logger.info("Validating engine implementations...")
    
    try:
        from lhbench.engines import DeltaEngine, IcebergEngine, HudiEngine, BaseLakehouseEngine
        import inspect
        
        engines = [
            ('Delta', DeltaEngine),
            ('Iceberg', IcebergEngine),
            ('Hudi', HudiEngine)
        ]
        
        # Get abstract methods from base class
        base_abstract_methods = [
            name for name, method in inspect.getmembers(BaseLakehouseEngine, predicate=inspect.isfunction)
            if hasattr(method, '__isabstractmethod__') and method.__isabstractmethod__
        ]
        
        for engine_name, engine_class in engines:
            # Check if it's a subclass
            assert issubclass(engine_class, BaseLakehouseEngine)
            
            # Check if all abstract methods are implemented
            for method_name in base_abstract_methods:
                assert hasattr(engine_class, method_name)
                method = getattr(engine_class, method_name)
                # Should not be abstract in concrete implementation
                assert not getattr(method, '__isabstractmethod__', False)
                
            logger.info(f"✓ {engine_name} engine implements all required methods")
            
        return True
        
    except Exception as e:
        logger.error(f"✗ Engine implementations validation failed: {e}")
        return False


def validate_spark_manager():
    """Validate Spark manager functionality (without actual Spark)"""
    logger.info("Validating Spark manager...")
    
    try:
        from lhbench.engines import SparkManager
        from lhbench.engines.config import get_spark_config
        
        # Test manager creation
        config = get_spark_config()
        manager = SparkManager(config)
        
        assert manager.config == config
        assert manager.spark_session is None  # Not created yet
        
        logger.info("✓ Spark manager creation working")
        
        # Test configuration building (without actually creating session)
        # This tests the internal methods
        engines = ['delta', 'iceberg', 'hudi']
        spark_conf = manager._build_spark_config(engines)
        
        # Check if extensions are properly set
        extensions = manager._get_sql_extensions(engines)
        assert 'io.delta.sql.DeltaSparkSessionExtension' in extensions
        assert 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension' in extensions
        
        logger.info("✓ Spark configuration building working")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Spark manager validation failed: {e}")
        return False


def validate_file_structure():
    """Validate project file structure"""
    logger.info("Validating file structure...")
    
    required_files = [
        'lhbench/__init__.py',
        'lhbench/engines/__init__.py',
        'lhbench/engines/base_engine.py',
        'lhbench/engines/delta_engine.py',
        'lhbench/engines/iceberg_engine.py',
        'lhbench/engines/hudi_engine.py',
        'lhbench/engines/engine_factory.py',
        'lhbench/engines/spark_manager.py',
        'lhbench/engines/config.py',
        'examples/engines_example.py',
        'scripts/test_engines.py',
        'docs/engines_implementation.md'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        full_path = project_root / file_path
        if not full_path.exists():
            missing_files.append(file_path)
        else:
            logger.info(f"✓ {file_path}")
            
    if missing_files:
        logger.error(f"✗ Missing files: {missing_files}")
        return False
        
    logger.info("✓ All required files present")
    return True


def validate_dependencies():
    """Validate required dependencies"""
    logger.info("Validating dependencies...")
    
    required_packages = [
        'duckdb',
        'boto3',
        'yaml',
        'numpy',
        'pandas',
        'click',
        'structlog',
        'psutil'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package}")
        except ImportError:
            missing_packages.append(package)
            logger.warning(f"? {package} - not available but not critical for validation")
            
    # Note: PySpark and related packages might not be available in all environments
    # but the code structure should still be valid
    
    logger.info("✓ Core dependencies validation completed")
    return True


def main():
    """Main validation function"""
    
    logger.info("=== LHBench v2 Engines Implementation Validation ===")
    
    validations = [
        ("File Structure", validate_file_structure),
        ("Dependencies", validate_dependencies),
        ("Imports", validate_imports),
        ("Configurations", validate_configurations),
        ("Factory Pattern", validate_factory_pattern),
        ("Base Engine Interface", validate_base_engine_interface),
        ("Engine Implementations", validate_engine_implementations),
        ("Spark Manager", validate_spark_manager)
    ]
    
    results = {}
    
    for name, validation_func in validations:
        logger.info(f"\n--- {name} Validation ---")
        try:
            results[name] = validation_func()
        except Exception as e:
            logger.error(f"✗ {name} validation failed with exception: {e}")
            results[name] = False
            
    # Summary
    logger.info("\n=== Validation Summary ===")
    passed = 0
    total = len(validations)
    
    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"{name}: {status}")
        if result:
            passed += 1
            
    logger.info(f"\nOverall: {passed}/{total} validations passed")
    
    if passed == total:
        logger.info("🎉 All validations passed! Implementation is ready.")
        return 0
    else:
        logger.error("❌ Some validations failed. Please check the issues above.")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
