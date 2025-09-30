"""
LHBench TPC-H Benchmark - Versão Standalone
Pipeline completo sem dependência do Airflow
"""

from .main import main, run_full_pipeline
from .config import config, LHBenchConfig
from .bronze_generator import generate_bronze_data
from .silver_converter import convert_all_silver_formats, convert_to_silver_format
from .benchmark_executor import run_complete_benchmark, benchmark_format
from .utils import setup_logging, validate_bronze_data, validate_silver_data

__version__ = "2.0.0-standalone"
__author__ = "LHBench Team"

__all__ = [
    # Funções principais
    "main",
    "run_full_pipeline",
    
    # Etapas individuais
    "generate_bronze_data",
    "convert_all_silver_formats",
    "convert_to_silver_format", 
    "run_complete_benchmark",
    "benchmark_format",
    
    # Configuração
    "config",
    "LHBenchConfig",
    
    # Utilitários
    "setup_logging",
    "validate_bronze_data",
    "validate_silver_data"
]