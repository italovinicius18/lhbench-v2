#!/usr/bin/env python3
"""
Script para executar benchmark completo com todos os formatos
Scale Factor 1 - Teste rápido
"""

import sys
import os
sys.path.append('./standalone')

from config import LHBenchConfig
from utils import setup_logging, check_dependencies
from bronze_generator import BronzeGenerator
from silver_converter import SilverConverter
from benchmark_executor import BenchmarkExecutor
import logging
from datetime import datetime

def main():
    """Execução principal do benchmark completo"""
    
    # Configuração para Scale Factor 1
    config = LHBenchConfig(
        scale_factor=1,
        benchmark_formats=["delta", "iceberg", "hudi"],
        benchmark_iterations=3,
        force_regenerate_bronze=True,  # Garantir dados limpos
        force_recreate_silver=True,    # Garantir dados limpos
        timeout_minutes=60,            # Mais tempo para todos os formatos
        results_path=f"./results/full_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    
    # Setup logging
    logger = setup_logging(config.log_level)
    logger.info("=== INICIANDO BENCHMARK COMPLETO LHBench TPC-H ===")
    logger.info(f"Scale Factor: {config.scale_factor}")
    logger.info(f"Formatos: {', '.join(config.benchmark_formats)}")
    logger.info(f"Iterações por formato: {config.benchmark_iterations}")
    
    try:
        # 1. Verificar dependências
        logger.info("1. Verificando dependências...")
        check_dependencies()
        
        # 2. Gerar dados Bronze (TPC-H)
        logger.info("2. Gerando dados Bronze (TPC-H)...")
        bronze_gen = BronzeGenerator(config)
        bronze_gen.generate_all_tables()
        
        # 3. Converter para formatos Silver
        logger.info("3. Convertendo para formatos Silver...")
        silver_conv = SilverConverter(config)
        
        for table_format in config.benchmark_formats:
            logger.info(f"   - Convertendo para formato: {table_format}")
            silver_conv.convert_all_tables(table_format)
        
        # 4. Executar benchmarks
        logger.info("4. Executando benchmarks...")
        benchmark_exec = BenchmarkExecutor(config)
        
        all_results = {}
        for table_format in config.benchmark_formats:
            logger.info(f"   - Executando benchmark para formato: {table_format}")
            results = benchmark_exec.run_benchmark(table_format)
            all_results[table_format] = results
        
        # 5. Relatório final
        logger.info("5. Gerando relatório final...")
        generate_summary_report(all_results, config)
        
        logger.info("=== BENCHMARK COMPLETO FINALIZADO COM SUCESSO ===")
        
    except Exception as e:
        logger.error(f"Erro durante execução do benchmark: {str(e)}")
        raise

def generate_summary_report(all_results, config):
    """Gerar relatório consolidado de todos os formatos"""
    
    report_file = os.path.join(config.results_path, "summary_report.txt")
    os.makedirs(config.results_path, exist_ok=True)
    
    with open(report_file, 'w') as f:
        f.write("=== RELATÓRIO CONSOLIDADO LHBench TPC-H ===\n")
        f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Scale Factor: {config.scale_factor}\n")
        f.write(f"Iterações: {config.benchmark_iterations}\n")
        f.write("=" * 50 + "\n\n")
        
        # Resumo por formato
        for table_format, results in all_results.items():
            f.write(f"FORMATO: {table_format.upper()}\n")
            f.write("-" * 30 + "\n")
            
            if 'summary' in results:
                summary = results['summary']
                f.write(f"Queries executadas: {summary.get('total_queries', 0)}\n")
                f.write(f"Queries com sucesso: {summary.get('successful_queries', 0)}\n")
                f.write(f"Tempo total: {summary.get('total_time', 0):.2f}s\n")
                f.write(f"Tempo médio por query: {summary.get('avg_time_per_query', 0):.2f}s\n")
                
                if 'failed_queries' in summary and summary['failed_queries']:
                    f.write(f"Queries falharam: {', '.join(map(str, summary['failed_queries']))}\n")
            
            f.write("\n")
        
        # Comparação de performance
        f.write("COMPARAÇÃO DE PERFORMANCE\n")
        f.write("=" * 30 + "\n")
        
        format_times = {}
        for table_format, results in all_results.items():
            if 'summary' in results:
                avg_time = results['summary'].get('avg_time_per_query', float('inf'))
                format_times[table_format] = avg_time
        
        # Ordenar por performance
        sorted_formats = sorted(format_times.items(), key=lambda x: x[1])
        
        for i, (table_format, avg_time) in enumerate(sorted_formats, 1):
            f.write(f"{i}. {table_format.upper()}: {avg_time:.2f}s por query\n")
    
    print(f"Relatório consolidado salvo em: {report_file}")

if __name__ == "__main__":
    main()