#!/usr/bin/env python3
"""
Script simplificado para executar benchmark completo
Scale Factor 1 - Todos os formatos
"""

import os
import sys
import logging
from datetime import datetime

# Adicionar diretório standalone ao path
sys.path.insert(0, './standalone')

def main():
    """Execução principal do benchmark completo"""
    
    # Setup básico de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== INICIANDO BENCHMARK COMPLETO LHBench TPC-H ===")
    logger.info("Scale Factor: 1")
    logger.info("Formatos: delta, iceberg, hudi")
    
    try:
        # Importar módulos após configurar o path
        from config import LHBenchConfig
        from bronze_generator import BronzeGenerator
        from silver_converter import SilverConverter
        from benchmark_executor import BenchmarkExecutor
        
        # Configuração para Scale Factor 1
        config = LHBenchConfig(
            scale_factor=1,
            benchmark_formats=["delta", "iceberg", "hudi"],
            benchmark_iterations=3,
            force_regenerate_bronze=True,
            force_recreate_silver=True,
            timeout_minutes=60,
            results_path=f"./results/full_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Criar diretório de resultados
        os.makedirs(config.results_path, exist_ok=True)
        
        # 1. Gerar dados Bronze (TPC-H)
        logger.info("1. Gerando dados Bronze (TPC-H)...")
        bronze_gen = BronzeGenerator(config)
        bronze_gen.generate_all_tables()
        
        # 2. Converter para formatos Silver
        logger.info("2. Convertendo para formatos Silver...")
        silver_conv = SilverConverter(config)
        
        for table_format in config.benchmark_formats:
            logger.info(f"   - Convertendo para formato: {table_format}")
            silver_conv.convert_all_tables(table_format)
        
        # 3. Executar benchmarks
        logger.info("3. Executando benchmarks...")
        benchmark_exec = BenchmarkExecutor(config)
        
        all_results = {}
        for table_format in config.benchmark_formats:
            logger.info(f"   - Executando benchmark para: {table_format}")
            results = benchmark_exec.run_benchmark(table_format)
            all_results[table_format] = results
        
        # 4. Gerar relatório final
        logger.info("4. Gerando relatório final...")
        generate_summary_report(all_results, config, logger)
        
        logger.info("=== BENCHMARK COMPLETO FINALIZADO COM SUCESSO ===")
        
    except Exception as e:
        logger.error(f"Erro durante execução do benchmark: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def generate_summary_report(all_results, config, logger):
    """Gerar relatório consolidado de todos os formatos"""
    
    report_file = os.path.join(config.results_path, "summary_report.txt")
    
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
    
    logger.info(f"Relatório consolidado salvo em: {report_file}")
    print(f"Relatório consolidado salvo em: {report_file}")

if __name__ == "__main__":
    main()