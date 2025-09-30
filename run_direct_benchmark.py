#!/usr/bin/env python3
"""
Script direto para executar benchmark - versão mais simples
"""

import os
import sys
sys.path.insert(0, './standalone')

# Importações diretas
from config import LHBenchConfig
import logging
from datetime import datetime

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=== INICIANDO BENCHMARK LHBench TPC-H ===")
    print("Scale Factor: 1")
    print("Formatos: delta, iceberg, hudi")
    
    # Configuração
    config = LHBenchConfig(
        scale_factor=1,
        benchmark_formats=["delta", "iceberg", "hudi"],
        benchmark_iterations=3,
        force_regenerate_bronze=True,
        force_recreate_silver=True,
        timeout_minutes=60
    )
    
    # Criar diretório de resultados
    results_dir = f"./benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(results_dir, exist_ok=True)
    
    print(f"Resultados serão salvos em: {results_dir}")
    
    try:
        # 1. Executar bronze (dados TPC-H)
        print("\n1. Gerando dados Bronze (TPC-H) com DuckDB...")
        from bronze_generator import BronzeGenerator
        bronze_gen = BronzeGenerator(config)
        bronze_gen.generate_all_tables()
        print("✓ Dados Bronze gerados com sucesso")
        
        # 2. Converter para formatos Silver
        print("\n2. Convertendo para formatos Silver...")
        from silver_converter import SilverConverter
        silver_conv = SilverConverter(config)
        
        for fmt in config.benchmark_formats:
            print(f"   - Convertendo para formato {fmt}...")
            silver_conv.convert_all_tables(fmt)
            print(f"   ✓ Formato {fmt} convertido")
        
        # 3. Executar benchmarks
        print("\n3. Executando benchmarks TPC-H...")
        from benchmark_executor import BenchmarkExecutor
        benchmark_exec = BenchmarkExecutor(config)
        
        all_results = {}
        for fmt in config.benchmark_formats:
            print(f"   - Executando benchmark para {fmt}...")
            results = benchmark_exec.run_benchmark(fmt)
            all_results[fmt] = results
            print(f"   ✓ Benchmark {fmt} concluído")
        
        # 4. Relatório final
        print("\n4. Gerando relatório final...")
        report_file = os.path.join(results_dir, "summary_report.txt")
        
        with open(report_file, 'w') as f:
            f.write("=== RELATÓRIO LHBench TPC-H ===\n")
            f.write(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Scale Factor: {config.scale_factor}\n")
            f.write(f"Iterações: {config.benchmark_iterations}\n")
            f.write("=" * 40 + "\n\n")
            
            # Resultados por formato
            format_times = {}
            for fmt, results in all_results.items():
                f.write(f"FORMATO: {fmt.upper()}\n")
                f.write("-" * 20 + "\n")
                
                if 'summary' in results:
                    summary = results['summary']
                    f.write(f"Queries executadas: {summary.get('total_queries', 0)}\n")
                    f.write(f"Queries com sucesso: {summary.get('successful_queries', 0)}\n")
                    f.write(f"Tempo total: {summary.get('total_time', 0):.2f}s\n")
                    f.write(f"Tempo médio: {summary.get('avg_time_per_query', 0):.2f}s\n")
                    format_times[fmt] = summary.get('avg_time_per_query', float('inf'))
                f.write("\n")
            
            # Ranking de performance
            f.write("RANKING DE PERFORMANCE\n")
            f.write("=" * 25 + "\n")
            sorted_formats = sorted(format_times.items(), key=lambda x: x[1])
            for i, (fmt, time) in enumerate(sorted_formats, 1):
                f.write(f"{i}. {fmt.upper()}: {time:.2f}s por query\n")
        
        print(f"✓ Relatório salvo em: {report_file}")
        print("\n=== BENCHMARK CONCLUÍDO COM SUCESSO ===")
        
        # Mostrar resumo no terminal
        print("\nRESUMO DE PERFORMANCE:")
        sorted_formats = sorted(format_times.items(), key=lambda x: x[1])
        for i, (fmt, time) in enumerate(sorted_formats, 1):
            print(f"{i}. {fmt.upper()}: {time:.2f}s por query")
        
    except Exception as e:
        print(f"❌ Erro durante execução: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()