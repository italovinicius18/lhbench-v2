#!/usr/bin/env python3
"""
Script principal simplificado do LHBench standalone
"""
import sys
import argparse
import logging
from datetime import datetime

def main():
    """Função principal"""
    
    # Parser de argumentos
    parser = argparse.ArgumentParser(description="LHBench TPC-H Standalone")
    parser.add_argument("--scale-factor", "-sf", type=int, default=1, help="Scale Factor TPC-H")
    parser.add_argument("--formats", nargs="+", choices=["delta", "iceberg", "hudi"], 
                        default=["delta", "iceberg", "hudi"], help="Formatos para benchmark")
    parser.add_argument("--iterations", "-i", type=int, default=3, help="Iterações por query")
    parser.add_argument("--force-bronze", action="store_true", help="Forçar regeneração bronze")
    parser.add_argument("--force-silver", action="store_true", help="Forçar recriação silver")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    print("=" * 50)
    print("LHBench TPC-H Standalone")
    print("=" * 50)
    print(f"Scale Factor: {args.scale_factor}")
    print(f"Formatos: {', '.join(args.formats)}")
    print(f"Iterações: {args.iterations}")
    print()
    
    try:
        # Importar módulos
        from config import LHBenchConfig
        from bronze_generator import generate_bronze_data
        from silver_converter import convert_all_silver_formats
        from benchmark_executor import run_complete_benchmark
        
        # Configuração
        config = LHBenchConfig(
            scale_factor=args.scale_factor,
            benchmark_formats=args.formats,
            benchmark_iterations=args.iterations,
            force_regenerate_bronze=args.force_bronze,
            force_recreate_silver=args.force_silver,
            results_path=f"../benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        results = {}
        
        # 1. Geração Bronze
        print("1️⃣ Gerando dados Bronze (TPC-H)...")
        bronze_results = generate_bronze_data(args.scale_factor, args.force_bronze)
        results['bronze'] = bronze_results
        print("✅ Dados Bronze gerados com sucesso")
        
        # 2. Conversão Silver
        print("\n2️⃣ Convertendo para formatos Silver...")
        silver_results = convert_all_silver_formats(
            scale_factor=args.scale_factor,
            formats=args.formats,
            force_recreate=args.force_silver
        )
        results['silver'] = silver_results
        print("✅ Conversão Silver concluída")
        
        # 3. Benchmark
        print("\n3️⃣ Executando benchmarks TPC-H...")
        benchmark_results = run_complete_benchmark(
            scale_factor=args.scale_factor,
            formats=args.formats,
            iterations=args.iterations
        )
        results['benchmark'] = benchmark_results
        
        # 4. Relatório final
        print("\n4️⃣ Gerando relatório...")
        
        # Mostrar resultados no terminal
        print("\n" + "=" * 50)
        print("RESULTADOS FINAIS")
        print("=" * 50)
        
        format_times = {}
        for fmt, fmt_results in benchmark_results.items():
            if 'summary' in fmt_results:
                summary = fmt_results['summary']
                avg_time = summary.get('avg_time_per_query', 0)
                format_times[fmt] = avg_time
                
                print(f"\n{fmt.upper()}:")
                print(f"  Queries executadas: {summary.get('total_queries', 0)}")
                print(f"  Queries com sucesso: {summary.get('successful_queries', 0)}")
                print(f"  Tempo médio por query: {avg_time:.2f}s")
        
        # Ranking
        if format_times:
            print(f"\n{'RANKING DE PERFORMANCE'}")
            print("-" * 30)
            sorted_formats = sorted(format_times.items(), key=lambda x: x[1])
            for i, (fmt, time) in enumerate(sorted_formats, 1):
                print(f"{i}. {fmt.upper()}: {time:.2f}s por query")
        
        print("\n✅ Benchmark completo finalizado com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro durante execução: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())