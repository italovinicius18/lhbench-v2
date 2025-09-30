"""
Script principal do LHBench standalone
Executa todo o pipeline TPC-H sem dependência do Airflow
"""
import sys
import argparse
import logging
from datetime import datetime
from typing import Dict, Any

from config import LHBenchConfig
from utils import setup_logging, print_summary_banner
from bronze_generator import generate_bronze_data
from silver_converter import convert_all_silver_formats
from benchmark_executor import run_complete_benchmark

def run_full_pipeline(scale_factor: int = None, 
                     force_regenerate_bronze: bool = False,
                     force_recreate_silver: bool = False,
                     benchmark_formats: list = None,
                     benchmark_iterations: int = None,
                     skip_bronze: bool = False,
                     skip_silver: bool = False,
                     skip_benchmark: bool = False) -> Dict[str, Any]:
    """
    Executa pipeline completo TPC-H
    
    Args:
        scale_factor: Scale factor para TPC-H
        force_regenerate_bronze: Forçar regeneração bronze
        force_recreate_silver: Forçar recriação silver
        benchmark_formats: Lista de formatos para benchmark
        benchmark_iterations: Iterações por query
        skip_bronze: Pular etapa bronze
        skip_silver: Pular etapa silver
        skip_benchmark: Pular etapa benchmark
    
    Returns:
        Dict com resultados de todas as etapas
    """
    logger = setup_logging()
    
    # Usar configurações padrão se não especificadas
    if scale_factor is None:
        scale_factor = config.scale_factor
    if benchmark_formats is None:
        benchmark_formats = config.benchmark_formats
    if benchmark_iterations is None:
        benchmark_iterations = config.benchmark_iterations
    
    execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print_summary_banner("LHBENCH TPC-H PIPELINE COMPLETO", {
        "Execution ID": execution_id,
        "Scale Factor": scale_factor,
        "Formatos Benchmark": ", ".join(benchmark_formats),
        "Iterações": benchmark_iterations,
        "Skip Bronze": skip_bronze,
        "Skip Silver": skip_silver,
        "Skip Benchmark": skip_benchmark
    })
    
    pipeline_results = {
        "execution_id": execution_id,
        "scale_factor": scale_factor,
        "start_time": datetime.now().isoformat(),
        "config": {
            "force_regenerate_bronze": force_regenerate_bronze,
            "force_recreate_silver": force_recreate_silver,
            "benchmark_formats": benchmark_formats,
            "benchmark_iterations": benchmark_iterations
        },
        "stages": {}
    }
    
    try:
        # ETAPA 1: Geração Bronze
        if not skip_bronze:
            logger.info("🥉 ETAPA 1: Geração de dados Bronze")
            bronze_result = generate_bronze_data(scale_factor, force_regenerate_bronze)
            pipeline_results["stages"]["bronze"] = bronze_result
            
            if bronze_result["status"] == "error":
                logger.error("❌ Falha na etapa Bronze - interrompendo pipeline")
                pipeline_results["status"] = "failed_at_bronze"
                return pipeline_results
        else:
            logger.info("⏭️ Pulando etapa Bronze")
            pipeline_results["stages"]["bronze"] = {"status": "skipped"}
        
        # ETAPA 2: Conversão Silver
        if not skip_silver:
            logger.info("\n🥈 ETAPA 2: Conversão Bronze → Silver")
            silver_result = convert_all_silver_formats(
                scale_factor, benchmark_formats, force_recreate_silver
            )
            pipeline_results["stages"]["silver"] = silver_result
            
            if silver_result["status"] == "error":
                logger.error("❌ Falha na etapa Silver - interrompendo pipeline")
                pipeline_results["status"] = "failed_at_silver"
                return pipeline_results
        else:
            logger.info("⏭️ Pulando etapa Silver")
            pipeline_results["stages"]["silver"] = {"status": "skipped"}
        
        # ETAPA 3: Benchmark Gold
        if not skip_benchmark:
            logger.info("\n🥇 ETAPA 3: Benchmark TPC-H Gold")
            benchmark_result = run_complete_benchmark(
                scale_factor, benchmark_formats, benchmark_iterations
            )
            pipeline_results["stages"]["benchmark"] = benchmark_result
            
            if benchmark_result["status"] == "error":
                logger.error("❌ Falha na etapa Benchmark")
                pipeline_results["status"] = "failed_at_benchmark"
                return pipeline_results
        else:
            logger.info("⏭️ Pulando etapa Benchmark")
            pipeline_results["stages"]["benchmark"] = {"status": "skipped"}
        
        # Pipeline concluído com sucesso
        pipeline_results["status"] = "success"
        pipeline_results["end_time"] = datetime.now().isoformat()
        
        # Resumo final
        logger.info("\n" + "="*80)
        logger.info(" PIPELINE TPC-H CONCLUÍDO COM SUCESSO! ".center(80))
        logger.info("="*80)
        
        if not skip_bronze:
            bronze_status = pipeline_results["stages"]["bronze"]["status"]
            logger.info(f"🥉 Bronze: {bronze_status}")
        
        if not skip_silver:
            silver_status = pipeline_results["stages"]["silver"]["status"]
            silver_formats = pipeline_results["stages"]["silver"].get("formats_successful", [])
            logger.info(f"🥈 Silver: {silver_status} - Formatos: {', '.join(silver_formats)}")
        
        if not skip_benchmark:
            benchmark_status = pipeline_results["stages"]["benchmark"]["status"]
            if benchmark_status == "success":
                ranking = pipeline_results["stages"]["benchmark"]["comparison"]["format_ranking"]
                if ranking:
                    best_format = ranking[0]["format"]
                    best_time = ranking[0]["overall_avg_time"]
                    logger.info(f"🥇 Benchmark: {benchmark_status} - Melhor: {best_format} ({best_time:.2f}s)")
                else:
                    logger.info(f"🥇 Benchmark: {benchmark_status}")
        
        logger.info("="*80)
        
        return pipeline_results
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrompido pelo usuário")
        pipeline_results["status"] = "interrupted"
        pipeline_results["end_time"] = datetime.now().isoformat()
        return pipeline_results
        
    except Exception as e:
        logger.error(f"💥 Erro fatal no pipeline: {e}")
        pipeline_results["status"] = "error"
        pipeline_results["error"] = str(e)
        pipeline_results["end_time"] = datetime.now().isoformat()
        return pipeline_results

def main():
    """Função principal com argumentos CLI"""
    parser = argparse.ArgumentParser(
        description="LHBench TPC-H Benchmark - Versão Standalone",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python -m standalone.main                           # Pipeline completo
  python -m standalone.main --scale-factor 1         # SF=1
  python -m standalone.main --skip-bronze            # Pular bronze
  python -m standalone.main --benchmark-only         # Só benchmark
  python -m standalone.main --formats delta iceberg  # Apenas Delta e Iceberg
        """
    )
    
    # Argumentos de configuração
    parser.add_argument(
        "--scale-factor", "-sf", type=int, default=config.scale_factor,
        help=f"Scale Factor TPC-H (padrão: {config.scale_factor})"
    )
    
    parser.add_argument(
        "--formats", nargs="+", choices=["delta", "iceberg", "hudi"],
        default=config.benchmark_formats,
        help=f"Formatos para benchmark (padrão: {' '.join(config.benchmark_formats)})"
    )
    
    parser.add_argument(
        "--iterations", "-i", type=int, default=config.benchmark_iterations,
        help=f"Iterações por query (padrão: {config.benchmark_iterations})"
    )
    
    # Argumentos de controle de etapas
    parser.add_argument(
        "--skip-bronze", action="store_true",
        help="Pular geração de dados bronze"
    )
    
    parser.add_argument(
        "--skip-silver", action="store_true", 
        help="Pular conversão silver"
    )
    
    parser.add_argument(
        "--skip-benchmark", action="store_true",
        help="Pular benchmark gold"
    )
    
    parser.add_argument(
        "--bronze-only", action="store_true",
        help="Executar apenas geração bronze"
    )
    
    parser.add_argument(
        "--silver-only", action="store_true",
        help="Executar apenas conversão silver"
    )
    
    parser.add_argument(
        "--benchmark-only", action="store_true",
        help="Executar apenas benchmark"
    )
    
    # Argumentos de força
    parser.add_argument(
        "--force-bronze", action="store_true",
        help="Forçar regeneração bronze mesmo se existir"
    )
    
    parser.add_argument(
        "--force-silver", action="store_true",
        help="Forçar recriação silver mesmo se existir"
    )
    
    # Argumentos de configuração avançada
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=config.log_level, help=f"Nível de log (padrão: {config.log_level})"
    )
    
    args = parser.parse_args()
    
    # Ajustar configuração global
    config.log_level = args.log_level
    
    # Determinar etapas a executar
    if args.bronze_only:
        skip_silver = True
        skip_benchmark = True
        skip_bronze = False
    elif args.silver_only:
        skip_bronze = True
        skip_benchmark = True
        skip_silver = False
    elif args.benchmark_only:
        skip_bronze = True
        skip_silver = True
        skip_benchmark = False
    else:
        skip_bronze = args.skip_bronze
        skip_silver = args.skip_silver
        skip_benchmark = args.skip_benchmark
    
    try:
        results = run_full_pipeline(
            scale_factor=args.scale_factor,
            force_regenerate_bronze=args.force_bronze,
            force_recreate_silver=args.force_silver,
            benchmark_formats=args.formats,
            benchmark_iterations=args.iterations,
            skip_bronze=skip_bronze,
            skip_silver=skip_silver,
            skip_benchmark=skip_benchmark
        )
        
        # Códigos de saída baseados no resultado
        if results["status"] == "success":
            return 0
        elif results["status"] == "interrupted":
            return 130  # Código padrão para SIGINT
        else:
            return 1
            
    except Exception as e:
        print(f"💥 Erro fatal: {e}")
        return 1

if __name__ == "__main__":
    exit(main())