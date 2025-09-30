"""
Executor de Benchmark TPC-H Gold
Versão standalone sem dependência do Airflow
"""
import logging
import time
import json
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

from .config import config
from .utils import setup_logging, save_results, validate_silver_data, Timer, print_summary_banner
from .tpch_queries import TPCH_QUERIES

def create_spark_session(app_name: str = "LHBench-Benchmark"):
    """Cria sessão Spark para benchmark"""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError("PySpark não está instalado. Execute: pip install pyspark")
    
    spark_config = config.get_spark_config()
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.execution.pyarrow.enabled", "true")
    
    # Aplicar todas as configurações
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def execute_query_with_timeout(spark, query: str, query_id: str, timeout_seconds: int = 1800) -> Dict[str, Any]:
    """Executa uma query com timeout"""
    logger = logging.getLogger("lhbench.benchmark")
    
    def run_query():
        start_time = time.time()
        try:
            df = spark.sql(query)
            result = df.collect()  # Força execução
            end_time = time.time()
            
            return {
                "status": "success",
                "execution_time": end_time - start_time,
                "row_count": len(result),
                "result_sample": [row.asDict() for row in result[:5]] if result else []
            }
        except Exception as e:
            end_time = time.time()
            return {
                "status": "error",
                "execution_time": end_time - start_time,
                "error": str(e)
            }
    
    # Executar com timeout
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_query)
        try:
            result = future.result(timeout=timeout_seconds)
            return result
        except TimeoutError:
            logger.warning(f"⏰ Query {query_id} timeout após {timeout_seconds}s")
            return {
                "status": "timeout",
                "execution_time": timeout_seconds,
                "error": f"Query timeout after {timeout_seconds} seconds"
            }

def register_tables_for_format(spark, format_name: str, scale_factor: int):
    """Registra tabelas do formato como views temporárias"""
    logger = logging.getLogger("lhbench.benchmark")
    
    tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    silver_base = f"s3a://{config.silver_bucket}/sf_{scale_factor}/{format_name}"
    
    registered_tables = []
    
    for table in tpch_tables:
        try:
            table_path = f"{silver_base}/{table}"
            
            # Ler com formato apropriado
            if format_name == "delta":
                df = spark.read.format("delta").load(table_path)
            elif format_name == "iceberg":
                df = spark.read.format("iceberg").load(table_path)
            elif format_name == "hudi":
                df = spark.read.format("hudi").load(table_path)
            else:
                raise ValueError(f"Formato {format_name} não suportado")
            
            # Registrar como view temporária
            df.createOrReplaceTempView(table)
            registered_tables.append(table)
            
            logger.info(f"   ✅ {table}: {df.count():,} linhas registradas")
            
        except Exception as e:
            logger.error(f"   ❌ {table}: erro ao registrar - {e}")
    
    return registered_tables

def run_warmup_queries(spark, format_name: str, registered_tables: List[str]) -> Dict[str, Any]:
    """Executa queries de warmup"""
    logger = logging.getLogger("lhbench.benchmark")
    
    logger.info(f"🔥 Executando warmup para {format_name}...")
    
    warmup_queries = [
        "SELECT COUNT(*) FROM customer",
        "SELECT COUNT(*) FROM orders", 
        "SELECT COUNT(*) FROM lineitem LIMIT 1000"
    ]
    
    warmup_results = []
    
    for i, query in enumerate(warmup_queries, 1):
        try:
            start_time = time.time()
            df = spark.sql(query)
            result = df.collect()
            end_time = time.time()
            
            warmup_results.append({
                "warmup_query": i,
                "execution_time": end_time - start_time,
                "status": "success"
            })
            
            logger.info(f"   ✅ Warmup {i}: {end_time - start_time:.2f}s")
            
        except Exception as e:
            warmup_results.append({
                "warmup_query": i,
                "status": "error",
                "error": str(e)
            })
            logger.error(f"   ❌ Warmup {i}: {e}")
    
    return {
        "warmup_executed": True,
        "warmup_results": warmup_results
    }

def benchmark_format(format_name: str, scale_factor: int, iterations: int = 3, 
                    run_warmup: bool = True, timeout_minutes: int = 30) -> Dict[str, Any]:
    """
    Executa benchmark TPC-H para um formato específico
    
    Args:
        format_name: Nome do formato (delta, iceberg, hudi)
        scale_factor: Scale factor dos dados
        iterations: Número de iterações por query
        run_warmup: Executar warmup antes do benchmark
        timeout_minutes: Timeout por query em minutos
    
    Returns:
        Dict com resultados do benchmark
    """
    logger = setup_logging()
    
    print_summary_banner(f"BENCHMARK TPC-H - {format_name.upper()}", {
        "Scale Factor": scale_factor,
        "Iterações": iterations,
        "Timeout": f"{timeout_minutes} min",
        "Warmup": run_warmup
    })
    
    try:
        with Timer(f"Benchmark {format_name}", logger) as timer:
            # Criar sessão Spark
            spark = create_spark_session(f"LHBench-Benchmark-{format_name}")
            logger.info("✅ Sessão Spark criada")
            
            # Registrar tabelas
            logger.info(f"📋 Registrando tabelas {format_name}...")
            registered_tables = register_tables_for_format(spark, format_name, scale_factor)
            
            if len(registered_tables) == 0:
                raise Exception(f"Nenhuma tabela {format_name} pôde ser registrada")
            
            logger.info(f"✅ {len(registered_tables)} tabelas registradas")
            
            # Warmup (opcional)
            warmup_data = {}
            if run_warmup:
                warmup_data = run_warmup_queries(spark, format_name, registered_tables)
            
            # Executar benchmark das 22 queries
            logger.info(f"🚀 Iniciando benchmark TPC-H - {len(TPCH_QUERIES)} queries")
            
            timeout_seconds = timeout_minutes * 60
            query_results = {}
            
            for query_id in sorted(TPCH_QUERIES.keys()):
                query_sql = TPCH_QUERIES[query_id]
                
                logger.info(f"🔄 Executando {query_id}...")
                
                iteration_results = []
                
                # Executar múltiplas iterações
                for iteration in range(1, iterations + 1):
                    logger.info(f"   Iteração {iteration}/{iterations}")
                    
                    result = execute_query_with_timeout(
                        spark, query_sql, f"{query_id}_iter{iteration}", timeout_seconds
                    )
                    
                    result["iteration"] = iteration
                    result["query_id"] = query_id
                    iteration_results.append(result)
                    
                    if result["status"] == "success":
                        logger.info(f"      ✅ {result['execution_time']:.2f}s - {result['row_count']} linhas")
                    elif result["status"] == "timeout":
                        logger.warning(f"      ⏰ Timeout")
                    else:
                        logger.error(f"      ❌ Erro: {result.get('error', 'Desconhecido')}")
                
                # Calcular estatísticas da query
                successful_iterations = [r for r in iteration_results if r["status"] == "success"]
                
                if successful_iterations:
                    times = [r["execution_time"] for r in successful_iterations]
                    query_stats = {
                        "min_time": min(times),
                        "max_time": max(times),
                        "avg_time": sum(times) / len(times),
                        "successful_iterations": len(successful_iterations),
                        "total_iterations": iterations
                    }
                    logger.info(f"   📊 {query_id}: {query_stats['avg_time']:.2f}s (avg) - {len(successful_iterations)}/{iterations} sucessos")
                else:
                    query_stats = {
                        "min_time": None,
                        "max_time": None,
                        "avg_time": None,
                        "successful_iterations": 0,
                        "total_iterations": iterations
                    }
                    logger.warning(f"   ❌ {query_id}: 0/{iterations} sucessos")
                
                query_results[query_id] = {
                    "query_id": query_id,
                    "iterations": iteration_results,
                    "statistics": query_stats
                }
            
            # Parar Spark
            spark.stop()
            logger.info("✅ Sessão Spark finalizada")
            
            # Calcular estatísticas gerais
            all_successful_queries = [qid for qid, r in query_results.items() 
                                    if r["statistics"]["successful_iterations"] > 0]
            
            total_avg_time = 0
            if all_successful_queries:
                total_avg_time = sum(query_results[qid]["statistics"]["avg_time"] 
                                   for qid in all_successful_queries 
                                   if query_results[qid]["statistics"]["avg_time"] is not None)
            
            results = {
                "status": "success",
                "format": format_name,
                "scale_factor": scale_factor,
                "iterations": iterations,
                "timeout_minutes": timeout_minutes,
                "total_queries": len(TPCH_QUERIES),
                "successful_queries": len(all_successful_queries),
                "total_benchmark_time": total_avg_time,
                "registered_tables": registered_tables,
                "warmup": warmup_data,
                "query_results": query_results,
                "duration_seconds": timer.duration
            }
            
            # Salvar resultados
            local_file, s3_file = save_results(results, f"benchmark_{format_name}")
            logger.info(f"📊 Resultados salvos: {local_file}")
            
            return results
            
    except Exception as e:
        logger.error(f"❌ Erro no benchmark {format_name}: {e}")
        return {
            "status": "error",
            "format": format_name,
            "error": str(e),
            "scale_factor": scale_factor
        }

def run_complete_benchmark(scale_factor: int = None, formats: List[str] = None, 
                          iterations: int = None, run_warmup: bool = True,
                          timeout_minutes: int = None) -> Dict[str, Any]:
    """
    Executa benchmark completo para todos os formatos
    
    Args:
        scale_factor: Scale factor (padrão: config.scale_factor)
        formats: Lista de formatos (padrão: config.benchmark_formats)
        iterations: Iterações por query (padrão: config.benchmark_iterations)
        run_warmup: Executar warmup
        timeout_minutes: Timeout por query (padrão: config.timeout_minutes)
    
    Returns:
        Dict com resultados de todos os benchmarks
    """
    logger = setup_logging()
    
    if scale_factor is None:
        scale_factor = config.scale_factor
    if formats is None:
        formats = config.benchmark_formats
    if iterations is None:
        iterations = config.benchmark_iterations
    if timeout_minutes is None:
        timeout_minutes = config.timeout_minutes
    
    print_summary_banner("BENCHMARK TPC-H COMPLETO", {
        "Scale Factor": scale_factor,
        "Formatos": ", ".join(formats),
        "Iterações": iterations,
        "Timeout": f"{timeout_minutes} min"
    })
    
    # Validar dados silver
    silver_validation = validate_silver_data(scale_factor, formats)
    if silver_validation["validation_status"] != "success":
        logger.error("❌ Dados silver insuficientes para benchmark")
        return {
            "status": "error",
            "error": "Silver data validation failed",
            "silver_validation": silver_validation
        }
    
    available_formats = silver_validation["ready_formats"]
    if not available_formats:
        logger.error("❌ Nenhum formato silver disponível")
        return {
            "status": "error",
            "error": "No silver formats available",
            "available_formats": available_formats
        }
    
    logger.info(f"✅ Formatos disponíveis: {', '.join(available_formats)}")
    
    all_results = {}
    
    try:
        with Timer("Benchmark Completo", logger) as timer:
            # Executar benchmark para cada formato
            for format_name in available_formats:
                logger.info(f"\n🔄 Iniciando benchmark {format_name.upper()}")
                
                result = benchmark_format(
                    format_name, scale_factor, iterations, 
                    run_warmup, timeout_minutes
                )
                
                all_results[format_name] = result
                
                if result["status"] == "success":
                    successful = result["successful_queries"]
                    total = result["total_queries"]
                    logger.info(f"✅ Benchmark {format_name}: {successful}/{total} queries sucessos")
                else:
                    logger.error(f"❌ Benchmark {format_name} falhou")
            
            # Criar comparativo
            comparison = create_benchmark_comparison(all_results)
            
            summary = {
                "status": "success",
                "scale_factor": scale_factor,
                "formats_benchmarked": list(all_results.keys()),
                "iterations": iterations,
                "timeout_minutes": timeout_minutes,
                "individual_results": all_results,
                "comparison": comparison,
                "duration_seconds": timer.duration
            }
            
            # Salvar resumo
            local_file, s3_file = save_results(summary, "benchmark_complete")
            logger.info(f"📊 Resumo completo salvo: {local_file}")
            
            return summary
            
    except Exception as e:
        logger.error(f"❌ Erro no benchmark completo: {e}")
        return {
            "status": "error",
            "error": str(e),
            "scale_factor": scale_factor,
            "individual_results": all_results
        }

def create_benchmark_comparison(benchmark_results: Dict[str, Any]) -> Dict[str, Any]:
    """Cria comparativo entre formatos"""
    
    # Extrair estatísticas por query
    query_comparison = {}
    
    # Obter todas as queries que foram executadas
    all_queries = set()
    for format_results in benchmark_results.values():
        if format_results.get("status") == "success":
            all_queries.update(format_results["query_results"].keys())
    
    # Comparar cada query entre formatos
    for query_id in sorted(all_queries):
        query_comparison[query_id] = {}
        
        for format_name, format_results in benchmark_results.items():
            if (format_results.get("status") == "success" and 
                query_id in format_results["query_results"]):
                
                query_stats = format_results["query_results"][query_id]["statistics"]
                query_comparison[query_id][format_name] = {
                    "avg_time": query_stats["avg_time"],
                    "min_time": query_stats["min_time"],
                    "successful_iterations": query_stats["successful_iterations"]
                }
    
    # Ranking por formato (média geral)
    format_ranking = []
    
    for format_name, format_results in benchmark_results.items():
        if format_results.get("status") == "success":
            successful_queries = [
                qid for qid, r in format_results["query_results"].items()
                if r["statistics"]["successful_iterations"] > 0
            ]
            
            if successful_queries:
                avg_times = [
                    format_results["query_results"][qid]["statistics"]["avg_time"]
                    for qid in successful_queries
                    if format_results["query_results"][qid]["statistics"]["avg_time"] is not None
                ]
                
                overall_avg = sum(avg_times) / len(avg_times) if avg_times else float('inf')
                
                format_ranking.append({
                    "format": format_name,
                    "overall_avg_time": overall_avg,
                    "successful_queries": len(successful_queries),
                    "total_queries": len(format_results["query_results"])
                })
    
    # Ordenar por performance (menor tempo = melhor)
    format_ranking.sort(key=lambda x: x["overall_avg_time"])
    
    return {
        "query_comparison": query_comparison,
        "format_ranking": format_ranking,
        "best_format": format_ranking[0]["format"] if format_ranking else None
    }

def main():
    """Função principal para execução standalone"""
    import sys
    
    # Verificar argumentos
    if len(sys.argv) > 1:
        format_name = sys.argv[1].lower()
        if format_name not in ["delta", "iceberg", "hudi", "all"]:
            print("❌ Formato inválido. Use: delta, iceberg, hudi ou all")
            return 1
    else:
        format_name = "all"
    
    try:
        if format_name == "all":
            results = run_complete_benchmark()
            
            if results["status"] == "success":
                print("\n🎉 Benchmark completo finalizado!")
                
                if results["comparison"]["format_ranking"]:
                    print("\n🏆 Ranking de Performance:")
                    for i, fmt in enumerate(results["comparison"]["format_ranking"], 1):
                        print(f"   {i}. {fmt['format'].upper()}: {fmt['overall_avg_time']:.2f}s (média)")
                        print(f"      Sucessos: {fmt['successful_queries']}/{fmt['total_queries']} queries")
                
                print(f"\n⏱️  Tempo total: {results['duration_seconds']:.2f}s")
                return 0
            else:
                print(f"\n❌ Erro no benchmark: {results.get('error', 'Erro desconhecido')}")
                return 1
        else:
            results = benchmark_format(format_name, config.scale_factor, config.benchmark_iterations)
            
            if results["status"] == "success":
                print(f"\n🎉 Benchmark {format_name} finalizado!")
                print(f"📊 Queries executadas: {results['successful_queries']}/{results['total_queries']}")
                print(f"⏱️  Tempo total: {results['duration_seconds']:.2f}s")
                return 0
            else:
                print(f"\n❌ Erro no benchmark: {results.get('error', 'Erro desconhecido')}")
                return 1
                
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário")
        return 1
    except Exception as e:
        print(f"\n💥 Erro fatal: {e}")
        return 1

if __name__ == "__main__":
    exit(main())