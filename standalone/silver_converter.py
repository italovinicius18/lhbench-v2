"""
Conversor Bronze para Silver (Delta, Iceberg, Hudi)
Versão standalone sem dependência do Airflow
"""
import logging
from datetime import datetime
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Config será passado como parâmetro
from utils import setup_logging, save_results, validate_bronze_data, validate_silver_data, Timer, print_summary_banner

def create_spark_session(app_name: str = "LHBench-Silver"):
    """Cria sessão Spark com configurações necessárias"""
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

def convert_table_to_delta(spark, table_name: str, source_path: str, target_path: str, scale_factor: int) -> Dict[str, Any]:
    """Converte uma tabela para formato Delta"""
    logger = logging.getLogger("lhbench.silver")
    
    try:
        with Timer(f"Conversão Delta - {table_name}", logger) as timer:
            # Ler dados bronze
            if config.input_format.lower() == "parquet":
                df = spark.read.parquet(source_path)
            else:  # CSV
                df = spark.read.option("header", "true").csv(source_path)
            
            # Escrever como Delta
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(target_path)
            
            # Verificar dados escritos
            row_count = spark.read.format("delta").load(target_path).count()
            
            return {
                "table": table_name,
                "status": "success",
                "source_path": source_path,
                "target_path": target_path,
                "row_count": row_count,
                "duration_seconds": timer.duration
            }
            
    except Exception as e:
        logger.error(f"❌ Erro convertendo {table_name} para Delta: {e}")
        return {
            "table": table_name,
            "status": "error",
            "error": str(e),
            "source_path": source_path,
            "target_path": target_path
        }

def convert_table_to_iceberg(spark, table_name: str, source_path: str, target_path: str, scale_factor: int) -> Dict[str, Any]:
    """Converte uma tabela para formato Iceberg"""
    logger = logging.getLogger("lhbench.silver")
    
    try:
        with Timer(f"Conversão Iceberg - {table_name}", logger) as timer:
            # Ler dados bronze
            if config.input_format.lower() == "parquet":
                df = spark.read.parquet(source_path)
            else:  # CSV
                df = spark.read.option("header", "true").csv(source_path)
            
            # Escrever como Iceberg
            df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .save(target_path)
            
            # Verificar dados escritos
            row_count = spark.read.format("iceberg").load(target_path).count()
            
            return {
                "table": table_name,
                "status": "success",
                "source_path": source_path,
                "target_path": target_path,
                "row_count": row_count,
                "duration_seconds": timer.duration
            }
            
    except Exception as e:
        logger.error(f"❌ Erro convertendo {table_name} para Iceberg: {e}")
        return {
            "table": table_name,
            "status": "error",
            "error": str(e),
            "source_path": source_path,
            "target_path": target_path
        }

def convert_table_to_hudi(spark, table_name: str, source_path: str, target_path: str, scale_factor: int) -> Dict[str, Any]:
    """Converte uma tabela para formato Hudi"""
    logger = logging.getLogger("lhbench.silver")
    
    try:
        with Timer(f"Conversão Hudi - {table_name}", logger) as timer:
            # Ler dados bronze
            if config.input_format.lower() == "parquet":
                df = spark.read.parquet(source_path)
            else:  # CSV
                df = spark.read.option("header", "true").csv(source_path)
            
            # Configurações específicas do Hudi para cada tabela
            hudi_options = {
                "hoodie.table.name": f"tpch_{table_name}_sf{scale_factor}",
                "hoodie.datasource.write.recordkey.field": get_hudi_record_key(table_name),
                "hoodie.datasource.write.partitionpath.field": get_hudi_partition_key(table_name),
                "hoodie.datasource.write.table.name": f"tpch_{table_name}_sf{scale_factor}",
                "hoodie.datasource.write.operation": "upsert",
                "hoodie.datasource.write.precombine.field": get_hudi_precombine_key(table_name),
                "hoodie.upsert.shuffle.parallelism": "200",
                "hoodie.insert.shuffle.parallelism": "200"
            }
            
            # Escrever como Hudi
            df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(target_path)
            
            # Verificar dados escritos
            row_count = spark.read.format("hudi").load(target_path).count()
            
            return {
                "table": table_name,
                "status": "success", 
                "source_path": source_path,
                "target_path": target_path,
                "row_count": row_count,
                "duration_seconds": timer.duration,
                "hudi_options": hudi_options
            }
            
    except Exception as e:
        logger.error(f"❌ Erro convertendo {table_name} para Hudi: {e}")
        return {
            "table": table_name,
            "status": "error",
            "error": str(e),
            "source_path": source_path,
            "target_path": target_path
        }

def get_hudi_record_key(table_name: str) -> str:
    """Retorna chave primária para cada tabela TPC-H"""
    keys = {
        "customer": "c_custkey",
        "lineitem": "l_orderkey,l_linenumber", 
        "nation": "n_nationkey",
        "orders": "o_orderkey",
        "part": "p_partkey",
        "partsupp": "ps_partkey,ps_suppkey",
        "region": "r_regionkey",
        "supplier": "s_suppkey"
    }
    return keys.get(table_name, f"{table_name}_key")

def get_hudi_partition_key(table_name: str) -> str:
    """Retorna chave de partição para cada tabela (ou vazio se sem partição)"""
    # Para TPC-H, maioria das tabelas não precisa de partição
    # Apenas lineitem e orders podem se beneficiar de particionamento por data
    if table_name in ["lineitem", "orders"]:
        return ""  # Sem particionamento por simplicidade
    return ""

def get_hudi_precombine_key(table_name: str) -> str:
    """Retorna campo de precombinação (timestamp) para Hudi"""
    # Para TPC-H, usar campos de data quando disponíveis
    date_fields = {
        "lineitem": "l_shipdate",
        "orders": "o_orderdate"
    }
    return date_fields.get(table_name, get_hudi_record_key(table_name).split(",")[0])

def convert_to_silver_format(format_name: str, scale_factor: int = None, force_recreate: bool = None) -> Dict[str, Any]:
    """
    Converte dados bronze para um formato silver específico
    
    Args:
        format_name: Nome do formato (delta, iceberg, hudi)
        scale_factor: Scale factor (padrão: config.scale_factor)
        force_recreate: Forçar recriação mesmo se dados existirem
    
    Returns:
        Dict com resultados da conversão
    """
    logger = setup_logging()
    
    if scale_factor is None:
        scale_factor = config.scale_factor
    if force_recreate is None:
        force_recreate = config.force_recreate_silver
    
    print_summary_banner(f"CONVERSÃO BRONZE → SILVER ({format_name.upper()})", {
        "Scale Factor": scale_factor,
        "Formato Source": config.input_format,
        "Formato Target": format_name,
        "Force Recreate": force_recreate
    })
    
    # Validar dados bronze
    bronze_validation = validate_bronze_data(scale_factor)
    if not bronze_validation["all_tables_found"]:
        logger.error("❌ Dados bronze incompletos ou inexistentes")
        return {
            "status": "error",
            "error": "Bronze data validation failed",
            "bronze_validation": bronze_validation
        }
    
    # Verificar se dados silver já existem
    if not force_recreate:
        silver_validation = validate_silver_data(scale_factor, [format_name])
        if format_name in silver_validation.get("ready_formats", []):
            logger.info(f"✅ Dados silver {format_name} já existem, pulando conversão")
            return {
                "status": "skipped",
                "format": format_name,
                "scale_factor": scale_factor,
                "reason": "data_already_exists",
                "validation": silver_validation
            }
    
    try:
        with Timer(f"Conversão Silver {format_name}", logger) as timer:
            # Criar sessão Spark
            spark = create_spark_session(f"LHBench-Silver-{format_name}")
            logger.info("✅ Sessão Spark criada")
            
            # Tabelas TPC-H
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            # Caminhos
            bronze_base = f"s3a://{config.bronze_bucket}/sf_{scale_factor}/{config.input_format}"
            silver_base = f"s3a://{config.silver_bucket}/sf_{scale_factor}/{format_name}"
            
            conversion_results = []
            
            # Função de conversão baseada no formato
            converters = {
                "delta": convert_table_to_delta,
                "iceberg": convert_table_to_iceberg,
                "hudi": convert_table_to_hudi
            }
            
            converter_func = converters.get(format_name)
            if not converter_func:
                raise ValueError(f"Formato {format_name} não suportado")
            
            # Converter tabelas (sequencialmente para evitar conflitos de recursos)
            for table in tpch_tables:
                source_path = f"{bronze_base}/{table}"
                target_path = f"{silver_base}/{table}"
                
                logger.info(f"🔄 Convertendo {table} para {format_name}...")
                result = converter_func(spark, table, source_path, target_path, scale_factor)
                conversion_results.append(result)
                
                if result["status"] == "success":
                    logger.info(f"   ✅ {table}: {result['row_count']:,} linhas em {result['duration_seconds']:.2f}s")
                else:
                    logger.error(f"   ❌ {table}: {result.get('error', 'Erro desconhecido')}")
            
            # Parar sessão Spark
            spark.stop()
            logger.info("✅ Sessão Spark finalizada")
            
            # Verificar conversão
            logger.info("🔍 Verificando dados convertidos...")
            silver_validation = validate_silver_data(scale_factor, [format_name])
            
            successful_tables = [r for r in conversion_results if r["status"] == "success"]
            failed_tables = [r for r in conversion_results if r["status"] == "error"]
            
            results = {
                "status": "success" if len(failed_tables) == 0 else "partial",
                "format": format_name,
                "scale_factor": scale_factor,
                "tables_processed": len(tpch_tables),
                "tables_successful": len(successful_tables),
                "tables_failed": len(failed_tables),
                "conversion_results": conversion_results,
                "validation": silver_validation,
                "duration_seconds": timer.duration
            }
            
            # Salvar resultados
            local_file, s3_file = save_results(results, f"silver_conversion_{format_name}")
            logger.info(f"📊 Resultados salvos: {local_file}")
            
            return results
            
    except Exception as e:
        logger.error(f"❌ Erro na conversão {format_name}: {e}")
        return {
            "status": "error",
            "format": format_name,
            "error": str(e),
            "scale_factor": scale_factor
        }

def convert_all_silver_formats(scale_factor: int = None, formats: List[str] = None, force_recreate: bool = None) -> Dict[str, Any]:
    """
    Converte dados bronze para todos os formatos silver
    
    Args:
        scale_factor: Scale factor (padrão: config.scale_factor)
        formats: Lista de formatos (padrão: config.benchmark_formats)
        force_recreate: Forçar recriação mesmo se dados existirem
    
    Returns:
        Dict com resultados de todas as conversões
    """
    logger = setup_logging()
    
    if scale_factor is None:
        scale_factor = config.scale_factor
    if formats is None:
        formats = config.benchmark_formats
    if force_recreate is None:
        force_recreate = config.force_recreate_silver
    
    print_summary_banner("CONVERSÃO BRONZE → SILVER (TODOS OS FORMATOS)", {
        "Scale Factor": scale_factor,
        "Formatos": ", ".join(formats),
        "Force Recreate": force_recreate
    })
    
    all_results = {}
    
    try:
        with Timer("Conversão Silver Completa", logger) as timer:
            # Converter cada formato sequencialmente
            for format_name in formats:
                logger.info(f"\n🔄 Iniciando conversão para {format_name.upper()}")
                result = convert_to_silver_format(format_name, scale_factor, force_recreate)
                all_results[format_name] = result
                
                if result["status"] == "success":
                    logger.info(f"✅ Conversão {format_name} concluída")
                elif result["status"] == "skipped":
                    logger.info(f"⏭️ Conversão {format_name} pulada")
                else:
                    logger.error(f"❌ Conversão {format_name} falhou")
            
            # Resumo final
            successful_formats = [f for f, r in all_results.items() if r["status"] == "success"]
            skipped_formats = [f for f, r in all_results.items() if r["status"] == "skipped"]
            failed_formats = [f for f, r in all_results.items() if r["status"] == "error"]
            
            summary = {
                "status": "success" if len(failed_formats) == 0 else "partial",
                "scale_factor": scale_factor,
                "formats_requested": formats,
                "formats_successful": successful_formats,
                "formats_skipped": skipped_formats,
                "formats_failed": failed_formats,
                "individual_results": all_results,
                "duration_seconds": timer.duration
            }
            
            # Salvar resumo
            local_file, s3_file = save_results(summary, "silver_conversion_all")
            logger.info(f"📊 Resumo salvo: {local_file}")
            
            return summary
            
    except Exception as e:
        logger.error(f"❌ Erro na conversão silver: {e}")
        return {
            "status": "error",
            "error": str(e),
            "scale_factor": scale_factor,
            "formats_requested": formats,
            "individual_results": all_results
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
            results = convert_all_silver_formats()
            
            if results["status"] == "success":
                print("\n🎉 Conversão Silver completa!")
                print(f"📊 Formatos convertidos: {', '.join(results['formats_successful'])}")
                if results['formats_skipped']:
                    print(f"⏭️ Formatos pulados: {', '.join(results['formats_skipped'])}")
                print(f"⏱️  Tempo total: {results['duration_seconds']:.2f}s")
                return 0
            else:
                print(f"\n⚠️ Conversão parcial:")
                if results.get('formats_successful'):
                    print(f"✅ Sucessos: {', '.join(results['formats_successful'])}")
                if results.get('formats_failed'):
                    print(f"❌ Falhas: {', '.join(results['formats_failed'])}")
                return 1
        else:
            results = convert_to_silver_format(format_name)
            
            if results["status"] == "success":
                print(f"\n🎉 Conversão {format_name} concluída!")
                print(f"📊 Tabelas convertidas: {results['tables_successful']}/{results['tables_processed']}")
                print(f"⏱️  Tempo total: {results['duration_seconds']:.2f}s")
                return 0
            elif results["status"] == "skipped":
                print(f"\n⏭️ Conversão {format_name} pulada - dados já existem")
                return 0
            else:
                print(f"\n❌ Erro na conversão: {results.get('error', 'Erro desconhecido')}")
                return 1
                
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário")
        return 1
    except Exception as e:
        print(f"\n💥 Erro fatal: {e}")
        return 1

if __name__ == "__main__":
    exit(main())