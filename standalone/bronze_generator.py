"""
Gerador de dados TPC-H Bronze usando DuckDB
Versão standalone sem dependência do Airflow
"""
import os
import logging
from datetime import datetime
from typing import Dict, Any

# Config será passado como parâmetro
from utils import setup_logging, save_results, validate_bronze_data, Timer, print_summary_banner

def generate_bronze_data(scale_factor: int = None, force_regenerate: bool = None) -> Dict[str, Any]:
    """
    Gera dados TPC-H Bronze usando DuckDB
    
    Args:
        scale_factor: Scale factor para TPC-H (padrão: config.scale_factor)
        force_regenerate: Forçar regeneração mesmo se dados existirem
    
    Returns:
        Dict com resultados da geração
    """
    logger = setup_logging()
    
    if scale_factor is None:
        scale_factor = config.scale_factor
    if force_regenerate is None:
        force_regenerate = config.force_regenerate_bronze
    
    print_summary_banner("GERAÇÃO DADOS TPC-H BRONZE", {
        "Scale Factor": scale_factor,
        "Formato": config.input_format,
        "Force Regenerate": force_regenerate,
        "MinIO Endpoint": config.minio_endpoint
    })
    
    # Verificar se dados já existem
    if not force_regenerate:
        validation = validate_bronze_data(scale_factor)
        if validation["all_tables_found"]:
            logger.info("✅ Dados TPC-H já existem, pulando geração")
            return {
                "status": "skipped",
                "scale_factor": scale_factor,
                "reason": "data_already_exists",
                "validation": validation
            }
    
    try:
        with Timer("Geração TPC-H Bronze", logger) as timer:
            # Importar DuckDB
            try:
                import duckdb
            except ImportError:
                raise ImportError("DuckDB não está instalado. Execute: pip install duckdb")
            
            # Criar conexão DuckDB
            conn = duckdb.connect(':memory:')
            logger.info("✅ Conexão DuckDB estabelecida")
            
            # Instalar e carregar extensão TPC-H
            conn.execute("INSTALL tpch;")
            conn.execute("LOAD tpch;")
            logger.info("✅ Extensão TPC-H carregada")
            
            # Configurar S3 para MinIO
            minio_host = config.minio_endpoint.replace('http://', '').replace('https://', '')
            conn.execute(f"""
                SET s3_endpoint = '{minio_host}';
                SET s3_access_key_id = '{config.minio_access_key}';
                SET s3_secret_access_key = '{config.minio_secret_key}';
                SET s3_use_ssl = false;
                SET s3_url_style = 'path';
            """)
            logger.info("✅ Configuração S3/MinIO definida")
            
            # Gerar dados TPC-H
            logger.info(f"🔄 Gerando dados TPC-H Scale Factor {scale_factor}...")
            conn.execute(f"CALL dbgen(sf = {scale_factor});")
            logger.info("✅ Dados TPC-H gerados na memória")
            
            # Tabelas TPC-H
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            generation_results = []
            
            # Caminho base no S3
            s3_base_path = f"sf_{scale_factor}/{config.input_format}"
            
            # Exportar cada tabela para S3
            for table in tpch_tables:
                logger.info(f"📤 Exportando tabela {table}...")
                
                if config.input_format.lower() == "parquet":
                    s3_url = f"s3://{config.bronze_bucket}/{s3_base_path}/{table}/{table}.parquet"
                    conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT PARQUET);")
                else:  # CSV
                    s3_url = f"s3://{config.bronze_bucket}/{s3_base_path}/{table}/{table}.csv"
                    conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT CSV, HEADER);")
                
                # Contar linhas
                result = conn.execute(f"SELECT COUNT(*) as rows FROM {table};").fetchone()
                row_count = result[0] if result else 0
                
                generation_results.append({
                    "table": table,
                    "s3_url": s3_url,
                    "row_count": row_count
                })
                
                logger.info(f"   ✅ {table}: {row_count:,} linhas → {s3_url}")
            
            # Fechar conexão
            conn.close()
            
            # Verificar dados gerados
            logger.info("🔍 Verificando dados gerados...")
            validation = validate_bronze_data(scale_factor)
            
            results = {
                "status": "success",
                "scale_factor": scale_factor,
                "input_format": config.input_format,
                "tables_generated": len(tpch_tables),
                "s3_base_path": s3_base_path,
                "generation_results": generation_results,
                "validation": validation,
                "duration_seconds": timer.duration
            }
            
            # Salvar resultados
            local_file, s3_file = save_results(results, "bronze_generation")
            logger.info(f"📊 Resultados salvos: {local_file}")
            
            return results
            
    except Exception as e:
        logger.error(f"❌ Erro na geração TPC-H: {e}")
        return {
            "status": "error",
            "error": str(e),
            "scale_factor": scale_factor
        }

def main():
    """Função principal para execução standalone"""
    try:
        results = generate_bronze_data()
        
        if results["status"] == "success":
            print("\n🎉 Geração Bronze concluída com sucesso!")
            print(f"📊 Scale Factor: {results['scale_factor']}")
            print(f"📁 Tabelas geradas: {results['tables_generated']}")
            print(f"⏱️  Tempo total: {results['duration_seconds']:.2f}s")
            return 0
        elif results["status"] == "skipped":
            print("\n⏭️ Geração Bronze pulada - dados já existem")
            return 0
        else:
            print(f"\n❌ Erro na geração: {results.get('error', 'Erro desconhecido')}")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário")
        return 1
    except Exception as e:
        print(f"\n💥 Erro fatal: {e}")
        return 1

if __name__ == "__main__":
    exit(main())