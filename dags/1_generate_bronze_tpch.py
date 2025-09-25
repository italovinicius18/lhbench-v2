"""
DAG para geração de dados TPC-H usando DuckDB com extensão nativa
Gera dados diretamente no MinIO via S3 usando DuckDB + TPC-H extension
Muito mais eficiente que geradores externos!
"""

from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import os

# Configurações TPC-H
SCALE_FACTOR = 1  # Comece com SF=1 (cerca de 1GB)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BRONZE_BUCKET = "bronze"


@dag(
    dag_id="1_generate_bronze_tpch",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Execução manual
    catchup=False,
    tags=["tpch", "bronze", "duckdb", "minio", "s3"],
    max_active_runs=1,
    params={
        "scale_factor": 10,
        "force_regenerate": False,
        "parallel_children": 5,
        "output_format": "csv"  # csv ou parquet
    },
)
def generate_tpch_bronze_direct():

    @task
    def check_existing_data(**context):
        """Verifica se os dados TPC-H já existem no S3"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", SCALE_FACTOR)
        force_regenerate = params.get("force_regenerate", False)
        output_format = params.get("output_format", "parquet")

        print(f"🔍 Verificando dados existentes para Scale Factor {scale_factor}")
        
        s3_path = f"sf_{scale_factor}"
        
        if force_regenerate:
            print("🔄 Forçando regeneração dos dados")
            return {
                "scale_factor": scale_factor,
                "needs_generation": True,
                "s3_path": s3_path,
                "output_format": output_format
            }
        
        # Verificar se tabelas existem no S3
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Tabelas TPC-H esperadas
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            tables_found = []
            
            for table in tpch_tables:
                table_prefix = f"{s3_path}/{table}/"
                try:
                    response = s3_client.list_objects_v2(
                        Bucket=BRONZE_BUCKET,
                        Prefix=table_prefix,
                        MaxKeys=1
                    )
                    if response.get('Contents'):
                        tables_found.append(table)
                        print(f"   ✅ {table}: dados encontrados")
                    else:
                        print(f"   ❌ {table}: sem dados")
                except ClientError:
                    print(f"   ❌ {table}: erro ao verificar")
            
            if len(tables_found) == len(tpch_tables):
                print(f"✅ Todas as tabelas TPC-H SF {scale_factor} já existem no S3")
                return {
                    "scale_factor": scale_factor,
                    "needs_generation": False,
                    "s3_path": s3_path,
                    "output_format": output_format,
                    "existing_tables": tables_found
                }
            else:
                print(f"📋 Encontradas {len(tables_found)}/{len(tpch_tables)} tabelas. Geração necessária.")
                return {
                    "scale_factor": scale_factor,
                    "needs_generation": True,
                    "s3_path": s3_path,
                    "output_format": output_format
                }
                
        except Exception as e:
            print(f"❌ Erro ao verificar dados existentes: {e}")
            # Em caso de erro, assumir que precisa gerar
            return {
                "scale_factor": scale_factor,
                "needs_generation": True,
                "s3_path": s3_path,
                "output_format": output_format
            }

    @task
    def generate_tpch_data_duckdb(check_result: dict, **context):
        """Gera dados TPC-H usando DuckDB e salva diretamente no S3/MinIO"""

        if not check_result["needs_generation"]:
            print("⏭️ Pulando geração - dados já existem")
            return {"skipped": True}

        scale_factor = check_result["scale_factor"]
        s3_path = check_result["s3_path"]
        output_format = check_result["output_format"]
        
        params = context["params"]
        parallel_children = params.get("parallel_children", 4)

        batch_timestamp = datetime.now()
        batch_id = batch_timestamp.strftime("%Y%m%d_%H%M%S")

        print(f"🚀 Iniciando geração TPC-H com DuckDB")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📅 Batch ID: {batch_id}")
        print(f"📂 Destino S3: s3://{BRONZE_BUCKET}/{s3_path}/")
        print(f"📄 Formato: {output_format}")
        print(f"🔄 Paralelismo: {parallel_children} partições")

        try:
            import duckdb
            
            # Criar conexão DuckDB
            conn = duckdb.connect(':memory:')
            print("✅ Conexão DuckDB estabelecida")
            
            # Instalar e carregar extensão TPC-H
            conn.execute("INSTALL tpch;")
            conn.execute("LOAD tpch;")
            print("✅ Extensão TPC-H carregada")
            
            # Configurar S3 settings para MinIO
            conn.execute(f"""
                SET s3_endpoint = '{MINIO_ENDPOINT.replace('http://', '')}';
                SET s3_access_key_id = '{MINIO_ACCESS_KEY}';
                SET s3_secret_access_key = '{MINIO_SECRET_KEY}';
                SET s3_use_ssl = false;
                SET s3_url_style = 'path';
            """)
            print("✅ Configuração S3 definida")
            
            # Tabelas TPC-H
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            generation_results = []
            
            # Gerar dados em paralelo usando children/step approach
            if parallel_children > 1:
                print(f"🔄 Gerando dados em {parallel_children} partições paralelas...")
                
                for step in range(parallel_children):
                    print(f"📦 Gerando partição {step + 1}/{parallel_children}")
                    
                    # Gerar dados para esta partição
                    conn.execute(f"""
                        CALL dbgen(
                            sf = {scale_factor}, 
                            children = {parallel_children}, 
                            step = {step}
                        );
                    """)
                    
                    # Exportar cada tabela para S3
                    for table in tpch_tables:
                        if output_format.lower() == "parquet":
                            s3_url = f"s3://{BRONZE_BUCKET}/{s3_path}/{table}/part_{step:03d}.parquet"
                            conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT PARQUET);")
                        else:  # CSV
                            s3_url = f"s3://{BRONZE_BUCKET}/{s3_path}/{table}/part_{step:03d}.csv"
                            conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT CSV, HEADER);")
                        
                        print(f"   ✅ {table} partição {step} → {s3_url}")
                    
                    # Limpar tabelas para próxima partição
                    for table in tpch_tables:
                        conn.execute(f"DROP TABLE IF EXISTS {table};")
                        
            else:
                print("📦 Gerando dados em partição única...")
                
                # Gerar dados
                conn.execute(f"CALL dbgen(sf = {scale_factor});")
                print("✅ Dados TPC-H gerados na memória")
                
                # Exportar cada tabela para S3
                for table in tpch_tables:
                    if output_format.lower() == "parquet":
                        s3_url = f"s3://{BRONZE_BUCKET}/{s3_path}/{table}/{table}.parquet"
                        conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT PARQUET);")
                    else:  # CSV
                        s3_url = f"s3://{BRONZE_BUCKET}/{s3_path}/{table}/{table}.csv"
                        conn.execute(f"COPY {table} TO '{s3_url}' (FORMAT CSV, HEADER);")
                    
                    # Obter estatísticas da tabela
                    result = conn.execute(f"SELECT COUNT(*) as rows FROM {table};").fetchone()
                    row_count = result[0] if result else 0
                    
                    generation_results.append({
                        "table": table,
                        "s3_url": s3_url,
                        "row_count": row_count
                    })
                    
                    print(f"   ✅ {table}: {row_count:,} linhas → {s3_url}")
            
            # Fechar conexão
            conn.close()
            
            print(f"🎉 Geração TPC-H concluída com sucesso!")
            print(f"📊 Estatísticas:")
            print(f"   - Scale Factor: {scale_factor}")
            print(f"   - Tabelas: {len(tpch_tables)}")
            print(f"   - Formato: {output_format}")
            print(f"   - Localização: s3://{BRONZE_BUCKET}/{s3_path}/")
            print(f"   - Batch ID: {batch_id}")

            return {
                "batch_id": batch_id,
                "scale_factor": scale_factor,
                "tables_generated": len(tpch_tables),
                "s3_path": s3_path,
                "output_format": output_format,
                "generation_results": generation_results,
                "skipped": False,
            }
            
        except Exception as e:
            print(f"❌ Erro na geração TPC-H: {e}")
            raise

    @task
    def verify_s3_data(generation_result: dict):
        """Verifica se os dados foram salvos corretamente no S3"""

        if generation_result.get("skipped"):
            print("⏭️ Verificação pulada - nenhum dado foi gerado")
            return generation_result

        scale_factor = generation_result["scale_factor"]
        s3_path = generation_result["s3_path"]
        output_format = generation_result["output_format"]

        print(f"🔍 Verificando dados TPC-H no S3")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📂 Caminho S3: s3://{BRONZE_BUCKET}/{s3_path}/")
        print(f"📄 Formato: {output_format}")

        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Tabelas TPC-H esperadas
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            verification_results = []
            total_objects = 0
            total_size_bytes = 0

            for table in tpch_tables:
                table_prefix = f"{s3_path}/{table}/"
                
                try:
                    # Listar objetos da tabela
                    response = s3_client.list_objects_v2(
                        Bucket=BRONZE_BUCKET,
                        Prefix=table_prefix
                    )
                    
                    if 'Contents' in response:
                        objects = response['Contents']
                        table_objects = len(objects)
                        table_size = sum(obj['Size'] for obj in objects)
                        
                        total_objects += table_objects
                        total_size_bytes += table_size
                        
                        verification_results.append({
                            "table": table,
                            "objects": table_objects,
                            "size_mb": table_size / (1024 * 1024),
                            "status": "✅ OK"
                        })
                        
                        print(f"   ✅ {table}: {table_objects} arquivos, {table_size / (1024 * 1024):.2f} MB")
                    else:
                        verification_results.append({
                            "table": table,
                            "status": "❌ Sem dados"
                        })
                        print(f"   ❌ {table}: nenhum arquivo encontrado")
                        
                except ClientError as e:
                    verification_results.append({
                        "table": table,
                        "status": f"❌ Erro: {e}"
                    })
                    print(f"   ❌ {table}: erro ao verificar - {e}")

            # Resumo da verificação
            successful_tables = len([r for r in verification_results if r["status"] == "✅ OK"])
            
            print(f"\n📊 Resumo da Verificação:")
            print(f"   - Tabelas verificadas: {successful_tables}/{len(tpch_tables)}")
            print(f"   - Objetos totais: {total_objects}")
            print(f"   - Tamanho total: {total_size_bytes / (1024 * 1024):.2f} MB")
            print(f"   - Localização: s3://{BRONZE_BUCKET}/{s3_path}/")

            if successful_tables == len(tpch_tables):
                print("🎉 Todas as tabelas TPC-H foram salvas no S3 com sucesso!")
                return {
                    **generation_result,
                    "verification_status": "success",
                    "tables_verified": successful_tables,
                    "total_objects": total_objects,
                    "total_size_mb": total_size_bytes / (1024 * 1024)
                }
            else:
                print("⚠️ Algumas tabelas não foram encontradas no S3")
                return {
                    **generation_result,
                    "verification_status": "partial",
                    "tables_verified": successful_tables,
                    "total_objects": total_objects,
                    "total_size_mb": total_size_bytes / (1024 * 1024)
                }
                
        except Exception as e:
            print(f"❌ Erro na verificação S3: {e}")
            return {
                **generation_result,
                "verification_status": "error",
                "error": str(e)
            }

    @task
    def create_summary_report(verification_result: dict):
        """Cria relatório final de geração"""
        
        if verification_result.get("skipped"):
            print("⏭️ Relatório pulado - dados não foram gerados")
            return verification_result
        
        print("� Gerando relatório final...")
        
        scale_factor = verification_result["scale_factor"]
        s3_path = verification_result["s3_path"]
        output_format = verification_result["output_format"]
        
        print(f"\n🎯 RELATÓRIO TPC-H - GERAÇÃO BRONZE")
        print(f"=" * 50)
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📅 Data/Hora: {datetime.now()}")
        print(f"🏷️ Batch ID: {verification_result['batch_id']}")
        print(f"📂 Localização S3: s3://{BRONZE_BUCKET}/{s3_path}/")
        print(f"� Formato: {output_format}")
        print(f"")
        print(f"📋 Tabelas TPC-H:")
        print(f"   customer, lineitem, nation, orders")
        print(f"   part, partsupp, region, supplier")
        print(f"")
        print(f"📊 Estatísticas:")
        
        if verification_result.get("verification_status") == "success":
            print(f"   ✅ Status: SUCESSO")
            print(f"   📁 Tabelas: {verification_result.get('tables_verified', 0)}/8")
            print(f"   📄 Arquivos: {verification_result.get('total_objects', 0)}")
            print(f"   � Tamanho: {verification_result.get('total_size_mb', 0):.2f} MB")
        else:
            print(f"   ⚠️ Status: {verification_result.get('verification_status', 'UNKNOWN')}")
        
        print(f"")
        print(f"🔗 Acesso via MinIO Console: http://localhost:9001")
        print(f"🔗 Bucket Bronze: {BRONZE_BUCKET}")
        print(f"=" * 50)
        
        return verification_result

    # Pipeline de execução otimizado
    check_task = check_existing_data()
    generate_task = generate_tpch_data_duckdb(check_task)
    verify_task = verify_s3_data(generate_task)
    summary_task = create_summary_report(verify_task)

    # Dependências do pipeline
    check_task >> generate_task >> verify_task >> summary_task


generate_tpch_bronze_direct()