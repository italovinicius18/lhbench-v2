from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp, coalesce
from airflow.models import Variable

@dag(
    dag_id="2_bronze_to_silver_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Execução manual ou após o DAG 1
    catchup=False,
    tags=["tpch", "iceberg", "silver", "bronze-to-silver"],
    max_active_runs=1,
    params={
        "scale_factor": 10,
        "input_format": "csv",  # parquet ou csv (deve coincidir com output_format do DAG 1)
        "force_recreate": False,
        "validate_data": True
    },
)
def bronze_to_silver_iceberg():

    @task
    def validate_bronze_data(**context):
        """Valida se os dados bronze existem e estão no formato correto"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 1)
        input_format = params.get("input_format", "parquet")
        
        print(f"🔍 Validando dados bronze TPC-H")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📄 Formato esperado: {input_format}")
        
        # Caminho dos dados bronze (baseado na estrutura do DAG 1)
        bronze_base_path = f"sf_{scale_factor}"
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=Variable.get("MINIO_ENDPOINT"),
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
            )
            
            # Tabelas TPC-H esperadas (baseado no DAG 1)
            tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            validation_results = {}
            
            for table in tpch_tables:
                table_prefix = f"{bronze_base_path}/{table}/"
                
                try:
                    response = s3_client.list_objects_v2(
                        Bucket="bronze",
                        Prefix=table_prefix,
                        MaxKeys=10
                    )
                    
                    if 'Contents' in response and len(response['Contents']) > 0:
                        files = response['Contents']
                        total_size = sum(obj['Size'] for obj in files)
                        file_count = len(files)
                        
                        # Verificar extensão dos arquivos
                        sample_file = files[0]['Key']
                        detected_format = None
                        if sample_file.endswith('.parquet'):
                            detected_format = 'parquet'
                        elif sample_file.endswith('.csv'):
                            detected_format = 'csv'
                        
                        validation_results[table] = {
                            "status": "found",
                            "file_count": file_count,
                            "total_size_mb": total_size / (1024 * 1024),
                            "detected_format": detected_format,
                            "sample_file": sample_file,
                            "full_path": f"s3a://bronze/{table_prefix}"
                        }
                        
                        print(f"   ✅ {table}: {file_count} arquivos, {total_size/(1024*1024):.2f} MB ({detected_format})")
                        
                        # Validar formato
                        if detected_format != input_format:
                            print(f"      ⚠️  Formato detectado ({detected_format}) != esperado ({input_format})")
                        
                    else:
                        validation_results[table] = {
                            "status": "not_found",
                            "full_path": f"s3a://bronze/{table_prefix}"
                        }
                        print(f"   ❌ {table}: dados não encontrados")
                        
                except ClientError as e:
                    validation_results[table] = {
                        "status": "error",
                        "error": str(e),
                        "full_path": f"s3a://bronze/{table_prefix}"
                    }
                    print(f"   ❌ {table}: erro ao acessar - {str(e)}")
            
            # Verificar se todas as tabelas foram encontradas
            found_tables = [t for t, r in validation_results.items() if r["status"] == "found"]
            
            if len(found_tables) == len(tpch_tables):
                print(f"✅ Todas as {len(tpch_tables)} tabelas TPC-H encontradas no bronze")
                return {
                    "validation_status": "success",
                    "bronze_path": bronze_base_path,
                    "scale_factor": scale_factor,
                    "input_format": input_format,
                    "found_tables": found_tables,
                    "validation_results": validation_results
                }
            else:
                print(f"⚠️ Apenas {len(found_tables)}/{len(tpch_tables)} tabelas encontradas")
                return {
                    "validation_status": "partial",
                    "bronze_path": bronze_base_path,
                    "scale_factor": scale_factor,
                    "input_format": input_format,
                    "found_tables": found_tables,
                    "validation_results": validation_results
                }
                
        except Exception as e:
            print(f"❌ Erro na validação: {str(e)}")
            return {
                "validation_status": "error",
                "error": str(e),
                "scale_factor": scale_factor,
                "input_format": input_format
            }

    @task.pyspark(
        conn_id="spark_default",
        config_kwargs={
            # Apache Iceberg jars
            "spark.jars.packages": ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            # Iceberg extensions
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            
            # Catálogo Iceberg usando Hadoop
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            # warehouse será configurado dinamicamente na task
            
            # S3A / MinIO
            "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Configurações de performance para TPC-H
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            
            # Iceberg specific configurations
            "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
        }
    )
    def process_bronze_to_silver_iceberg(validation_result: dict, spark: SparkSession = None, sc: SparkContext = None, **context):
        """Converte dados TPC-H do bronze para silver usando Apache Iceberg"""
        
        if validation_result["validation_status"] == "error":
            print("❌ Erro na validação - abortando processamento")
            raise Exception(f"Validation failed: {validation_result.get('error', 'Unknown error')}")
        
        if not validation_result.get("found_tables"):
            print("❌ Nenhuma tabela encontrada - abortando processamento")
            raise Exception("No tables found in bronze layer")
            
        print("🚀 Iniciando conversão Bronze → Silver Iceberg")
        
        params = context["params"]
        force_recreate = params.get("force_recreate", False)
        
        bronze_path = validation_result["bronze_path"]
        scale_factor = validation_result["scale_factor"]
        input_format = validation_result["input_format"]
        found_tables = validation_result["found_tables"]
        
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📂 Bronze Path: s3a://bronze/{bronze_path}/")
        print(f"📄 Input Format: {input_format}")
        print(f"🔄 Force Recreate: {force_recreate}")
        
        # Configurar warehouse dinâmico para seguir padrão: silver/sf_X/iceberg/
        iceberg_warehouse = f"s3a://silver/sf_{scale_factor}/"
        spark.conf.set("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
        print(f"🏠 Warehouse Iceberg: {iceberg_warehouse}")
        
        # Criar namespace (database) se não existir
        namespace = f"iceberg"
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
            print(f"✅ Namespace iceberg.{namespace} criado/verificado")
        except Exception as e:
            print(f"⚠️ Aviso ao criar namespace: {str(e)}")
        
        # Definir configurações específicas das tabelas TPC-H
        table_configs = {
            "customer": {
                "key_cols": ["c_custkey"],
                "partition_cols": None,
                "description": "Clientes TPC-H"
            },
            "supplier": {
                "key_cols": ["s_suppkey"],
                "partition_cols": None,
                "description": "Fornecedores TPC-H"
            },
            "part": {
                "key_cols": ["p_partkey"],
                "partition_cols": None,
                "description": "Peças TPC-H"
            },
            "partsupp": {
                "key_cols": ["ps_partkey", "ps_suppkey"],
                "partition_cols": None,
                "description": "Fornecedores de Peças TPC-H"
            },
            "orders": {
                "key_cols": ["o_orderkey"],
                "partition_cols": ["o_orderstatus"],  # Particionar por status para otimizar queries
                "description": "Pedidos TPC-H"
            },
            "lineitem": {
                "key_cols": ["l_orderkey", "l_linenumber"],
                "partition_cols": ["l_shipmode"],  # Particionar por modo de envio
                "description": "Itens de Pedidos TPC-H"
            },
            "nation": {
                "key_cols": ["n_nationkey"],
                "partition_cols": None,
                "description": "Nações TPC-H (dimensão)"
            },
            "region": {
                "key_cols": ["r_regionkey"],
                "partition_cols": None,
                "description": "Regiões TPC-H (dimensão)"
            }
        }
        
        processing_results = []
        
        # Processar cada tabela encontrada
        for table_name in found_tables:
            if table_name not in table_configs:
                print(f"⚠️ Configuração não encontrada para tabela {table_name} - pulando")
                continue
                
            config = table_configs[table_name]
            key_cols = config["key_cols"]
            partition_cols = config.get("partition_cols")
            description = config["description"]
            
            print(f"\n📊 Processando {description} ({table_name})")
            
            try:
                # 1) Ler dados do bronze
                bronze_table_path = f"s3a://bronze/{bronze_path}/{table_name}/*"
                
                print(f"   📥 Lendo de: {bronze_table_path}")
                
                try:
                    if input_format.lower() == "parquet":
                        df_bronze = spark.read.parquet(bronze_table_path)
                    else:  # CSV
                        df_bronze = (
                            spark.read
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .csv(bronze_table_path)
                        )
                    
                    total_records = df_bronze.count()
                    if total_records == 0:
                        print(f"   ⚠️ Nenhum registro encontrado para {table_name}")
                        continue
                        
                    print(f"   📊 Registros encontrados: {total_records:,}")
                    
                    # Mostrar schema da tabela
                    print(f"   📋 Schema:")
                    for field in df_bronze.schema.fields[:5]:  # Primeiros 5 campos
                        print(f"      - {field.name}: {field.dataType}")
                    if len(df_bronze.schema.fields) > 5:
                        print(f"      ... e mais {len(df_bronze.schema.fields) - 5} campos")
                    
                except Exception as e:
                    print(f"   ❌ Erro ao ler bronze para {table_name}: {str(e)}")
                    continue
                
                # 2) Preparar dados para silver
                df_silver = df_bronze.withColumn("silver_processed_at", current_timestamp())
                
                # Remover duplicatas por chave primária
                df_silver = df_silver.dropDuplicates(key_cols)
                unique_records = df_silver.count()
                
                print(f"   🔄 Registros únicos após deduplicação: {unique_records:,}")
                
                if unique_records != total_records:
                    duplicates_removed = total_records - unique_records
                    print(f"   🧹 Duplicatas removidas: {duplicates_removed:,}")
                
                # 3) Definir tabela Iceberg no silver
                # Estrutura: iceberg.sf_{scale_factor}.{table_name}
                # O catálogo Hadoop gerencia automaticamente a localização
                iceberg_table = f"iceberg.{namespace}.{table_name}"
                
                print(f"   💾 Salvando como: {iceberg_table}")
                print(f"   📍 Catálogo gerenciará a localização automaticamente")
                
                # 4) Verificar se deve recriar ou fazer merge
                table_exists = False
                try:
                    # Verificar se tabela existe
                    spark.sql(f"DESCRIBE TABLE {iceberg_table}")
                    table_exists = True
                    print(f"   📋 Tabela Iceberg {iceberg_table} já existe")
                except Exception:
                    table_exists = False
                    print(f"   🆕 Tabela Iceberg {iceberg_table} será criada")
                
                if force_recreate or not table_exists:
                    print(f"   🛠️ {'Recriando' if force_recreate else 'Criando'} tabela Iceberg")
                    
                    # Deletar tabela se for para recriar
                    if force_recreate and table_exists:
                        try:
                            spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
                            print(f"   🗑️ Tabela anterior removida")
                        except Exception as e:
                            print(f"   ⚠️ Aviso ao remover tabela: {str(e)}")
                    
                    # Criar nova tabela Iceberg
                    writer = df_silver.write.format("iceberg").mode("overwrite")
                    
                    # Adicionar particionamento se especificado
                    if partition_cols:
                        # Para Iceberg, particionamento é definido na criação da tabela
                        print(f"   📁 Particionando por: {', '.join(partition_cols)}")
                        
                        # Criar tabela com particionamento
                        df_silver.createOrReplaceTempView("temp_table")
                        
                        # Construir DDL com particionamento - sem LOCATION para catálogo Hadoop
                        partition_spec = ", ".join([f"{col}" for col in partition_cols])
                        create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {iceberg_table}
                        USING iceberg
                        PARTITIONED BY ({partition_spec})
                        AS SELECT * FROM temp_table
                        """
                        
                        spark.sql(create_sql)
                        spark.catalog.dropTempView("temp_table")
                    else:
                        # Criar sem particionamento - usar apenas saveAsTable para catálogo Hadoop
                        writer.saveAsTable(iceberg_table)
                    
                    final_count = unique_records
                    
                else:
                    print(f"   🔄 Fazendo MERGE na tabela Iceberg existente")
                    
                    # Criar view temporária para MERGE
                    df_silver.createOrReplaceTempView("updates")
                    
                    # Preparar condição de merge
                    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in key_cols])
                    
                    # Executar MERGE usando SQL
                    merge_sql = f"""
                    MERGE INTO {iceberg_table} AS target
                    USING updates AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """
                    
                    spark.sql(merge_sql)
                    
                    # Limpar view temporária
                    spark.catalog.dropTempView("updates")
                    
                    # Contar registros após merge
                    final_count = spark.sql(f"SELECT COUNT(*) FROM {iceberg_table}").collect()[0][0]
                
                print(f"   ✅ Iceberg criado: {final_count:,} registros")
                
                # 5) Executar comandos de manutenção Iceberg
                try:
                    print(f"   🔧 Executando manutenção Iceberg...")
                    
                    # Compact files (equivalente ao OPTIMIZE do Delta)
                    spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{iceberg_table}')")
                    print(f"   ✅ Compactação de arquivos concluída")
                    
                    # Remove orphan files (limpeza)
                    spark.sql(f"CALL iceberg.system.remove_orphan_files(table => '{iceberg_table}')")
                    print(f"   🧹 Limpeza de arquivos órfãos concluída")
                    
                except Exception as opt_e:
                    print(f"   ⚠️ Falha na manutenção Iceberg: {str(opt_e)}")
                
                # 6) Coletar estatísticas da tabela
                try:
                    # Obter metadados da tabela
                    history_df = spark.sql(f"SELECT * FROM {iceberg_table}.history ORDER BY made_current_at DESC LIMIT 1")
                    last_operation = "unknown"
                    
                    if history_df.count() > 0:
                        last_operation = history_df.collect()[0]["operation"]
                    
                    processing_results.append({
                        "table_name": table_name,
                        "bronze_records": total_records,
                        "silver_records": final_count,
                        "duplicates_removed": total_records - unique_records,
                        "iceberg_table": iceberg_table,
                        "last_operation": last_operation,
                        "partitioned_by": partition_cols,
                        "status": "success"
                    })
                    
                except Exception as e:
                    processing_results.append({
                        "table_name": table_name,
                        "bronze_records": total_records,
                        "silver_records": final_count,
                        "iceberg_table": iceberg_table,
                        "status": "success_with_warnings",
                        "warning": str(e)
                    })
                
                print(f"   ✅ {table_name} processado com sucesso")
                
            except Exception as e:
                print(f"   ❌ Erro ao processar {table_name}: {str(e)}")
                processing_results.append({
                    "table_name": table_name,
                    "status": "error",
                    "error": str(e)
                })
                continue
        
        # Relatório final
        print(f"\n🎉 Conversão Bronze → Silver Iceberg concluída!")
        
        successful_tables = [r for r in processing_results if r["status"] == "success"]
        error_tables = [r for r in processing_results if r["status"] == "error"]
        
        print(f"\n📊 RELATÓRIO FINAL:")
        print(f"   ✅ Tabelas processadas com sucesso: {len(successful_tables)}")
        print(f"   ❌ Tabelas com erro: {len(error_tables)}")
        
        total_bronze_records = sum(r.get("bronze_records", 0) for r in successful_tables)
        total_silver_records = sum(r.get("silver_records", 0) for r in successful_tables)
        total_duplicates = sum(r.get("duplicates_removed", 0) for r in successful_tables)
        
        print(f"   📊 Total registros bronze: {total_bronze_records:,}")
        print(f"   📊 Total registros silver: {total_silver_records:,}")
        print(f"   🧹 Total duplicatas removidas: {total_duplicates:,}")
        
        print(f"\n📋 Detalhes por tabela:")
        for result in successful_tables:
            table_name = result["table_name"]
            bronze_count = result["bronze_records"]
            silver_count = result["silver_records"]
            iceberg_table = result.get("iceberg_table", "N/A")
            print(f"   📈 {table_name}: {bronze_count:,} → {silver_count:,} ({iceberg_table})")
        
        if error_tables:
            print(f"\n❌ Tabelas com erro:")
            for result in error_tables:
                print(f"   - {result['table_name']}: {result['error']}")
        
        return {
            "processing_results": processing_results,
            "successful_tables": len(successful_tables),
            "error_tables": len(error_tables),
            "total_bronze_records": total_bronze_records,
            "total_silver_records": total_silver_records,
            "total_duplicates_removed": total_duplicates
        }

    @task
    def generate_data_quality_report(processing_result: dict, **context):
        """Gera relatório de qualidade dos dados convertidos para Iceberg"""
        
        print("📊 Gerando relatório de qualidade dos dados...")
        
        successful_tables = [r for r in processing_result["processing_results"] if r["status"] == "success"]
        
        if not successful_tables:
            print("⚠️ Nenhuma tabela foi processada com sucesso")
            return {"status": "no_data"}
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 1)
        
        print(f"\n📋 RELATÓRIO DE QUALIDADE - TPC-H SF {scale_factor} ICEBERG")
        print("=" * 65)
        
        print(f"📅 Data/Hora: {datetime.now()}")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"🧊 Formato: Apache Iceberg")
        print(f"✅ Tabelas processadas: {len(successful_tables)}")
        print(f"📊 Total registros: {processing_result['total_silver_records']:,}")
        
        print(f"\n📈 Detalhamento por tabela:")
        for result in successful_tables:
            table_name = result["table_name"]
            bronze_count = result["bronze_records"]
            silver_count = result["silver_records"]
            duplicates = result.get("duplicates_removed", 0)
            partitioned = result.get("partitioned_by")
            iceberg_table = result.get("iceberg_table", "N/A")
            
            print(f"   🔹 {table_name}:")
            print(f"      - Registros: {bronze_count:,} → {silver_count:,}")
            print(f"      - Tabela Iceberg: {iceberg_table}")
            if duplicates > 0:
                print(f"      - Duplicatas removidas: {duplicates:,}")
            if partitioned:
                print(f"      - Particionado por: {', '.join(partitioned)}")
        
        print(f"\n💾 Localização Silver Iceberg: s3://silver/sf_{scale_factor}/iceberg/")
        print(f"🗃️ Namespace: iceberg.sf_{scale_factor}")
        print(f"🔗 Acesso MinIO: http://localhost:9001")
        print("=" * 65)
        
        return {
            "report_generated": True,
            "scale_factor": scale_factor,
            "format": "iceberg",
            "tables_processed": len(successful_tables),
            "total_records": processing_result['total_silver_records']
        }

    # Pipeline de execução
    validation_task = validate_bronze_data()
    processing_task = process_bronze_to_silver_iceberg(validation_task)
    report_task = generate_data_quality_report(processing_task)

    # Dependências
    validation_task >> processing_task >> report_task

bronze_to_silver_iceberg()