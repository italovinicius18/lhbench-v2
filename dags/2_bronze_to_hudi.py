from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp, coalesce
from airflow.models import Variable

@dag(
    dag_id="2_bronze_to_silver_hudi",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Execução manual ou após o DAG 1
    catchup=False,
    tags=["tpch", "hudi", "silver", "bronze-to-silver"],
    max_active_runs=1,
    params={
        "scale_factor": 10,
        "input_format": "csv",  # parquet ou csv (deve coincidir com output_format do DAG 1)
        "force_recreate": False,
        "validate_data": True
    },
)
def bronze_to_silver_hudi():

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
            # Apache Hudi jars
            "spark.jars.packages": ",".join([
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.0",
            ]),
            # Hudi extensions
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            
            # Serialization (necessário para Hudi)
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.catalog.spark_catalog.type": "hudi",
            
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
            
            # Hudi specific configurations (configurações básicas)
            "hoodie.metadata.enable": "true",
            "hoodie.metadata.index.async": "false",
        }
    )
    def process_bronze_to_silver_hudi(validation_result: dict, spark: SparkSession = None, sc: SparkContext = None, **context):
        """Converte dados TPC-H do bronze para silver usando Apache Hudi"""
        
        if validation_result["validation_status"] == "error":
            print("❌ Erro na validação - abortando processamento")
            raise Exception(f"Validation failed: {validation_result.get('error', 'Unknown error')}")
        
        if not validation_result.get("found_tables"):
            print("❌ Nenhuma tabela encontrada - abortando processamento")
            raise Exception("No tables found in bronze layer")
            
        print("🚀 Iniciando conversão Bronze → Silver Hudi")
        
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
        
        # Definir configurações específicas das tabelas TPC-H para Hudi
        table_configs = {
            "customer": {
                "record_key": "c_custkey",
                "precombine_field": "silver_processed_at",  # Campo para resolver conflitos
                "partition_path": None,  # Sem particionamento para tabela pequena
                "table_type": "COPY_ON_WRITE",  # COW para queries frequentes
                "description": "Clientes TPC-H"
            },
            "supplier": {
                "record_key": "s_suppkey",
                "precombine_field": "silver_processed_at",
                "partition_path": None,
                "table_type": "COPY_ON_WRITE",
                "description": "Fornecedores TPC-H"
            },
            "part": {
                "record_key": "p_partkey",
                "precombine_field": "silver_processed_at",
                "partition_path": None,
                "table_type": "COPY_ON_WRITE",
                "description": "Peças TPC-H"
            },
            "partsupp": {
                "record_key": "ps_partkey,ps_suppkey",  # Chave composta
                "precombine_field": "silver_processed_at",
                "partition_path": None,
                "table_type": "COPY_ON_WRITE",
                "description": "Fornecedores de Peças TPC-H"
            },
            "orders": {
                "record_key": "o_orderkey",
                "precombine_field": "silver_processed_at",
                "partition_path": "o_orderstatus",  # Particionar por status
                "table_type": "COPY_ON_WRITE",  # COW para análises OLAP
                "description": "Pedidos TPC-H"
            },
            "lineitem": {
                "record_key": "l_orderkey,l_linenumber",  # Chave composta
                "precombine_field": "silver_processed_at",
                "partition_path": "l_shipmode",  # Particionar por modo de envio
                "table_type": "MERGE_ON_READ",  # MOR para tabela grande com muitas atualizações
                "description": "Itens de Pedidos TPC-H"
            },
            "nation": {
                "record_key": "n_nationkey",
                "precombine_field": "silver_processed_at",
                "partition_path": None,
                "table_type": "COPY_ON_WRITE",
                "description": "Nações TPC-H (dimensão)"
            },
            "region": {
                "record_key": "r_regionkey",
                "precombine_field": "silver_processed_at",
                "partition_path": None,
                "table_type": "COPY_ON_WRITE",
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
            record_key = config["record_key"]
            precombine_field = config["precombine_field"]
            partition_path = config.get("partition_path")
            table_type = config["table_type"]
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
                
                # Remover duplicatas por chave primária (importante para Hudi)
                key_columns = record_key.split(",")
                df_silver = df_silver.dropDuplicates(key_columns)
                unique_records = df_silver.count()
                
                print(f"   🔄 Registros únicos após deduplicação: {unique_records:,}")
                
                if unique_records != total_records:
                    duplicates_removed = total_records - unique_records
                    print(f"   🧹 Duplicatas removidas: {duplicates_removed:,}")
                
                # 3) Definir caminho da tabela Hudi no silver
                # Estrutura: s3a://silver/sf_{scale_factor}/hudi/{table_name}/
                silver_path = f"s3a://silver/sf_{scale_factor}/hudi/{table_name}/"
                
                print(f"   💾 Salvando em: {silver_path}")
                print(f"   🔑 Record Key: {record_key}")
                print(f"   🕐 Precombine Field: {precombine_field}")
                print(f"   📊 Table Type: {table_type}")
                if partition_path:
                    print(f"   📁 Partition Path: {partition_path}")
                
                # 4) Configurar opções Hudi específicas para a tabela
                hudi_options = {
                    "hoodie.table.name": f"tpch_{table_name}_sf_{scale_factor}",
                    "hoodie.datasource.write.recordkey.field": record_key,
                    "hoodie.datasource.write.precombine.field": precombine_field,
                    "hoodie.datasource.write.table.type": table_type,
                    "hoodie.datasource.write.operation": "upsert" if not force_recreate else "bulk_insert",
                    
                    # Configurações de performance
                    "hoodie.bulkinsert.shuffle.parallelism": "4",
                    "hoodie.upsert.shuffle.parallelism": "4",
                    "hoodie.delete.shuffle.parallelism": "4",
                    
                    # Configurações básicas
                    "hoodie.parquet.small.file.limit": "134217728",  # 128MB
                    
                    # Metadados
                    "hoodie.metadata.enable": "true",
                    "hoodie.metadata.index.async": "false",
                }
                
                # Configurações específicas por tipo de tabela
                if table_type == "MERGE_ON_READ":
                    # Para MOR, habilitar compactação automática
                    hudi_options.update({
                        "hoodie.compact.inline": "true",
                        "hoodie.compact.inline.max.delta.commits": "5",
                        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
                        "hoodie.cleaner.commits.retained": "3"
                    })
                    print(f"   🔄 Configurado para MOR com compactação automática")
                else:
                    # Para COW, não usar compactação (não suportada)
                    hudi_options.update({
                        "hoodie.compact.inline": "false",
                        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS", 
                        "hoodie.cleaner.commits.retained": "5"
                    })
                    print(f"   📝 Configurado para COW sem compactação")
                
                # Adicionar particionamento se especificado
                if partition_path:
                    hudi_options["hoodie.datasource.write.partitionpath.field"] = partition_path
                    hudi_options["hoodie.datasource.write.hive_style_partitioning"] = "true"
                else:
                    hudi_options["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
                
                # 5) Verificar se deve recriar ou fazer upsert
                table_exists = False
                try:
                    # Verificar se o caminho existe no S3 primeiro
                    import boto3
                    s3_client = boto3.client(
                        's3',
                        endpoint_url=Variable.get("MINIO_ENDPOINT"),
                        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
                    )
                    
                    # Extrair bucket e path do silver_path
                    # s3a://silver/sf_10/hudi/lineitem/ -> bucket=silver, prefix=sf_10/hudi/lineitem/
                    path_parts = silver_path.replace("s3a://", "").split("/", 1)
                    bucket = path_parts[0]
                    prefix = path_parts[1] if len(path_parts) > 1 else ""
                    
                    # Verificar se há arquivos no caminho
                    response = s3_client.list_objects_v2(
                        Bucket=bucket,
                        Prefix=prefix,
                        MaxKeys=1
                    )
                    
                    if 'Contents' in response and len(response['Contents']) > 0:
                        # Se há arquivos, tentar ler como Hudi para confirmar que é uma tabela válida
                        try:
                            test_df = spark.read.format("hudi").load(silver_path)
                            test_df.limit(1).collect()  # Força a leitura
                            table_exists = True
                            print(f"   📋 Tabela Hudi {table_name} já existe e é válida")
                        except Exception as read_error:
                            print(f"   ⚠️ Arquivos existem mas tabela Hudi inválida: {str(read_error)}")
                            table_exists = False
                    else:
                        table_exists = False
                        print(f"   🆕 Tabela Hudi {table_name} será criada (caminho vazio)")
                        
                except Exception as e:
                    table_exists = False
                    print(f"   🆕 Tabela Hudi {table_name} será criada (erro na verificação: {str(e)})")
                
                if force_recreate and table_exists:
                    print(f"   🔄 Force recreate habilitado - usando bulk_insert")
                    hudi_options["hoodie.datasource.write.operation"] = "bulk_insert"
                
                # 6) Escrever dados no formato Hudi
                writer = (df_silver.write
                         .format("hudi")
                         .options(**hudi_options)
                         .mode("append"))  # Hudi gerencia upserts internamente
                
                writer.save(silver_path)
                
                print(f"   ✅ Tabela Hudi salva com sucesso")
                
                # 7) Verificar dados escritos
                try:
                    final_df = spark.read.format("hudi").load(silver_path)
                    final_count = final_df.count()
                    print(f"   📊 Registros finais na tabela Hudi: {final_count:,}")
                except Exception as e:
                    print(f"   ⚠️ Erro ao verificar contagem final: {str(e)}")
                    final_count = unique_records
                
                # 8) Executar operações de manutenção Hudi específicas por tipo
                try:
                    print(f"   🔧 Executando manutenção Hudi...")
                    
                    if table_type == "MERGE_ON_READ":
                        print(f"   🗜️ Tabela MOR: compactação automática habilitada")
                        # Para MOR, a compactação automática foi configurada nas opções
                        # Não executamos compactação manual aqui para evitar conflitos
                    else:
                        print(f"   � Tabela COW: otimizada por design, sem compactação necessária")
                        # Para COW, não há compactação - tabelas já são otimizadas
                        
                    print(f"   ✅ Manutenção Hudi concluída")
                    
                except Exception as opt_e:
                    print(f"   ⚠️ Falha na manutenção Hudi: {str(opt_e)}")
                
                # 9) Coletar estatísticas da tabela
                processing_results.append({
                    "table_name": table_name,
                    "bronze_records": total_records,
                    "silver_records": final_count,
                    "duplicates_removed": total_records - unique_records,
                    "silver_path": silver_path,
                    "hudi_table_name": hudi_options["hoodie.table.name"],
                    "table_type": table_type,
                    "record_key": record_key,
                    "partitioned_by": partition_path,
                    "status": "success"
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
        print(f"\n🎉 Conversão Bronze → Silver Hudi concluída!")
        
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
            table_type = result.get("table_type", "N/A")
            hudi_table = result.get("hudi_table_name", "N/A")
            print(f"   📈 {table_name}: {bronze_count:,} → {silver_count:,} ({table_type}) [{hudi_table}]")
        
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
        """Gera relatório de qualidade dos dados convertidos para Hudi"""
        
        print("📊 Gerando relatório de qualidade dos dados...")
        
        successful_tables = [r for r in processing_result["processing_results"] if r["status"] == "success"]
        
        if not successful_tables:
            print("⚠️ Nenhuma tabela foi processada com sucesso")
            return {"status": "no_data"}
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 1)
        
        print(f"\n📋 RELATÓRIO DE QUALIDADE - TPC-H SF {scale_factor} HUDI")
        print("=" * 65)
        
        print(f"📅 Data/Hora: {datetime.now()}")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"🚁 Formato: Apache Hudi")
        print(f"✅ Tabelas processadas: {len(successful_tables)}")
        print(f"📊 Total registros: {processing_result['total_silver_records']:,}")
        
        # Estatísticas por tipo de tabela Hudi
        cow_tables = [r for r in successful_tables if r.get("table_type") == "COPY_ON_WRITE"]
        mor_tables = [r for r in successful_tables if r.get("table_type") == "MERGE_ON_READ"]
        
        print(f"\n🏗️ Distribuição por tipo de tabela:")
        print(f"   📝 COPY_ON_WRITE (COW): {len(cow_tables)} tabelas")
        print(f"   🔄 MERGE_ON_READ (MOR): {len(mor_tables)} tabelas")
        
        print(f"\n📈 Detalhamento por tabela:")
        for result in successful_tables:
            table_name = result["table_name"]
            bronze_count = result["bronze_records"]
            silver_count = result["silver_records"]
            duplicates = result.get("duplicates_removed", 0)
            partitioned = result.get("partitioned_by")
            table_type = result.get("table_type", "N/A")
            hudi_table = result.get("hudi_table_name", "N/A")
            record_key = result.get("record_key", "N/A")
            
            print(f"   🔹 {table_name} ({table_type}):")
            print(f"      - Registros: {bronze_count:,} → {silver_count:,}")
            print(f"      - Tabela Hudi: {hudi_table}")
            print(f"      - Record Key: {record_key}")
            if duplicates > 0:
                print(f"      - Duplicatas removidas: {duplicates:,}")
            if partitioned:
                print(f"      - Particionado por: {partitioned}")
        
        print(f"\n💾 Localização Silver Hudi: s3://silver/sf_{scale_factor}/hudi/")
        print(f"🔗 Acesso MinIO: http://localhost:9001")
        print(f"📖 Guia Hudi:")
        print(f"   - COW: Otimizado para leitura (queries analíticas)")
        print(f"   - MOR: Otimizado para escrita (ingestão frequente)")
        print("=" * 65)
        
        return {
            "report_generated": True,
            "scale_factor": scale_factor,
            "format": "hudi",
            "tables_processed": len(successful_tables),
            "cow_tables": len(cow_tables),
            "mor_tables": len(mor_tables),
            "total_records": processing_result['total_silver_records']
        }

    # Pipeline de execução
    validation_task = validate_bronze_data()
    processing_task = process_bronze_to_silver_hudi(validation_task)
    report_task = generate_data_quality_report(processing_task)

    # Dependências
    validation_task >> processing_task >> report_task

bronze_to_silver_hudi()