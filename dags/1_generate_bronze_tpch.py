"""
DAG para geração de dados TPC-H sintéticos usando tpchgen-cli
Gera dados diretamente no MinIO usando boto3 para máxima eficiência.
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import subprocess
import tempfile
import os
import boto3
from botocore.exceptions import ClientError

# Configurações TPC-H
SCALE_FACTOR = 1  # Comece com SF=1 (cerca de 1GB)
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_BRONZE = "bronze"


def get_minio_client():
    """Cria cliente MinIO usando boto3"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )


@dag(
    dag_id="generate_tpch_bronze_direct",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["tpch", "bronze", "minio-direct", "streaming"],
    max_active_runs=1,
    params={
        "scale_factor": 1,
        "force_regenerate": False,
        "start_part": 1,
        "end_part": 2,
    },
)
def generate_tpch_bronze_direct():

    @task
    def setup_minio():
        """Configura o bucket no MinIO"""
        print("🔧 Configurando MinIO...")
        
        s3_client = get_minio_client()
        
        # Criar bucket se não existir
        try:
            s3_client.head_bucket(Bucket=BUCKET_BRONZE)
            print(f"✅ Bucket {BUCKET_BRONZE} já existe")
        except ClientError:
            try:
                s3_client.create_bucket(Bucket=BUCKET_BRONZE)
                print(f"✅ Bucket {BUCKET_BRONZE} criado")
            except Exception as e:
                print(f"❌ Erro ao criar bucket: {e}")
                raise
        
        return {"bucket": BUCKET_BRONZE, "endpoint": MINIO_ENDPOINT}

    @task
    def check_existing_scale_factor(**context):
        """Verifica se o scale factor já foi processado"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", SCALE_FACTOR)
        force_regenerate = params.get("force_regenerate", False)

        print(f"🔍 Verificando Scale Factor {scale_factor}")

        s3_client = get_minio_client()
        
        # Verificar se já existe dados para este scale factor
        try:
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_BRONZE,
                Prefix=f"sf{scale_factor}/",
                MaxKeys=1000
            )
            
            scale_factor_exists = False
            if 'Contents' in response:
                # Verifica se existe pelo menos 8 tabelas (as 8 tabelas do TPC-H)
                tables_found = set()
                for obj in response['Contents']:
                    for table in ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]:
                        if f"sf{scale_factor}/{table}/" in obj['Key']:
                            tables_found.add(table)
                
                if len(tables_found) >= 8:
                    scale_factor_exists = True

            needs_generation = force_regenerate or not scale_factor_exists

            if scale_factor_exists and not force_regenerate:
                print(f"⚠️ Scale Factor {scale_factor} já existe. Use force_regenerate=True para regerar")
            else:
                print(f"✅ Scale Factor {scale_factor} será gerado")

            return {"scale_factor": scale_factor, "needs_generation": needs_generation}
            
        except Exception as e:
            print(f"❌ Erro ao verificar dados existentes: {e}")
            return {"scale_factor": scale_factor, "needs_generation": True}

    @task
    def generate_and_upload_tpch(check_result: dict, **context):
        """Gera dados TPC-H e faz upload direto para MinIO usando boto3"""

        if not check_result["needs_generation"]:
            print("⏭️ Pulando geração - dados já existem")
            return {"skipped": True}

        params = context["params"]
        scale_factor = check_result["scale_factor"]
        start_part = params.get("start_part", 1)
        end_part = params.get("end_part", 2)

        batch_timestamp = datetime.now()
        batch_id = batch_timestamp.strftime("%Y%m%d_%H%M%S")

        print(f"🚀 Iniciando geração TPC-H Scale Factor {scale_factor}")
        print(f"📅 Batch ID: {batch_id}")
        print(f"🔢 Partições: {start_part} até {end_part}")

        # Tabelas TPC-H (mantendo nomes originais)
        tpch_tables = [
            "customer",
            "lineitem", 
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier"
        ]

        s3_client = get_minio_client()
        total_files = 0
        total_size = 0

        with tempfile.TemporaryDirectory() as temp_dir:

            # Gerar partições incrementais para tabelas lineitem e orders
            for part_num in range(start_part, end_part + 1):
                print(f"🔢 Processando partição {part_num}")

                # Diretório para esta partição
                part_dir = os.path.join(temp_dir, f"part_{part_num}")
                os.makedirs(part_dir, exist_ok=True)

                # Gerar apenas lineitem e orders com partições (as tabelas grandes)
                for table in ["lineitem", "orders"]:
                    gen_cmd = [
                        "tpchgen-cli",
                        "--tables", table,
                        "--scale-factor", str(scale_factor),
                        "--format", "tbl",
                        "--output-dir", part_dir,
                        "--parts", "10",  # Total de 10 partições
                        "--part", str(part_num),
                    ]

                    print(f"🔧 Gerando: {' '.join(gen_cmd)}")

                    # Gerar dados
                    result = subprocess.run(gen_cmd, capture_output=True, text=True)

                    if result.returncode != 0:
                        print(f"❌ Erro ao gerar {table} part {part_num}: {result.stderr}")
                        continue

                    print(f"✅ {table} partição {part_num} gerado com sucesso")

                # Upload dos arquivos desta partição usando boto3
                for root, dirs, files in os.walk(part_dir):
                    for file in files:
                        if file.endswith(".tbl"):
                            local_path = os.path.join(root, file)
                            file_size = os.path.getsize(local_path)

                            # Extrair nome da tabela do arquivo
                            table_name = None
                            for tpch_table in tpch_tables:
                                if tpch_table in file or tpch_table in root:
                                    table_name = tpch_table
                                    break

                            if not table_name:
                                continue

                            # Caminho no MinIO: sf{scale_factor}/{table_name}/{file}
                            s3_key = f"sf{scale_factor}/{table_name}/{file}"

                            print(f"⬆️ Upload: {file} ({file_size:,} bytes) -> s3://{BUCKET_BRONZE}/{s3_key}")

                            try:
                                # Upload usando boto3
                                s3_client.upload_file(local_path, BUCKET_BRONZE, s3_key)
                                print(f"✅ Upload concluído: {file}")
                                total_files += 1
                                total_size += file_size
                            except Exception as e:
                                print(f"❌ Erro no upload: {e}")
                                raise Exception(f"Falha no upload de {file}: {e}")

            # Gerar tabelas dimensão (sem particionamento) apenas uma vez
            if start_part == 1:  # Só na primeira partição
                for tpch_table in tpch_tables:
                    if tpch_table in ["lineitem", "orders"]:
                        continue  # Já processadas acima

                    print(f"📊 Processando tabela dimensão: {tpch_table}")

                    # Diretório temporário para esta tabela
                    table_dir = os.path.join(temp_dir, tpch_table)
                    os.makedirs(table_dir, exist_ok=True)

                    # Comando tpchgen-cli para tabela específica
                    gen_cmd = [
                        "tpchgen-cli",
                        "--scale-factor", str(scale_factor),
                        "--tables", tpch_table,
                        "--format", "tbl",
                        "--output-dir", table_dir,
                        "--parts", "4",  # 4 partições para paralelismo
                    ]

                    print(f"🔧 Gerando: {' '.join(gen_cmd)}")

                    # Gerar dados
                    result = subprocess.run(gen_cmd, capture_output=True, text=True)

                    if result.returncode != 0:
                        print(f"❌ Erro ao gerar {tpch_table}: {result.stderr}")
                        continue

                    print(f"✅ {tpch_table} gerado com sucesso")

                    # Upload de todos os arquivos da tabela
                    for root, dirs, files in os.walk(table_dir):
                        for file in files:
                            if file.endswith(".tbl"):
                                local_path = os.path.join(root, file)
                                file_size = os.path.getsize(local_path)

                                # Caminho no MinIO: sf{scale_factor}/{table_name}/{file}
                                s3_key = f"sf{scale_factor}/{tpch_table}/{file}"

                                print(f"⬆️ Upload: {file} ({file_size:,} bytes) -> s3://{BUCKET_BRONZE}/{s3_key}")

                                try:
                                    # Upload usando boto3
                                    s3_client.upload_file(local_path, BUCKET_BRONZE, s3_key)
                                    print(f"✅ Upload concluído: {file}")
                                    total_files += 1
                                    total_size += file_size
                                except Exception as e:
                                    print(f"❌ Erro no upload: {e}")
                                    raise Exception(f"Falha no upload de {file}: {e}")

                    print(f"🗑️ Limpando arquivos temporários de {tpch_table}")

        print(f"🎉 Geração e upload completos!")
        print(f"📊 Estatísticas finais:")
        print(f"   - Scale Factor: {scale_factor}")
        print(f"   - Partições processadas: {end_part - start_part + 1}")
        print(f"   - Arquivos enviados: {total_files}")
        print(f"   - Tamanho total: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        print(f"   - Batch ID: {batch_id}")

        return {
            "batch_id": batch_id,
            "scale_factor": scale_factor,
            "files_uploaded": total_files,
            "total_size_bytes": total_size,
            "skipped": False,
        }

    @task
    def verify_upload(upload_result: dict):
        """Verifica se os dados foram carregados corretamente no MinIO"""

        if upload_result.get("skipped"):
            print("⏭️ Verificação pulada - nenhum dado foi gerado")
            return upload_result

        scale_factor = upload_result["scale_factor"]

        print(f"🔍 Verificando upload do Scale Factor {scale_factor}")

        s3_client = get_minio_client()

        try:
            # Listar arquivos no MinIO
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_BRONZE,
                Prefix=f"sf{scale_factor}/",
                MaxKeys=1000
            )

            if 'Contents' in response:
                sf_files = [
                    obj for obj in response['Contents'] 
                    if f"sf{scale_factor}/" in obj['Key']
                ]

                print(f"✅ Verificação concluída")
                print(f"📁 Arquivos encontrados para SF {scale_factor}: {len(sf_files)}")

                total_size = 0
                for obj in sf_files[:10]:  # Mostrar primeiros 10
                    size_mb = obj['Size'] / (1024 * 1024)
                    total_size += obj['Size']
                    print(f"   📄 {obj['Key']} ({size_mb:.2f} MB)")

                if len(sf_files) > 10:
                    print(f"   ... e mais {len(sf_files) - 10} arquivos")
                
                # Calcular tamanho total de todos os arquivos
                for obj in sf_files[10:]:
                    total_size += obj['Size']
                
                print(f"📊 Tamanho total no MinIO: {total_size / (1024 * 1024):.2f} MB")
            else:
                print("❌ Nenhum arquivo encontrado no MinIO!")
                
        except Exception as e:
            print(f"❌ Erro na verificação: {e}")

        print("🏗️ Dados TPC-H prontos no MinIO!")

        return upload_result

    @task
    def install_dependencies():
        """Instala dependências necessárias"""
        print("📦 Instalando dependências...")
        
        try:
            # Verificar se tpchgen-cli está instalado
            result = subprocess.run(["tpchgen-cli", "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ tpchgen-cli já instalado")
            else:
                raise Exception("tpchgen-cli não encontrado")
        except:
            print("📥 Instalando tpchgen-cli...")
            result = subprocess.run(["pip", "install", "tpchgen-cli"], capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Erro ao instalar tpchgen-cli: {result.stderr}")
            print("✅ tpchgen-cli instalado")
        
        # Verificar boto3
        try:
            import boto3
            print("✅ boto3 já disponível")
        except ImportError:
            print("📥 Instalando boto3...")
            result = subprocess.run(["pip", "install", "boto3"], capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Erro ao instalar boto3: {result.stderr}")
            print("✅ boto3 instalado")
        
        return {"dependencies": "installed"}

    # Pipeline de execução
    check_task = check_existing_scale_factor()
    generate_task = generate_and_upload_tpch(check_task)
    verify_task = verify_upload(generate_task)

    # Dependências
    check_task >> generate_task >> verify_task


generate_tpch_bronze_direct()