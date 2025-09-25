from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

@dag(
    dag_id="0_master_tpch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Execução manual apenas
    catchup=False,
    tags=["tpch", "master", "orchestrator", "pipeline", "end-to-end"],
    max_active_runs=1,
    description="DAG orquestradora que executa todo o pipeline TPC-H de forma completamente linear",
    params={
        "scale_factor": 10,
        "input_format": "csv",  # csv ou parquet
        "force_regenerate_bronze": False,
        "force_recreate_silver": False,
        "run_benchmark": True,
        "benchmark_formats": ["delta", "iceberg", "hudi"],
        "benchmark_iterations": 3,
        "cleanup_on_failure": False
    },
)
def master_tpch_pipeline():
    """
    DAG Master que orquestra todo o pipeline TPC-H de forma linear:
    1. Geração de dados bronze (TPC-H)
    2. Conversão para silver Delta
    3. Conversão para silver Iceberg  
    4. Conversão para silver Hudi
    5. Benchmark das 22 queries TPC-H
    6. Relatório final consolidado
    """

    @task
    def validate_pipeline_parameters(**context):
        """Valida os parâmetros do pipeline e configurações"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 10)
        input_format = params.get("input_format", "csv")
        benchmark_formats = params.get("benchmark_formats", ["delta", "iceberg", "hudi"])
        
        print(f"🔍 Validando parâmetros do pipeline TPC-H")
        print(f"📊 Scale Factor: {scale_factor}")
        print(f"📄 Formato de entrada: {input_format}")
        print(f"🎯 Formatos para benchmark: {benchmark_formats}")
        
        # Validações
        if scale_factor < 1:
            raise ValueError("Scale factor deve ser >= 1")
        
        if input_format not in ["csv", "parquet"]:
            raise ValueError("Formato deve ser 'csv' ou 'parquet'")
        
        valid_formats = ["delta", "iceberg", "hudi"]
        invalid_formats = [f for f in benchmark_formats if f not in valid_formats]
        if invalid_formats:
            raise ValueError(f"Formatos inválidos: {invalid_formats}. Válidos: {valid_formats}")
        
        # Verificar conectividade MinIO
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=Variable.get("MINIO_ENDPOINT"),
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
            )
            
            # Testar conectividade
            s3_client.list_buckets()
            print("✅ Conectividade MinIO validada")
            
            # Verificar buckets necessários
            required_buckets = ["bronze", "silver", "gold"]
            existing_buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
            
            for bucket in required_buckets:
                if bucket not in existing_buckets:
                    print(f"⚠️ Criando bucket ausente: {bucket}")
                    try:
                        s3_client.create_bucket(Bucket=bucket)
                        print(f"✅ Bucket {bucket} criado")
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                            raise
                else:
                    print(f"✅ Bucket {bucket} disponível")
            
        except Exception as e:
            print(f"❌ Erro na validação MinIO: {str(e)}")
            raise
        
        print("🎉 Validação do pipeline concluída com sucesso!")
        
        return {
            "validation_status": "success",
            "scale_factor": scale_factor,
            "input_format": input_format,
            "benchmark_formats": benchmark_formats,
            "pipeline_start_time": datetime.now().isoformat()
        }

    @task
    def generate_pipeline_report(validation_result: dict, **context):
        """Gera relatório inicial do pipeline"""
        
        params = context["params"]
        
        print(f"📋 RELATÓRIO INICIAL - PIPELINE TPC-H MASTER")
        print(f"=" * 60)
        print(f"📅 Data/Hora de início: {validation_result['pipeline_start_time']}")
        print(f"📊 Scale Factor: {validation_result['scale_factor']}")
        print(f"📄 Formato de entrada: {validation_result['input_format']}")
        print(f"🎯 Formatos benchmark: {', '.join(validation_result['benchmark_formats'])}")
        print(f"")
        print(f"📋 Etapas do Pipeline:")
        print(f"   1️⃣ Geração Bronze (TPC-H)")
        print(f"   2️⃣ Conversão Silver (Delta)")
        print(f"   3️⃣ Conversão Silver (Iceberg)")  
        print(f"   4️⃣ Conversão Silver (Hudi)")
        print(f"   5️⃣ Benchmark Gold (22 queries)")
        print(f"   6️⃣ Relatório Final")
        print(f"")
        print(f"⚙️ Configurações:")
        print(f"   🔄 Force regenerate bronze: {params.get('force_regenerate_bronze', False)}")
        print(f"   🔄 Force recreate silver: {params.get('force_recreate_silver', False)}")
        print(f"   📊 Benchmark iterations: {params.get('benchmark_iterations', 3)}")
        print(f"=" * 60)
        
        return validation_result

    # 1️⃣ ETAPA 1: Geração de dados bronze TPC-H
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_generation",
        trigger_dag_id="1_generate_bronze_tpch",
        conf={
            "scale_factor": "{{ params.scale_factor }}",
            "output_format": "{{ params.input_format }}",
            "force_regenerate": "{{ params.force_regenerate_bronze }}",
            "parallel_children": 5
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 2️⃣ ETAPA 2: Conversão Bronze → Silver Delta
    trigger_silver_delta = TriggerDagRunOperator(
        task_id="trigger_silver_delta",
        trigger_dag_id="2_bronze_to_silver_delta",
        conf={
            "scale_factor": "{{ params.scale_factor }}",
            "input_format": "{{ params.input_format }}",
            "force_recreate": "{{ params.force_recreate_silver }}",
            "validate_data": True
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 3️⃣ ETAPA 3: Conversão Bronze → Silver Iceberg  
    trigger_silver_iceberg = TriggerDagRunOperator(
        task_id="trigger_silver_iceberg",
        trigger_dag_id="2_bronze_to_silver_iceberg",
        conf={
            "scale_factor": "{{ params.scale_factor }}",
            "input_format": "{{ params.input_format }}",
            "force_recreate": "{{ params.force_recreate_silver }}",
            "validate_data": True
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 4️⃣ ETAPA 4: Conversão Bronze → Silver Hudi
    trigger_silver_hudi = TriggerDagRunOperator(
        task_id="trigger_silver_hudi",
        trigger_dag_id="2_bronze_to_silver_hudi",
        conf={
            "scale_factor": "{{ params.scale_factor }}",
            "input_format": "{{ params.input_format }}",
            "force_recreate": "{{ params.force_recreate_silver }}",
            "validate_data": True
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # Sensor para aguardar todas as conversões silver
    @task
    def validate_silver_completion(**context):
        """Valida que todas as conversões silver foram concluídas"""
        
        params = context["params"]
        scale_factor = params.get("scale_factor", 10)
        
        print(f"🔍 Validando conclusão das conversões Silver")
        print(f"📊 Verificando dados para SF {scale_factor}")
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=Variable.get("MINIO_ENDPOINT"),
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY")
            )
            
            formats = ["delta", "iceberg", "hudi"]
            tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
            
            validation_results = {}
            
            for format_name in formats:
                format_tables_found = 0
                for table in tables:
                    prefix = f"sf_{scale_factor}/{format_name}/{table}/"
                    
                    try:
                        response = s3_client.list_objects_v2(
                            Bucket="silver",
                            Prefix=prefix,
                            MaxKeys=1
                        )
                        
                        if 'Contents' in response and len(response['Contents']) > 0:
                            format_tables_found += 1
                            
                    except ClientError:
                        pass
                
                validation_results[format_name] = {
                    "tables_found": format_tables_found,
                    "tables_expected": len(tables),
                    "complete": format_tables_found == len(tables)
                }
                
                status = "✅" if format_tables_found == len(tables) else "❌"
                print(f"   {status} {format_name}: {format_tables_found}/{len(tables)} tabelas")
            
            # Verificar se todos os formatos estão completos
            all_complete = all(r["complete"] for r in validation_results.values())
            
            if all_complete:
                print(f"✅ Todas as conversões Silver concluídas com sucesso!")
                return {
                    "validation_status": "success",
                    "formats_ready": list(validation_results.keys()),
                    "validation_results": validation_results
                }
            else:
                incomplete_formats = [f for f, r in validation_results.items() if not r["complete"]]
                raise Exception(f"Formatos incompletos: {incomplete_formats}")
                
        except Exception as e:
            print(f"❌ Erro na validação Silver: {str(e)}")
            raise

    # 5️⃣ ETAPA 5: Benchmark TPC-H nas 22 queries
    @task 
    def trigger_benchmark_conditionally(**context):
        """Dispara benchmark apenas se habilitado"""
        
        params = context["params"]
        run_benchmark = params.get("run_benchmark", True)
        
        if not run_benchmark:
            print("⏭️ Benchmark desabilitado via parâmetro")
            return {"status": "skipped", "reason": "disabled_by_parameter"}
        
        print("🚀 Preparando execução do benchmark...")
        return {"status": "ready_for_benchmark"}

    trigger_benchmark = TriggerDagRunOperator(
        task_id="trigger_tpch_benchmark",
        trigger_dag_id="6_tpch_benchmark_gold",
        conf={
            "scale_factor": "{{ params.scale_factor }}",
            "test_formats": "{{ params.benchmark_formats }}",
            "iterations": "{{ params.benchmark_iterations }}",
            "run_warmup": True,
            "timeout_minutes": 30,
            "save_results": True
        },
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # 6️⃣ ETAPA 6: Relatório Final do Pipeline
    @task
    def generate_final_report(validation_result: dict, silver_validation: dict, **context):
        """Gera relatório final consolidado do pipeline"""
        
        params = context["params"]
        pipeline_end_time = datetime.now()
        pipeline_start_time = datetime.fromisoformat(validation_result["pipeline_start_time"])
        total_duration = pipeline_end_time - pipeline_start_time
        
        print(f"\n🎉 RELATÓRIO FINAL - PIPELINE TPC-H MASTER")
        print(f"=" * 70)
        print(f"📅 Início: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📅 Fim: {pipeline_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Duração total: {total_duration}")
        print(f"")
        print(f"📊 Configuração do Pipeline:")
        print(f"   • Scale Factor: {validation_result['scale_factor']}")
        print(f"   • Formato: {validation_result['input_format']}")
        print(f"   • Formatos benchmark: {', '.join(validation_result['benchmark_formats'])}")
        print(f"")
        print(f"✅ Etapas Concluídas:")
        print(f"   1️⃣ Geração Bronze TPC-H: ✅")
        print(f"   2️⃣ Conversão Silver Delta: ✅")
        print(f"   3️⃣ Conversão Silver Iceberg: ✅") 
        print(f"   4️⃣ Conversão Silver Hudi: ✅")
        
        if params.get("run_benchmark", True):
            print(f"   5️⃣ Benchmark 22 Queries TPC-H: ✅")
        else:
            print(f"   5️⃣ Benchmark TPC-H: ⏭️ (Desabilitado)")
        
        print(f"")
        print(f"📊 Resultados Silver:")
        for format_name, result in silver_validation["validation_results"].items():
            status = "✅" if result["complete"] else "❌"
            print(f"   {status} {format_name.upper()}: {result['tables_found']}/8 tabelas")
        
        print(f"")
        print(f"💾 Localização dos Dados:")
        print(f"   📂 Bronze: s3://bronze/sf_{validation_result['scale_factor']}/")
        print(f"   📂 Silver: s3://silver/sf_{validation_result['scale_factor']}/")
        print(f"   📂 Gold: s3://gold/ (resultados benchmark)")
        print(f"")
        print(f"🔗 Acesso MinIO: {Variable.get('MINIO_ENDPOINT', 'http://localhost:9001')}")
        print(f"=" * 70)
        
        return {
            "pipeline_status": "completed",
            "start_time": validation_result["pipeline_start_time"],
            "end_time": pipeline_end_time.isoformat(),
            "duration_seconds": total_duration.total_seconds(),
            "scale_factor": validation_result["scale_factor"],
            "formats_completed": list(silver_validation["validation_results"].keys()),
            "benchmark_executed": params.get("run_benchmark", True)
        }

    @task
    def cleanup_on_failure(**context):
        """Limpeza opcional em caso de falha"""
        
        params = context["params"]
        cleanup_enabled = params.get("cleanup_on_failure", False)
        
        if not cleanup_enabled:
            print("🚫 Limpeza automática desabilitada")
            return {"status": "cleanup_disabled"}
        
        # Implementar limpeza se necessário
        print("🧹 Executando limpeza pós-falha...")
        # Aqui poderíamos limpar dados parciais, etc.
        
        return {"status": "cleanup_completed"}

    # 📋 DEFINIÇÃO DO PIPELINE LINEAR
    
    # Inicialização
    validation_task = validate_pipeline_parameters()
    report_task = generate_pipeline_report(validation_task)
    
    # Bronze (Etapa 1)
    bronze_trigger = trigger_bronze
    
    # Silver (Etapas 2-4) - Executadas de forma linear/sequencial
    silver_delta = trigger_silver_delta
    silver_iceberg = trigger_silver_iceberg  
    silver_hudi = trigger_silver_hudi
    silver_validation_task = validate_silver_completion()
    
    # Benchmark (Etapa 5)
    benchmark_check = trigger_benchmark_conditionally()
    benchmark_trigger = trigger_benchmark
    
    # Finalização (Etapa 6)
    final_report = generate_final_report(validation_task, silver_validation_task)
    
    # 🔗 DEPENDÊNCIAS DO PIPELINE
    
    # Inicialização
    validation_task >> report_task >> bronze_trigger
    
    # Bronze → Silver (Linear/Sequencial)
    bronze_trigger  >> silver_delta
    silver_delta >> silver_iceberg
    silver_iceberg >> silver_hudi
    
    # Silver → Validação
    silver_hudi >> silver_validation_task
    
    # Validação → Benchmark
    silver_validation_task >> benchmark_check
    benchmark_check >> benchmark_trigger
    
    # Benchmark → Relatório Final
    benchmark_trigger >> final_report


master_tpch_pipeline()