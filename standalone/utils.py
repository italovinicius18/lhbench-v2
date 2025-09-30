"""
Utilitários comuns para o LHBench standalone
"""
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

from .config import config

# Importações opcionais para AWS/S3
try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    print("⚠️ boto3 não instalado - funcionalidades S3 limitadas")

def setup_logging(log_level: str = None) -> logging.Logger:
    """Configura logging estruturado"""
    level = getattr(logging, (log_level or config.log_level).upper())
    
    # Formatter personalizado
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Handler para arquivo
    file_handler = logging.FileHandler(f"{config.results_path}/lhbench.log")
    file_handler.setFormatter(formatter)
    
    # Configurar logger root
    logger = logging.getLogger("lhbench")
    logger.setLevel(level)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def get_s3_client():
    """Cria cliente S3 para MinIO"""
    if not HAS_BOTO3:
        raise ImportError("boto3 não está instalado. Execute: pip install boto3")
    
    return boto3.client(
        's3',
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key
    )

def save_results(results: Dict[str, Any], stage: str, execution_id: str = None) -> str:
    """Salva resultados em JSON local e S3"""
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Estrutura dos resultados
    results_with_metadata = {
        "execution_id": execution_id,
        "stage": stage,
        "scale_factor": config.scale_factor,
        "timestamp": timestamp,
        "datetime": datetime.now().isoformat(),
        "config": {
            "scale_factor": config.scale_factor,
            "benchmark_formats": config.benchmark_formats,
            "benchmark_iterations": config.benchmark_iterations,
            "input_format": config.input_format
        },
        "data": results
    }
    
    # Salvar localmente
    local_file = f"{config.results_path}/{stage}_{execution_id}_{timestamp}.json"
    with open(local_file, 'w') as f:
        json.dump(results_with_metadata, f, indent=2, default=str)
    
    # Tentar salvar no S3 (opcional)
    try:
        if not HAS_BOTO3:
            logger.warning("boto3 não disponível - pulando salvamento S3")
            return local_file, None
            
        s3_client = get_s3_client()  
        s3_key = f"benchmark_results/sf_{config.scale_factor}/{execution_id}/{stage}_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=config.gold_bucket,
            Key=s3_key,
            Body=json.dumps(results_with_metadata, indent=2, default=str),
            ContentType="application/json"
        )
        
        return local_file, f"s3://{config.gold_bucket}/{s3_key}"
    
    except Exception as e:
        logger = logging.getLogger("lhbench")
        logger.warning(f"Falha ao salvar no S3: {e}")
        return local_file, None

def check_s3_table_exists(bucket: str, prefix: str) -> Dict[str, Any]:
    """Verifica se uma tabela existe no S3 e retorna metadados"""
    try:
        if not HAS_BOTO3:
            return {
                "exists": False,
                "error": "boto3 not available",
                "path": f"s3a://{bucket}/{prefix}"
            }
            
        s3_client = get_s3_client()
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=20
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            # Filtrar apenas arquivos de dados
            data_files = [obj for obj in response['Contents'] 
                         if not obj['Key'].endswith('/') and obj['Size'] > 0]
            
            if data_files:
                total_size = sum(obj['Size'] for obj in data_files)
                file_count = len(data_files)
                
                return {
                    "exists": True,
                    "file_count": file_count,
                    "total_size_mb": total_size / (1024 * 1024),
                    "path": f"s3a://{bucket}/{prefix}",
                    "files": [obj['Key'] for obj in data_files[:5]]  # Primeiros 5 arquivos
                }
        
        return {
            "exists": False,
            "path": f"s3a://{bucket}/{prefix}"
        }
        
    except ClientError as e:
        return {
            "exists": False,
            "error": str(e),
            "path": f"s3a://{bucket}/{prefix}"
        }

def validate_bronze_data(scale_factor: int = None) -> Dict[str, Any]:
    """Valida se dados bronze existem"""
    if scale_factor is None:
        scale_factor = config.scale_factor
    
    logger = logging.getLogger("lhbench")
    logger.info(f"🔍 Validando dados bronze - Scale Factor {scale_factor}")
    
    bronze_path = f"sf_{scale_factor}/{config.input_format}"
    tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    
    validation_results = {}
    
    for table in tpch_tables:
        table_prefix = f"{bronze_path}/{table}/"
        result = check_s3_table_exists(config.bronze_bucket, table_prefix)
        validation_results[table] = result
        
        if result["exists"]:
            logger.info(f"   ✅ {table}: {result['file_count']} arquivos, {result['total_size_mb']:.2f} MB")
        else:
            logger.warning(f"   ❌ {table}: não encontrado")
    
    found_tables = [t for t, r in validation_results.items() if r["exists"]]
    
    return {
        "validation_status": "success" if len(found_tables) == len(tpch_tables) else "partial",
        "bronze_path": bronze_path,
        "scale_factor": scale_factor,
        "input_format": config.input_format,
        "found_tables": found_tables,
        "missing_tables": [t for t in tpch_tables if t not in found_tables],
        "validation_results": validation_results,
        "all_tables_found": len(found_tables) == len(tpch_tables)
    }

def validate_silver_data(scale_factor: int = None, formats: List[str] = None) -> Dict[str, Any]:
    """Valida se dados silver existem nos formatos especificados"""
    if scale_factor is None:
        scale_factor = config.scale_factor
    if formats is None:
        formats = config.benchmark_formats
    
    logger = logging.getLogger("lhbench")
    logger.info(f"🔍 Validando dados silver - Scale Factor {scale_factor}, Formatos: {formats}")
    
    tpch_tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    validation_results = {}
    
    for format_name in formats:
        logger.info(f"📋 Validando formato {format_name.upper()}...")
        format_results = {}
        
        for table in tpch_tables:
            table_prefix = f"sf_{scale_factor}/{format_name}/{table}/"
            result = check_s3_table_exists(config.silver_bucket, table_prefix)
            format_results[table] = result
            
            if result["exists"]:
                logger.info(f"   ✅ {table}: {result['file_count']} arquivos, {result['total_size_mb']:.2f} MB")
            else:
                logger.warning(f"   ❌ {table}: não encontrado")
        
        validation_results[format_name] = format_results
    
    # Determinar formatos prontos
    ready_formats = []
    for format_name in formats:
        format_results = validation_results[format_name]
        found_tables = [t for t, r in format_results.items() if r["exists"]]
        
        if len(found_tables) >= len(tpch_tables) * 0.75:  # 75% das tabelas
            ready_formats.append(format_name)
            logger.info(f"   ✅ {format_name}: {len(found_tables)}/{len(tpch_tables)} tabelas - APROVADO")
        else:
            logger.warning(f"   ❌ {format_name}: {len(found_tables)}/{len(tpch_tables)} tabelas - INSUFICIENTE")
    
    return {
        "validation_status": "success" if ready_formats else "error",
        "scale_factor": scale_factor,
        "ready_formats": ready_formats,
        "validation_results": validation_results,
        "formats_tested": formats
    }

class Timer:
    """Context manager para medir tempo de execução"""
    
    def __init__(self, name: str, logger: logging.Logger = None):
        self.name = name
        self.logger = logger or logging.getLogger("lhbench")
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"🚀 Iniciando {self.name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"✅ {self.name} concluído em {duration:.2f}s")
        else:
            self.logger.error(f"❌ {self.name} falhou após {duration:.2f}s: {exc_val}")
    
    @property
    def duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

def create_execution_report(stage: str, results: Dict[str, Any], duration: float = None) -> Dict[str, Any]:
    """Cria relatório padronizado de execução"""
    report = {
        "stage": stage,
        "execution_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "timestamp": datetime.now().isoformat(),
        "scale_factor": config.scale_factor,
        "duration_seconds": duration,
        "status": "success" if not results.get("error") else "error",
        "results": results
    }
    
    return report

def print_summary_banner(title: str, details: Dict[str, Any] = None):
    """Imprime banner de resumo formatado"""
    logger = logging.getLogger("lhbench")
    
    banner_width = 80
    logger.info("=" * banner_width)
    logger.info(f" {title} ".center(banner_width))
    logger.info("=" * banner_width)
    
    if details:
        for key, value in details.items():
            logger.info(f"{key}: {value}")
        logger.info("=" * banner_width)