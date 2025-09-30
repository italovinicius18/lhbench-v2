# LHBench TPC-H Benchmark - Versão Standalone

Uma versão completamente independente do LHBench que executa o benchmark TPC-H sem depender do Airflow.

## 🚀 Características

- **Zero dependência do Airflow**: Execução direta em Python
- **Pipeline completo**: Bronze → Silver → Gold (Benchmark)
- **Múltiplos formatos**: Delta Lake, Apache Iceberg, Apache Hudi
- **Configuração simples**: Via variáveis de ambiente ou código
- **Logs estruturados**: Acompanhamento detalhado do progresso
- **Execução flexível**: Pipeline completo ou etapas individuais

## 📋 Pré-requisitos

### Infraestrutura
- **MinIO** ou S3 compatível rodando
- **Buckets criados**: `bronze`, `silver`, `gold`
- **Java 11+** para Apache Spark

### Python
- **Python 3.8+**
- **Memória**: Mínimo 8GB RAM (recomendado 16GB+ para SF > 10)

## 🔧 Instalação

### 1. Instalar dependências

```bash
# Instalar dependências essenciais
pip install -r requirements-standalone.txt

# Ou instalar individualmente
pip install duckdb pyspark boto3 pandas pyarrow delta-spark
```

### 2. Configurar ambiente

```bash
# Configurações MinIO/S3
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"

# Configurações TPC-H
export SCALE_FACTOR=10
export BENCHMARK_ITERATIONS=3

# Configurações Spark (opcional)
export SPARK_EXECUTOR_MEMORY="4g"
export SPARK_DRIVER_MEMORY="2g"
```

### 3. Verificar conectividade

```python
# Teste rápido
from standalone.utils import get_s3_client

s3 = get_s3_client()
buckets = s3.list_buckets()
print("Buckets:", [b['Name'] for b in buckets['Buckets']])
```

## 🎯 Uso Rápido

### Pipeline Completo

```bash
# Execução completa - SF=10, todos os formatos
python -m standalone.main

# Scale Factor personalizado
python -m standalone.main --scale-factor 1

# Apenas Delta Lake para teste rápido
python -m standalone.main --formats delta --iterations 1
```

### Etapas Individuais

```bash
# Apenas geração Bronze
python -m standalone.main --bronze-only

# Apenas conversão Silver
python -m standalone.main --silver-only --formats delta iceberg

# Apenas Benchmark
python -m standalone.main --benchmark-only
```

### Uso Programático

```python
from standalone import run_full_pipeline

# Pipeline completo
results = run_full_pipeline(
    scale_factor=10,
    benchmark_formats=["delta", "iceberg", "hudi"],
    benchmark_iterations=3
)

print(f"Status: {results['status']}")
```

## 📊 Exemplos Práticos

### Exemplo 1: Teste Rápido (SF=1, Delta apenas)

```python
from standalone import run_full_pipeline

results = run_full_pipeline(
    scale_factor=1,
    benchmark_formats=["delta"],
    benchmark_iterations=1,
    force_regenerate_bronze=True
)
```

### Exemplo 2: Benchmark Completo

```python
from standalone import run_full_pipeline

results = run_full_pipeline(
    scale_factor=10,
    benchmark_formats=["delta", "iceberg", "hudi"], 
    benchmark_iterations=3
)

# Ver ranking de performance
if results["status"] == "success":
    ranking = results["stages"]["benchmark"]["comparison"]["format_ranking"]
    for i, fmt in enumerate(ranking, 1):
        print(f"{i}. {fmt['format']}: {fmt['overall_avg_time']:.2f}s")
```

### Exemplo 3: Etapas Separadas

```python
from standalone import generate_bronze_data, convert_to_silver_format, benchmark_format

# 1. Gerar dados bronze
bronze_result = generate_bronze_data(scale_factor=1)

# 2. Converter para Delta
silver_result = convert_to_silver_format("delta", scale_factor=1)

# 3. Executar benchmark
benchmark_result = benchmark_format("delta", scale_factor=1, iterations=1)
```

## ⚙️ Configuração Avançada

### Arquivo de Configuração

```python
from standalone import LHBenchConfig

config = LHBenchConfig(
    scale_factor=10,
    minio_endpoint="http://minio.local:9000",
    minio_access_key="admin",
    minio_secret_key="password123",
    
    # Spark tuning
    spark_executor_memory="8g",
    spark_driver_memory="4g", 
    spark_executor_cores=4,
    spark_executor_instances=3,
    
    # Benchmark settings
    benchmark_formats=["delta", "iceberg"],
    benchmark_iterations=5,
    timeout_minutes=60,
    
    # Output
    results_path="./results",
    save_detailed_results=True
)
```

### Configuração por Variáveis de Ambiente

```bash
# TPC-H
export SCALE_FACTOR=100
export BENCHMARK_ITERATIONS=5
export BENCHMARK_FORMATS="delta,iceberg,hudi"

# MinIO/S3
export MINIO_ENDPOINT="http://s3.amazonaws.com"
export MINIO_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"  
export MINIO_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export BRONZE_BUCKET="tpch-bronze"
export SILVER_BUCKET="tpch-silver"
export GOLD_BUCKET="tpch-gold"

# Spark
export SPARK_EXECUTOR_MEMORY="16g"
export SPARK_DRIVER_MEMORY="8g"
export SPARK_EXECUTOR_CORES=8
export SPARK_EXECUTOR_INSTANCES=4
```

## 📈 Monitoramento e Resultados

### Logs Estruturados

```
2025-09-29 14:30:15 | lhbench | INFO | 🚀 Iniciando Geração TPC-H Bronze
2025-09-29 14:30:16 | lhbench | INFO | ✅ Extensão TPC-H carregada
2025-09-29 14:30:20 | lhbench | INFO |    ✅ customer: 150,000 linhas → s3://bronze/sf_1/parquet/customer/
2025-09-29 14:30:25 | lhbench | INFO |    ✅ lineitem: 6,001,215 linhas → s3://bronze/sf_1/parquet/lineitem/
2025-09-29 14:30:30 | lhbench | INFO | ✅ Geração TPC-H Bronze concluída em 15.2s
```

### Resultados Salvos

Os resultados são salvos automaticamente:

- **Local**: `./results/` (JSON detalhado)
- **S3/MinIO**: `s3://gold/benchmark_results/` 

Estrutura dos resultados:
```
results/
├── bronze_generation_20250929_143015.json
├── silver_conversion_delta_20250929_143045.json
├── benchmark_delta_20250929_143200.json
└── benchmark_complete_20250929_143300.json
```

### Métricas de Performance

```json
{
  "comparison": {
    "format_ranking": [
      {
        "format": "delta",
        "overall_avg_time": 45.32,
        "successful_queries": 22,
        "total_queries": 22
      },
      {
        "format": "iceberg", 
        "overall_avg_time": 52.18,
        "successful_queries": 22,
        "total_queries": 22
      }
    ],
    "best_format": "delta"
  }
}
```

## 🔧 Troubleshooting

### Problema: Erro de memória Spark

```bash
# Aumentar memória
export SPARK_EXECUTOR_MEMORY="8g"
export SPARK_DRIVER_MEMORY="4g"

# Ou reduzir Scale Factor
python -m standalone.main --scale-factor 1
```

### Problema: Timeout nas queries

```bash
# Aumentar timeout
python -m standalone.main --timeout-minutes 60
```

### Problema: MinIO não conecta

```python
# Verificar conectividade
from standalone.utils import get_s3_client

try:
    s3 = get_s3_client()
    response = s3.list_buckets()
    print("✅ MinIO conectado")
except Exception as e:
    print(f"❌ Erro MinIO: {e}")
```

### Problema: Dependências Spark

```bash
# Verificar Java
java -version

# Reinstalar PySpark
pip uninstall pyspark
pip install pyspark==3.5.5
```

## 📚 Estrutura do Código

```
standalone/
├── __init__.py              # API pública
├── config.py               # Configurações centralizadas
├── utils.py                # Utilitários comuns
├── bronze_generator.py     # Geração dados TPC-H (DuckDB)
├── silver_converter.py     # Conversão Bronze→Silver (Spark)
├── benchmark_executor.py   # Benchmark 22 queries (Spark)
├── tpch_queries.py        # Queries TPC-H padrão
└── main.py                # Script principal CLI
```

## 🚦 Comandos CLI Completos

```bash
# Ajuda
python -m standalone.main --help

# Pipeline completo com opções
python -m standalone.main \
  --scale-factor 10 \
  --formats delta iceberg hudi \
  --iterations 3 \
  --force-bronze \
  --log-level INFO

# Apenas uma etapa
python -m standalone.main --bronze-only --scale-factor 1
python -m standalone.main --silver-only --formats delta
python -m standalone.main --benchmark-only --iterations 1

# Pular etapas
python -m standalone.main --skip-bronze --skip-silver  # Só benchmark
python -m standalone.main --skip-benchmark             # Bronze + Silver
```

## 🎯 Performance Tips

### Para Scale Factors Grandes (SF >= 100)

```bash
# Configuração robusta
export SPARK_EXECUTOR_MEMORY="16g"
export SPARK_DRIVER_MEMORY="8g"
export SPARK_EXECUTOR_CORES=8
export SPARK_EXECUTOR_INSTANCES=6

# Executar com timeout maior
python -m standalone.main --scale-factor 100 --timeout-minutes 120
```

### Para Testes Rápidos

```bash
# Configuração mínima
python -m standalone.main \
  --scale-factor 1 \
  --formats delta \
  --iterations 1 \
  --force-bronze
```

### Para Máxima Performance

```bash
# Usar cluster Spark externo com configuração otimizada
export SPARK_MASTER="spark://cluster:7077"
export SPARK_EXECUTOR_INSTANCES=10
export SPARK_EXECUTOR_MEMORY="32g"
export SPARK_EXECUTOR_CORES=16
```

---

## 🔗 Migração do Airflow

Se você estava usando a versão com Airflow, a migração é simples:

**Antes (Airflow)**:
```python
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAG complexo com dependências...
```

**Depois (Standalone)**:
```python
from standalone import run_full_pipeline

# Uma linha!
results = run_full_pipeline(scale_factor=10)
```

**Vantagens da versão standalone**:
- ✅ Zero overhead do Airflow
- ✅ Execução mais rápida
- ✅ Debugging simplificado  
- ✅ Deploy mais fácil
- ✅ Menos dependências
- ✅ Controle total do fluxo