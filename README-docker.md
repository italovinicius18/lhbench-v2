# 🐳 LHBench TPC-H com Docker Compose

Execução completa do LHBench TPC-H usando Docker Compose com MinIO, Spark e a aplicação containerizada.

## 🚀 Início Rápido

### 1. **Teste Rápido (Scale Factor 1)**

```bash
# Construir e executar teste rápido
./run-docker.sh quick
```

### 2. **Configurar Recursos (Opcional)**

```bash
# Copiar configurações padrão
cp .env.example .env

# Editar conforme necessário
nano .env
```

### 3. **Benchmark Completo**

```bash
# SF=10, todos os formatos, 3 iterações
./run-docker.sh full
```

## 📋 Comandos Disponíveis

### Execução
```bash
# Teste rápido (SF=1, Delta, 1 iteração)
./run-docker.sh quick

# Benchmark completo (SF=10, todos formatos)
./run-docker.sh full

# Customizado
./run-docker.sh run [SCALE_FACTOR] [FORMATOS] [ITERAÇÕES]

# Exemplos:
./run-docker.sh run 1 "delta" 1
./run-docker.sh run 10 "delta iceberg hudi" 3
./run-docker.sh run 100 "delta iceberg" 5
```

### Etapas Individuais
```bash
# Apenas Bronze
./run-docker.sh bronze-only 10

# Apenas Silver  
./run-docker.sh silver-only 10 "delta iceberg" 

# Apenas Benchmark
./run-docker.sh benchmark-only 10 "delta"
```

### Gerenciamento
```bash
# Ver status dos serviços
./run-docker.sh status

# Ver logs
./run-docker.sh logs

# Acessar shell do container
./run-docker.sh shell

# Reconstruir imagem
./run-docker.sh build

# Limpar tudo
./run-docker.sh cleanup
```

## ⚙️ Configuração

### Arquivo `.env`

```bash
# TPC-H
SCALE_FACTOR=10
INPUT_FORMAT=parquet
BENCHMARK_FORMATS=delta,iceberg,hudi
BENCHMARK_ITERATIONS=3

# Spark (ajustar conforme recursos disponíveis)
SPARK_EXECUTOR_MEMORY=4g
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_INSTANCES=2

# Logging
LOG_LEVEL=INFO
```

### Recursos Recomendados

| Scale Factor | RAM | Executor Memory | Driver Memory |
|-------------|-----|-----------------|---------------|
| 1           | 4GB | 2g              | 1g            |
| 10          | 8GB | 4g              | 2g            |
| 100         | 16GB| 8g              | 4g            |
| 1000        | 32GB| 16g             | 8g            |

## 🏗️ Arquitetura Docker

### Serviços

- **MinIO**: Storage S3-compatível (portas 9000, 9001)
- **MinIO Client**: Cria buckets automaticamente
- **LHBench**: Aplicação principal
- **Spark Master/Worker**: Cluster Spark (opcional)

### Volumes

- `minio_data`: Dados persistentes do MinIO
- `./results`: Resultados dos benchmarks (host)
- `./logs`: Logs da aplicação (host)

### Network

- Rede interna `lhbench_network` para comunicação entre serviços

## 📊 Monitoramento

### MinIO Console
```bash
# Acessar interface web do MinIO
open http://localhost:9001
# Usuário: minioadmin / minioadmin
```

### Spark UI (se usando cluster)
```bash
# Ativar cluster Spark
docker-compose -f docker-compose.standalone.yml --profile spark-cluster up -d

# Acessar Spark Master UI
open http://localhost:8080
```

### Logs em Tempo Real
```bash
# Logs do LHBench
docker-compose -f docker-compose.standalone.yml logs -f lhbench

# Logs do MinIO
docker-compose -f docker-compose.standalone.yml logs -f minio
```

## 🔧 Troubleshooting

### Problema: Container sem memória

```bash
# Ajustar no .env
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g

# Ou reduzir Scale Factor
SCALE_FACTOR=1
```

### Problema: MinIO não conecta

```bash
# Verificar status
./run-docker.sh status

# Recriar serviços
./run-docker.sh cleanup
./run-docker.sh quick
```

### Problema: Build falha

```bash
# Limpar cache Docker
docker system prune -a

# Reconstruir
./run-docker.sh build
```

### Problema: Porta ocupada

```bash
# Parar serviços conflitantes
sudo lsof -i :9000
sudo lsof -i :9001

# Ou alterar portas no docker-compose.standalone.yml
```

## 🎯 Exemplos de Uso

### 1. Desenvolvimento/Teste
```bash
# Teste rápido para validar
./run-docker.sh quick

# Apenas bronze para validar geração
./run-docker.sh bronze-only 1
```

### 2. Benchmark Pequeno
```bash
# SF=10, apenas Delta para comparação rápida
./run-docker.sh run 10 "delta" 3
```

### 3. Benchmark Completo
```bash
# SF=100, todos os formatos, múltiplas iterações
SCALE_FACTOR=100 \
BENCHMARK_FORMATS="delta,iceberg,hudi" \
BENCHMARK_ITERATIONS=5 \
./run-docker.sh run 100 "delta iceberg hudi" 5
```

### 4. Debug Interativo
```bash
# Acessar shell do container
./run-docker.sh shell

# Dentro do container:
python -m standalone.main --help
python -c "from standalone.utils import get_s3_client; print('S3 OK')"
```

## 📁 Estrutura de Resultados

Após execução, os resultados ficam em:

```
results/
├── bronze_generation_20250929_143015.json
├── silver_conversion_delta_20250929_143045.json  
├── benchmark_delta_20250929_143200.json
└── benchmark_complete_20250929_143300.json

logs/
└── lhbench.log
```

## 🔄 Integração CI/CD

### GitHub Actions Example

```yaml
name: LHBench Benchmark

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Run LHBench Quick Test
        run: |
          chmod +x run-docker.sh
          ./run-docker.sh quick
          
      - name: Upload Results
        uses: actions/upload-artifact@v3
        with:
          name: lhbench-results
          path: results/
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    
    stages {
        stage('Benchmark') {
            steps {
                sh './run-docker.sh full'
            }
        }
        
        stage('Archive') {
            steps {
                archiveArtifacts artifacts: 'results/**/*.json'
            }
        }
    }
}
```

## 🔗 Comparação com Outras Versões

| Característica | Docker Compose | Standalone Local | Airflow |
|---------------|---------------|------------------|---------|
| Setup | Simples | Manual | Complexo |
| Dependências | Containerizado | Manual | Muitas |
| Isolamento | Total | Parcial | Parcial |
| Recursos | Configurável | Manual | Via DAGs |
| Monitoramento | Logs + UI | Logs | Airflow UI |
| Deploy | Docker only | Python + deps | Airflow stack |

**Vantagens Docker Compose:**
- ✅ Setup automático completo
- ✅ Isolamento total
- ✅ Fácil replicação
- ✅ MinIO incluído  
- ✅ Configuração via arquivo
- ✅ Logs centralizados

---

## 🚀 Começar Agora

```bash
# Clone e execute teste rápido
git clone <repo>
cd lhbench-v2
chmod +x run-docker.sh
./run-docker.sh quick
```

Em poucos minutos você terá um benchmark TPC-H completo rodando! 🎉