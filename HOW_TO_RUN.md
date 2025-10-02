# 🚀 Como Rodar o Lakehouse Benchmark

Guia rápido para executar o projeto agora que está configurado.

---

## ✅ Status Atual

O cluster Spark está **RODANDO** agora! 

```
✅ Spark Master: http://localhost:8080
✅ Worker 1: http://localhost:8081
✅ Worker 2: http://localhost:8082
```

---

## 📋 Opção 1: Testar Manualmente (Recomendado Agora)

Como o tpchgen ainda está compilando, você pode testar o Spark cluster manualmente:

### 1️⃣ Verificar Cluster
```bash
# Ver status dos containers
docker compose ps

# Ver logs do master
docker compose logs spark-master | tail -20

# Acessar Spark UI no navegador
# http://localhost:8080
```

### 2️⃣ Testar Spark Interativo
```bash
# Abrir shell no master
docker compose exec spark-master bash

# Dentro do container, testar PySpark
pyspark

# No PySpark prompt:
>>> data = [1, 2, 3, 4, 5]
>>> rdd = sc.parallelize(data)
>>> rdd.sum()
# Deve retornar: 15

>>> spark
# Deve mostrar info do SparkSession

>>> exit()
```

### 3️⃣ Testar Geração de Dados Simples
```bash
# Criar um DataFrame de teste e salvar como Parquet
docker compose exec spark-master pyspark <<EOF
from pyspark.sql import Row

# Criar dados de teste
data = [
    Row(id=1, name="Customer1", city="NYC"),
    Row(id=2, name="Customer2", city="LA"),
    Row(id=3, name="Customer3", city="SF")
]

df = spark.createDataFrame(data)
df.show()

# Salvar como Parquet
df.write.mode("overwrite").parquet("/data/bronze/test_data")
print("✅ Dados salvos em /data/bronze/test_data")
