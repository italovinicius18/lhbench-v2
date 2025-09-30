#!/usr/bin/env python3
"""
Script simples para converter dados Bronze para Delta Lake
"""

import os
from pyspark.sql import SparkSession

def create_spark_session():
    """Criar sessão Spark com configurações para Delta Lake"""
    
    return SparkSession.builder \
        .appName("LHBench-Delta-Converter") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

def convert_table_to_delta(spark, table_name):
    """Converter uma tabela específica para Delta"""
    
    print(f"  - Convertendo {table_name} para Delta...")
    
    # Ler dados Bronze (Parquet)
    bronze_path = f"s3a://bronze/sf_1/parquet/{table_name}"
    df = spark.read.parquet(bronze_path)
    
    # Salvar como Delta
    silver_path = f"s3a://silver/sf_1/delta/{table_name}"
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    
    print(f"    ✅ {table_name} convertido para Delta em {silver_path}")

def main():
    """Função principal"""
    
    print("Convertendo dados Bronze para Delta Lake...")
    
    # Criar sessão Spark
    spark = create_spark_session()
    
    # Tabelas TPC-H
    tables = [
        'customer', 'lineitem', 'nation', 'orders',
        'part', 'partsupp', 'region', 'supplier'
    ]
    
    # Converter cada tabela
    for table in tables:
        try:
            convert_table_to_delta(spark, table)
        except Exception as e:
            print(f"    ❌ Erro ao converter {table}: {str(e)}")
    
    # Encerrar Spark
    spark.stop()
    
    print("✅ Conversão para Delta Lake concluída!")

if __name__ == "__main__":
    main()