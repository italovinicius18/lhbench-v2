#!/usr/bin/env python3
"""
Script super simples para gerar dados TPC-H Scale Factor 1
"""

import os
import sys
import duckdb
from datetime import datetime

def generate_tpch_data():
    """Gerar dados TPC-H scale factor 1 com DuckDB"""
    
    print("Gerando dados TPC-H Scale Factor 1...")
    
    # Conectar DuckDB
    conn = duckdb.connect()
    
    # Instalar e carregar extensões
    conn.execute("INSTALL tpch;")
    conn.execute("LOAD tpch;")
    conn.execute("INSTALL aws;")
    conn.execute("LOAD aws;")
    
    # Configurar S3/MinIO
    minio_endpoint = "localhost:9000"
    access_key = "minioadmin"
    secret_key = "minioadmin"
    
    conn.execute(f"SET s3_endpoint = '{minio_endpoint}';")
    conn.execute(f"SET s3_access_key_id = '{access_key}';")
    conn.execute(f"SET s3_secret_access_key = '{secret_key}';")
    conn.execute("SET s3_use_ssl = false;")
    conn.execute("SET s3_url_style = 'path';")
    
    # Tabelas TPC-H
    tables = [
        'customer', 'lineitem', 'nation', 'orders', 
        'part', 'partsupp', 'region', 'supplier'
    ]
    
    # Gerar e exportar cada tabela
    for table in tables:
        print(f"  - Gerando tabela {table}...")
        
        # Criar tabela TPC-H
        conn.execute(f"CALL dbgen(sf = 1);")
        
        # Exportar para MinIO como Parquet
        s3_path = f"s3://bronze/sf_1/parquet/{table}/{table}.parquet"
        
        query = f"""
        COPY (SELECT * FROM {table}) 
        TO '{s3_path}' 
        (FORMAT PARQUET)
        """
        
        try:
            conn.execute(query)
            print(f"    ✅ {table} exportada para {s3_path}")
        except Exception as e:
            print(f"    ❌ Erro ao exportar {table}: {str(e)}")
    
    conn.close()
    print("✅ Geração de dados Bronze concluída!")

if __name__ == "__main__":
    generate_tpch_data()