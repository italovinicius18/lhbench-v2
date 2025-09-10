# Spark Lakehouse Engines Implementation

Este módulo implementa os engines de lakehouse (Delta Lake, Apache Iceberg, Apache Hudi) para o benchmark LHBench-v2.

## Componentes Implementados

### 1. Base Engine (`base_engine.py`)
- Classe abstrata base para todos os engines de lakehouse
- Define interface comum para operações como:
  - Criação de tabelas
  - Carregamento de dados
  - Execução de queries
  - Operações de merge
  - Otimização de tabelas
  - Obtenção de metadados

### 2. Delta Lake Engine (`delta_engine.py`)
- Implementação para Delta Lake
- Suporte a operações ACID
- Funcionalidades específicas:
  - Time travel queries
  - Vacuum operations
  - Z-ordering optimization
  - Delta merge operations

### 3. Iceberg Engine (`iceberg_engine.py`)
- Implementação para Apache Iceberg
- Suporte a múltiplos catálogos
- Funcionalidades específicas:
  - Schema evolution
  - Snapshot management
  - Time travel queries
  - Table maintenance operations

### 4. Hudi Engine (`hudi_engine.py`)
- Implementação para Apache Hudi
- Suporte a COW e MOR table types
- Funcionalidades específicas:
  - Incremental queries
  - Compaction operations
  - Clustering optimization
  - Timeline management

### 5. Engine Factory (`engine_factory.py`)
- Factory pattern para criação de engines
- MultiEngineManager para gerenciar múltiplos engines
- Funcionalidades para comparação de performance

### 6. Spark Manager (`spark_manager.py`)
- Gerenciamento de sessões Spark
- Configuração automática de extensões
- Validação de engines
- Configuração de catalogs

### 7. Configuration (`config.py`)
- Configurações padrão para cada engine
- Configurações específicas para TPC-DS
- Configurações de performance
