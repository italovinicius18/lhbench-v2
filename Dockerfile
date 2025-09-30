# Dockerfile para LHBench Standalone
FROM python:3.10-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    curl \
    wget \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Configurar Java
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fonte
COPY standalone/ ./standalone/
COPY README.md .

# Criar diretórios necessários
RUN mkdir -p /app/results /app/logs

# Configurar variáveis de ambiente padrão
ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Verificar instalação
RUN python -c "import pyspark; print('PySpark OK')" && \
    python -c "import duckdb; print('DuckDB OK')" && \
    java -version

# Comando padrão
CMD ["python", "-m", "standalone.main", "--help"]