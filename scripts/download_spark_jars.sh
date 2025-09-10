#!/bin/bash

# Script para baixar dependências JAR necessárias para os lakehouse engines
set -e

JARS_DIR="${1:-./jars}"
FORCE_DOWNLOAD="${2:-false}"

echo "📦 Baixando JARs dos lakehouse engines..."
echo "📁 Diretório de destino: $JARS_DIR"

# Criar diretório se não existir
mkdir -p "$JARS_DIR"

# Função para baixar JAR se não existir ou se forçado
download_jar() {
    local url="$1"
    local filename="$2"
    local filepath="$JARS_DIR/$filename"
    
    if [[ "$FORCE_DOWNLOAD" == "true" ]] || [[ ! -f "$filepath" ]]; then
        echo "⬇️  Baixando $filename..."
        curl -L -o "$filepath" "$url"
        echo "✅ $filename baixado com sucesso"
    else
        echo "⏭️  $filename já existe, pulando..."
    fi
}

# Delta Lake JARs
echo "🔷 Baixando Delta Lake JARs..."
download_jar "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar" "delta-core_2.12-2.4.0.jar"
download_jar "https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar" "delta-storage-2.4.0.jar"

# Iceberg JARs
echo "🧊 Baixando Iceberg JARs..."
download_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" "iceberg-spark-runtime-3.5_2.12-1.4.2.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar" "iceberg-aws-1.4.2.jar"

# Hudi JARs
echo "🏠 Baixando Hudi JARs..."
download_jar "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.0/hudi-spark3.4-bundle_2.12-0.14.0.jar" "hudi-spark3.4-bundle_2.12-0.14.0.jar"

# AWS S3 JARs
echo "☁️  Baixando AWS S3 JARs..."
download_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" "hadoop-aws-3.3.4.jar"
download_jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" "aws-java-sdk-bundle-1.12.262.jar"

# Listar JARs baixados
echo ""
echo "📋 JARs disponíveis em $JARS_DIR:"
ls -la "$JARS_DIR"/*.jar

echo ""
echo "✅ Todos os JARs foram baixados com sucesso!"
echo "💡 Para usar com Spark, adicione ao classpath:"
echo "   export SPARK_JARS_DIR=$JARS_DIR"
echo "   ou configure spark.jars.dir=$JARS_DIR"
