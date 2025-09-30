#!/bin/bash
# Script para executar benchmark completo LHBench
# Scale Factor 1 - Todos os formatos

echo "==================================================="
echo "LHBench TPC-H Benchmark Completo"
echo "==================================================="
echo "Scale Factor: 1"
echo "Formatos: delta, iceberg, hudi"
echo "Iterações: 3"
echo ""

cd /home/italo/lhbench-v2/standalone

# 1. Gerar dados Bronze
echo "1️⃣ Gerando dados Bronze (TPC-H)..."
python bronze_generator.py --scale-factor 1 --force
if [ $? -ne 0 ]; then
    echo "❌ Erro na geração Bronze"
    exit 1
fi
echo "✅ Dados Bronze gerados"
echo ""

# 2. Converter para formatos Silver
echo "2️⃣ Convertendo para formatos Silver..."

echo "   - Convertendo para Delta..."
python silver_converter.py --scale-factor 1 --format delta --force
if [ $? -ne 0 ]; then
    echo "❌ Erro na conversão Delta"
    exit 1
fi

echo "   - Convertendo para Iceberg..."  
python silver_converter.py --scale-factor 1 --format iceberg --force
if [ $? -ne 0 ]; then
    echo "❌ Erro na conversão Iceberg"
    exit 1
fi

echo "   - Convertendo para Hudi..."
python silver_converter.py --scale-factor 1 --format hudi --force
if [ $? -ne 0 ]; then
    echo "❌ Erro na conversão Hudi"
    exit 1
fi

echo "✅ Conversões Silver concluídas"
echo ""

# 3. Executar benchmarks
echo "3️⃣ Executando benchmarks TPC-H..."

echo "   - Benchmark Delta..."
python benchmark_executor.py --scale-factor 1 --format delta --iterations 3
if [ $? -ne 0 ]; then
    echo "❌ Erro no benchmark Delta"
    exit 1
fi

echo "   - Benchmark Iceberg..."
python benchmark_executor.py --scale-factor 1 --format iceberg --iterations 3
if [ $? -ne 0 ]; then
    echo "❌ Erro no benchmark Iceberg"
    exit 1
fi

echo "   - Benchmark Hudi..."
python benchmark_executor.py --scale-factor 1 --format hudi --iterations 3
if [ $? -ne 0 ]; then
    echo "❌ Erro no benchmark Hudi"
    exit 1
fi

echo "✅ Benchmarks concluídos"
echo ""

echo "==================================================="
echo "✅ BENCHMARK COMPLETO FINALIZADO COM SUCESSO!"
echo "==================================================="
echo ""
echo "Verifique os resultados nos diretórios:"
echo "- ../results/ (se houver)"
echo "- ../benchmark_results_*/"
echo ""