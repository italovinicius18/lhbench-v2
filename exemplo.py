"""
Script de exemplo para executar o LHBench standalone
"""
import os
import sys

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from standalone import run_full_pipeline, config

def exemplo_basico():
    """Exemplo básico - pipeline completo com SF=1"""
    print("🚀 Executando LHBench com Scale Factor 1")
    
    # Configurar para teste rápido
    results = run_full_pipeline(
        scale_factor=1,
        benchmark_formats=["delta"],  # Apenas Delta para teste
        benchmark_iterations=1,       # 1 iteração apenas
        force_regenerate_bronze=False
    )
    
    print(f"\n📊 Status final: {results['status']}")
    return results

def exemplo_completo():
    """Exemplo completo - todos os formatos"""
    print("🚀 Executando LHBench completo")
    
    results = run_full_pipeline(
        scale_factor=10,
        benchmark_formats=["delta", "iceberg", "hudi"],
        benchmark_iterations=3,
        force_regenerate_bronze=False,
        force_recreate_silver=False
    )
    
    print(f"\n📊 Status final: {results['status']}")
    
    # Mostrar ranking se benchmark executado
    if results["status"] == "success" and "benchmark" in results["stages"]:
        benchmark_data = results["stages"]["benchmark"]
        if "comparison" in benchmark_data and "format_ranking" in benchmark_data["comparison"]:
            print("\n🏆 Ranking de Performance:")
            for i, fmt in enumerate(benchmark_data["comparison"]["format_ranking"], 1):
                print(f"   {i}. {fmt['format'].upper()}: {fmt['overall_avg_time']:.2f}s")
    
    return results

def exemplo_bronze_apenas():
    """Exemplo - apenas geração bronze"""
    from standalone import generate_bronze_data
    
    print("🥉 Executando apenas geração Bronze")
    
    results = generate_bronze_data(
        scale_factor=1,
        force_regenerate=True
    )
    
    print(f"\n📊 Status: {results['status']}")
    if results['status'] == 'success':
        print(f"📁 Tabelas geradas: {results['tables_generated']}")
        print(f"⏱️  Tempo: {results['duration_seconds']:.2f}s")
    
    return results

def exemplo_silver_apenas():
    """Exemplo - apenas conversão silver"""
    from standalone import convert_to_silver_format
    
    print("🥈 Executando apenas conversão Silver (Delta)")
    
    results = convert_to_silver_format(
        format_name="delta",
        scale_factor=1,
        force_recreate=False
    )
    
    print(f"\n📊 Status: {results['status']}")
    if results['status'] == 'success':
        print(f"📁 Tabelas convertidas: {results['tables_successful']}/{results['tables_processed']}")
        print(f"⏱️  Tempo: {results['duration_seconds']:.2f}s")
    
    return results

def exemplo_benchmark_apenas():
    """Exemplo - apenas benchmark"""
    from standalone import benchmark_format
    
    print("🥇 Executando apenas Benchmark (Delta)")
    
    results = benchmark_format(
        format_name="delta",
        scale_factor=1,
        iterations=1,
        run_warmup=True,
        timeout_minutes=10
    )
    
    print(f"\n📊 Status: {results['status']}")
    if results['status'] == 'success':
        print(f"📁 Queries executadas: {results['successful_queries']}/{results['total_queries']}")
        print(f"⏱️  Tempo: {results['duration_seconds']:.2f}s")
    
    return results

def exemplo_configuracao_customizada():
    """Exemplo com configuração customizada"""
    from standalone import LHBenchConfig, run_full_pipeline
    
    # Criar configuração customizada
    custom_config = LHBenchConfig(
        scale_factor=1,
        minio_endpoint="http://localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        benchmark_formats=["delta", "iceberg"],
        benchmark_iterations=2,
        spark_executor_memory="2g",
        spark_driver_memory="1g",
        results_path="./custom_results"
    )
    
    print("🔧 Executando com configuração customizada")
    print(f"   Scale Factor: {custom_config.scale_factor}")
    print(f"   Formatos: {custom_config.benchmark_formats}")
    print(f"   Spark Memory: {custom_config.spark_executor_memory}")
    
    # Aplicar configuração (substituir global temporariamente)
    import standalone.config
    original_config = standalone.config.config
    standalone.config.config = custom_config
    
    try:
        results = run_full_pipeline()
        print(f"\n📊 Status final: {results['status']}")
        return results
    finally:
        # Restaurar configuração original
        standalone.config.config = original_config

if __name__ == "__main__":
    print("LHBench Standalone - Exemplos de Uso")
    print("="*50)
    
    # Verificar argumentos
    if len(sys.argv) > 1:
        exemplo = sys.argv[1].lower()
        
        if exemplo == "basico":
            exemplo_basico()
        elif exemplo == "completo":
            exemplo_completo()
        elif exemplo == "bronze":
            exemplo_bronze_apenas()
        elif exemplo == "silver":
            exemplo_silver_apenas()
        elif exemplo == "benchmark":
            exemplo_benchmark_apenas()
        elif exemplo == "custom":
            exemplo_configuracao_customizada()
        else:
            print(f"❌ Exemplo '{exemplo}' não reconhecido")
            print("Exemplos disponíveis: basico, completo, bronze, silver, benchmark, custom")
    else:
        print("Uso: python exemplo.py [basico|completo|bronze|silver|benchmark|custom]")
        print("\nExecutando exemplo básico...")
        exemplo_basico()