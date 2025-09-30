#!/bin/bash
# Script para executar LHBench standalone com Docker Compose

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 LHBench TPC-H Benchmark - Docker Compose${NC}"
echo "=============================================="

# Verificar se Docker está rodando
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker não está rodando. Inicie o Docker primeiro.${NC}"
    exit 1
fi

# Verificar se docker compose está disponível  
if ! docker compose version >/dev/null 2>&1; then
    echo -e "${RED}❌ docker compose não encontrado. Instale Docker Compose primeiro.${NC}"
    exit 1
fi

# Criar arquivo .env se não existir
if [ ! -f .env ]; then
    echo -e "${YELLOW}📝 Criando arquivo .env padrão...${NC}"
    cp .env.example .env
fi

# Função para executar comando
run_benchmark() {
    local scale_factor=${1:-1}
    local formats=${2:-"delta"}
    local iterations=${3:-1}
    local extra_args=${4:-""}
    
    echo -e "${BLUE}📊 Executando benchmark:${NC}"
    echo "   Scale Factor: $scale_factor"
    echo "   Formatos: $formats"
    echo "   Iterações: $iterations"
    echo ""
    
    # Definir variáveis de ambiente
    export SCALE_FACTOR=$scale_factor
    export BENCHMARK_FORMATS=$formats
    export BENCHMARK_ITERATIONS=$iterations
    
    # Construir comando
    local cmd="python -m standalone.main --scale-factor $scale_factor --formats $formats --iterations $iterations $extra_args"
    
    echo -e "${GREEN}🔧 Iniciando serviços...${NC}"
    docker compose -f docker-compose.standalone.yml up -d minio minio-client
    
    echo -e "${GREEN}⏳ Aguardando MinIO...${NC}"
    sleep 10
    
    echo -e "${GREEN}🏃 Executando LHBench...${NC}"
    docker compose -f docker-compose.standalone.yml run --rm lhbench $cmd
}

# Função para mostrar logs
show_logs() {
    echo -e "${BLUE}📋 Mostrando logs do MinIO...${NC}"
    docker compose -f docker-compose.standalone.yml logs minio
}

# Função para limpar
cleanup() {
    echo -e "${YELLOW}🧹 Parando e removendo containers...${NC}"
    docker compose -f docker-compose.standalone.yml down -v
    echo -e "${GREEN}✅ Limpeza concluída${NC}"
}

# Função para acessar container
shell() {
    echo -e "${BLUE}🐚 Acessando shell do container LHBench...${NC}"
    docker compose -f docker-compose.standalone.yml run --rm lhbench bash
}

# Função para build
build() {
    echo -e "${BLUE}🔨 Construindo imagem LHBench...${NC}"
    docker compose -f docker-compose.standalone.yml build lhbench
}

# Função para mostrar status
status() {
    echo -e "${BLUE}📊 Status dos serviços:${NC}"
    docker compose -f docker-compose.standalone.yml ps
    echo ""
    echo -e "${BLUE}💾 Volumes:${NC}"
    docker volume ls | grep lhbench || echo "Nenhum volume encontrado"
}

# Função para mostrar ajuda
show_help() {
    echo "Uso: $0 [COMANDO] [OPÇÕES]"
    echo ""
    echo "Comandos:"
    echo "  run [SF] [FORMATOS] [ITER] [ARGS]  Executar benchmark"
    echo "  quick                              Teste rápido (SF=1, Delta, 1 iter)"
    echo "  full                               Benchmark completo (SF=10, todos formatos)"
    echo "  bronze-only [SF]                   Apenas geração bronze"
    echo "  silver-only [SF] [FORMATOS]        Apenas conversão silver"
    echo "  benchmark-only [SF] [FORMATOS]     Apenas benchmark"
    echo "  build                              Construir imagem"
    echo "  shell                              Acessar shell do container"
    echo "  logs                               Mostrar logs"
    echo "  status                             Status dos serviços"
    echo "  cleanup                            Parar e remover tudo"
    echo "  help                               Mostrar esta ajuda"
    echo ""
    echo "Exemplos:"
    echo "  $0 quick                          # Teste rápido"
    echo "  $0 run 10 \"delta iceberg\" 3      # SF=10, Delta+Iceberg, 3 iterações"
    echo "  $0 bronze-only 100                # Gerar bronze SF=100"
    echo "  $0 full                           # Benchmark completo"
}

# Parse dos argumentos
case "${1:-help}" in
    "run")
        run_benchmark "${2:-1}" "${3:-delta}" "${4:-1}" "${5:-}"
        ;;
    "quick")
        echo -e "${GREEN}🚀 Executando teste rápido...${NC}"
        run_benchmark 1 "delta" 1 "--force-bronze"
        ;;
    "full")
        echo -e "${GREEN}🚀 Executando benchmark completo...${NC}"
        run_benchmark 10 "delta iceberg hudi" 3
        ;;
    "bronze-only")
        echo -e "${GREEN}🥉 Executando apenas Bronze...${NC}"
        run_benchmark "${2:-1}" "delta" 1 "--bronze-only"
        ;;
    "silver-only")
        echo -e "${GREEN}🥈 Executando apenas Silver...${NC}"
        run_benchmark "${2:-1}" "${3:-delta}" 1 "--silver-only"
        ;;
    "benchmark-only")
        echo -e "${GREEN}🥇 Executando apenas Benchmark...${NC}"
        run_benchmark "${2:-1}" "${3:-delta}" 1 "--benchmark-only"
        ;;
    "build")
        build
        ;;
    "shell")
        shell
        ;;
    "logs")
        show_logs
        ;;
    "status")
        status
        ;;
    "cleanup")
        cleanup
        ;;
    "help"|*)
        show_help
        ;;
esac