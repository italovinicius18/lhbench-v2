#!/bin/bash

# Lakehouse Benchmark Runner Script
# Simple wrapper around Docker Compose for easy execution

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    print_success "Docker installed: $(docker --version)"

    # Check Docker Compose
    if ! command -v docker compose &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    print_success "Docker Compose installed: $(docker compose --version)"

    # Check .env file
    if [ ! -f .env ]; then
        print_warning ".env file not found"
        print_info "Creating from .env.example..."
        cp .env.example .env
        print_success ".env created - please edit configuration"
        exit 0
    fi
    print_success ".env file found"

    echo ""
}

# Show configuration
show_config() {
    print_header "Configuration"

    if [ -f .env ]; then
        echo -e "${GREEN}Scale Factor:${NC} $(grep TPCH_SCALE_FACTOR .env | cut -d '=' -f2)"
        echo -e "${GREEN}Frameworks:${NC} $(grep '^FRAMEWORKS=' .env | cut -d '=' -f2)"
        echo -e "${GREEN}Worker Cores:${NC} $(grep SPARK_WORKER_CORES .env | cut -d '=' -f2)"
        echo -e "${GREEN}Worker Memory:${NC} $(grep SPARK_WORKER_MEMORY .env | cut -d '=' -f2)"
    fi

    echo ""
}

# Start services
start_services() {
    print_header "Starting Services"

    print_info "Starting Spark cluster..."
    docker compose up -d spark-master spark-worker-1 spark-worker-2

    print_info "Waiting for services to be ready..."
    sleep 10

    # Check if services are running
    if docker compose ps | grep -q "Up"; then
        print_success "Services started successfully"
        print_info "Spark Master UI: http://localhost:8080"
    else
        print_error "Failed to start services"
        docker compose logs
        exit 1
    fi

    echo ""
}

# Run benchmark
run_benchmark() {
    print_header "Running Benchmark"

    print_info "Starting benchmark orchestrator..."

    docker compose --profile benchmark run --rm benchmark-orchestrator

    if [ $? -eq 0 ]; then
        print_success "Benchmark completed successfully"
    else
        print_error "Benchmark failed"
        print_info "Check logs with: docker compose logs benchmark-orchestrator"
        exit 1
    fi

    echo ""
}

# Show results
show_results() {
    print_header "Results Location"

    DATA_DIR="/mnt/c/Users/italo/WSL_DATA/lakehouse-data"

    echo -e "${GREEN}Bronze (Generated Data):${NC}"
    echo "  $DATA_DIR/bronze/tpch/"

    echo -e "${GREEN}Silver (Framework Tables):${NC}"
    echo "  $DATA_DIR/silver/iceberg/"
    echo "  $DATA_DIR/silver/delta/"
    echo "  $DATA_DIR/silver/hudi/"

    echo -e "${GREEN}Gold (Metrics & Reports):${NC}"
    echo "  $DATA_DIR/gold/metrics/"

    echo -e "${GREEN}Local Results:${NC}"
    echo "  ./results/metrics/"
    echo "  ./results/logs/"

    echo ""
}

# Stop services
stop_services() {
    print_header "Stopping Services"

    docker compose down

    print_success "Services stopped"
    echo ""
}

# Main execution
main() {
    echo ""
    print_header "🚀 Lakehouse Benchmark"
    echo ""

    # Check prerequisites
    check_prerequisites

    # Show configuration
    show_config

    # Ask for confirmation
    read -p "$(echo -e ${YELLOW}Start benchmark? [y/N]: ${NC})" -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Cancelled by user"
        exit 0
    fi

    # Start services
    start_services

    # Run benchmark
    run_benchmark

    # Show results
    show_results

    # Ask to stop services
    read -p "$(echo -e ${YELLOW}Stop services? [y/N]: ${NC})" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        stop_services
    else
        print_info "Services left running"
        print_info "Stop with: docker compose down"
    fi

    print_success "Done!"
}

# Handle command line arguments
case "${1:-}" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    status)
        docker compose ps
        ;;
    logs)
        docker compose logs -f
        ;;
    *)
        main
        ;;
esac
