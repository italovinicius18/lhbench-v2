#!/bin/bash
# Setup script for LHBench v2

set -e

echo "🚀 LHBench v2 Setup"
echo "==================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "Makefile" ] || [ ! -f "setup.py" ]; then
    print_error "Please run this script from the LHBench v2 root directory"
    exit 1
fi

print_status "Checking system requirements..."

# Check Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is required but not installed"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
print_success "Python $PYTHON_VERSION found"

# Check Java
if ! command -v java &> /dev/null; then
    print_error "Java is required for Spark but not installed"
    print_warning "Please install Java 8 or 11:"
    print_warning "  sudo apt-get update && sudo apt-get install openjdk-11-jdk"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n1 | cut -d'"' -f2)
print_success "Java $JAVA_VERSION found"

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is required for MinIO but not installed"
    print_warning "Please install Docker first"
    exit 1
fi

if ! docker ps &> /dev/null; then
    print_error "Docker is not running or you don't have permission"
    print_warning "Make sure Docker is running and you're in the docker group"
    exit 1
fi

print_success "Docker is available"

# Check Make
if ! command -v make &> /dev/null; then
    print_error "Make is required but not installed"
    print_warning "Install with: sudo apt-get install build-essential"
    exit 1
fi

print_success "Make is available"

print_status "Setting up Python environment..."

# Check if venv exists
if [ ! -d "venv" ]; then
    print_status "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate venv
source venv/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip

# Install requirements
print_status "Installing Python dependencies..."
pip install -r requirements.txt

print_success "Python environment setup complete"

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    print_status "Creating .env file from template..."
    cp .env.example .env
    print_warning "Please review and customize .env file for your environment"
fi

# Setup data directory
print_status "Setting up data directory..."
source .env
mkdir -p "$DATA_PATH"
print_success "Data directory created: $DATA_PATH"

print_status "Running validation..."
python scripts/validate_implementation.py

print_success "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Review .env file and customize settings"
echo "  2. Run 'make dev-setup' to start MinIO"
echo "  3. Run 'make data-generate' to create test data"
echo "  4. Run 'make benchmark-run' to start benchmarking"
echo ""
echo "Quick start: make quick-start"
echo "Help: make help"
