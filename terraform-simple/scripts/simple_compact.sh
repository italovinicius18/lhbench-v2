#!/bin/bash
set -e
LOG_FILE="/var/log/lhbench-setup.log"
exec > >(tee -a $LOG_FILE) 2>&1
echo "Starting LHBench setup - $(date)"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y curl git python3-pip docker.io docker-compose
usermod -aG docker ubuntu
systemctl enable docker
systemctl start docker

# Instalar Astro CLI usando o método oficial
curl -sSL https://install.astronomer.io | sudo bash -s -- -y
chown root:root /usr/local/bin/astro
chmod +x /usr/local/bin/astro

mkdir -p /opt/lhbench
chown -R ubuntu:ubuntu /opt/lhbench
cd /opt/lhbench
git clone -b feat/tpch https://github.com/italovinicius18/lhbench-v2.git
cd lhbench-v2

# Criar diretório para MinIO
mkdir -p /opt/lhbench/data/minio
chown -R ubuntu:ubuntu /opt/lhbench/data

# Configurar variáveis de ambiente completas
cat > .env << EOF
AIRFLOW_UID=50000
MINIO_DATA_PATH=/opt/lhbench/data/minio
SPARK_EXECUTOR_MEMORY=${spark_executor_memory}
SPARK_DRIVER_MEMORY=${spark_driver_memory}
EOF

# Inicializar projeto Astro como usuário ubuntu
sudo -u ubuntu bash -c 'cd /opt/lhbench/lhbench-v2 && echo "y" | astro dev init --name lhbench-tpch'

# Configurar Astro para expor portas externamente
sudo -u ubuntu bash -c 'cd /opt/lhbench/lhbench-v2 && astro config set airflow.expose_port true'

# Exportar variáveis e iniciar serviços
sudo -u ubuntu bash -c 'cd /opt/lhbench/lhbench-v2 && export $(cat .env | xargs) && astro dev start --wait 300'
# Aguardar serviços iniciarem e configurar MinIO
(
  sleep 120
  curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
  chmod +x mc
  mv mc /usr/local/bin/
  
  # Configurar MinIO com credenciais corretas
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc mb local/bronze --ignore-existing
  mc mb local/silver --ignore-existing
  mc mb local/gold --ignore-existing
  mc anonymous set public local/bronze
  mc anonymous set public local/silver
  mc anonymous set public local/gold
  echo "MinIO buckets configured successfully"
  
  # Aplicar configurações do airflow_settings.yaml
  sudo -u ubuntu bash -c 'cd /opt/lhbench/lhbench-v2 && astro dev object import'
  echo "Airflow settings imported successfully"
) &

touch /var/log/lhbench-setup-complete
echo "LHBench setup complete - $(date)"
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)
echo "🌐 Airflow UI: http://$PUBLIC_IP:8080"
echo "📦 MinIO Console: http://$PUBLIC_IP:9001"
echo "🔑 Airflow Login: admin / admin"
echo "🔑 MinIO Login: minioadmin / minioadmin"
echo "✅ System ready for TPC-H benchmarks!"