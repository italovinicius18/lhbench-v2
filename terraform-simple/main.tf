# ============================================================================
# LHBench TPC-H - Usando Recursos Padrão AWS
# ============================================================================

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================================================
# Variables
# ============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "lhbench-simple"
}

variable "key_name" {
  description = "AWS Key Pair name for EC2 access"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.large"  # Testando com instância menor primeiro
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the instance"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ============================================================================
# Data Sources - Aproveitando recursos existentes
# ============================================================================

# Usar VPC padrão
data "aws_vpc" "default" {
  default = true
}

# Usar subnet padrão
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_subnet" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = ["us-east-1a"]  # Especificar zona que suporta t3.large
  }
}

# AMI Ubuntu mais recente
data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"]
}

# ============================================================================
# Security Group - Apenas o necessário
# ============================================================================

resource "aws_security_group" "lhbench" {
  name_prefix = "${var.project_name}-"
  vpc_id      = data.aws_vpc.default.id
  description = "Security group for LHBench instance"

  # SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "SSH access"
  }

  # Airflow UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "Airflow Web UI"
  }

  # MinIO Console
  ingress {
    from_port   = 9001
    to_port     = 9001
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "MinIO Console"
  }

  # MinIO API
  ingress {
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "MinIO API"
  }

  # Saída completa
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }

  tags = {
    Name    = "${var.project_name}-sg"
    Project = var.project_name
  }

  lifecycle {
    create_before_destroy = true
  }
}

# ============================================================================
# IAM - Comentado pois requer permissões especiais
# ============================================================================

# Removido IAM roles pois requer permissões administrativas
# Para produção, criar roles manualmente ou ter permissões IAM

# ============================================================================
# User Data Script
# ============================================================================

locals {
  user_data = base64encode(templatefile("${path.module}/scripts/simple_compact.sh", {
    spark_executor_memory  = "2g"  # Ajustado para t3.large (8GB RAM)
    spark_driver_memory    = "2g"
  }))
}

# ============================================================================
# EC2 Instance - Instância poderosa, setup simples
# ============================================================================

resource "aws_instance" "lhbench" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name      = var.key_name
  subnet_id     = data.aws_subnet.default.id

  vpc_security_group_ids      = [aws_security_group.lhbench.id]
  associate_public_ip_address = true

  user_data                   = local.user_data
  user_data_replace_on_change = false

  # Volume raiz maior para aproveitar a instância
  root_block_device {
    volume_type           = "gp3"
    volume_size           = 200  # 200GB para aproveitar melhor
    delete_on_termination = true
    encrypted             = true
    throughput            = 250  # Maior throughput
    iops                  = 4000 # Mais IOPS
  }

  # Tags
  tags = {
    Name        = "${var.project_name}-instance"
    Project     = var.project_name
    Environment = "simple"
    Purpose     = "LHBench TPC-H High Performance"
    InstanceType = var.instance_type
  }

  lifecycle {
    create_before_destroy = false
  }
}

# ============================================================================
# Outputs
# ============================================================================

output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.lhbench.id
}

output "public_ip" {
  description = "Public IP address of the instance"
  value       = aws_instance.lhbench.public_ip
}

output "public_dns" {
  description = "Public DNS name of the instance"
  value       = aws_instance.lhbench.public_dns
}

output "private_ip" {
  description = "Private IP address of the instance"
  value       = aws_instance.lhbench.private_ip
}

output "availability_zone" {
  description = "Availability zone of the instance"
  value       = aws_instance.lhbench.availability_zone
}

output "vpc_id" {
  description = "VPC ID (using default VPC)"
  value       = data.aws_vpc.default.id
}

output "subnet_id" {
  description = "Subnet ID (using default subnet)"
  value       = data.aws_subnet.default.id
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.lhbench.id
}

output "airflow_url" {
  description = "Airflow Web UI URL"
  value       = "http://${aws_instance.lhbench.public_ip}:8080"
}

output "minio_console_url" {
  description = "MinIO Console URL"
  value       = "http://${aws_instance.lhbench.public_ip}:9001"
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.lhbench.public_ip}"
}

output "ssm_command" {
  description = "AWS SSM command to connect (no SSH key needed)"
  value       = "aws ssm start-session --target ${aws_instance.lhbench.id}"
}

output "instance_specs" {
  description = "Instance specifications"
  value = {
    type      = var.instance_type
    vcpus     = "2"
    memory    = "8 GB"
    storage   = "200 GB EBS"
    network   = "Up to 5 Gbps"
    cost_hour = "$0.0928"
  }
}

output "estimated_monthly_cost" {
  description = "Estimated monthly cost in USD"
  value = {
    instance_cost_24_7    = "$66.82"   # 24/7
    instance_cost_8h_day  = "$22.27"   # 8h/day
    storage_cost          = "$16.00"   # 200GB gp3
    total_cost_24_7       = "$82.82"
    total_cost_8h_day     = "$38.27"
    note                  = "Estimates for t3.large. Much more affordable for testing!"
  }
}

output "connection_info" {
  description = "Connection information"
  value = {
    airflow = {
      url      = "http://${aws_instance.lhbench.public_ip}:8080"
      username = "admin"
      password = "admin123"
    }
    minio = {
      console_url = "http://${aws_instance.lhbench.public_ip}:9001"
      username    = "minioadmin"
      password    = "minioadmin123"
    }
    ssh = {
      command = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.lhbench.public_ip}"
      user    = "ubuntu"
    }
    ssm = {
      command = "aws ssm start-session --target ${aws_instance.lhbench.id}"
      note    = "No SSH key needed, works through AWS Console"
    }
  }
}

output "performance_info" {
  description = "Expected performance characteristics"
  value = {
    tpch_scale_factor_max = "1-2"
    bronze_generation     = "3-5 minutes (SF=1)"
    silver_conversion     = "5-10 minutes each (SF=1)"
    benchmark_execution   = "15-30 minutes (SF=1)"
    total_runtime         = "30-60 minutes (SF=1)"
    spark_config = {
      executor_memory = "2g"
      driver_memory   = "2g"
      executor_cores  = "1"
      max_executors   = "2"
    }
  }
}

output "cost_optimization_tips" {
  description = "Tips to optimize costs"
  value = [
    "💡 Stop when not in use: aws ec2 stop-instances --instance-ids ${aws_instance.lhbench.id}",
    "💡 Start when needed: aws ec2 start-instances --instance-ids ${aws_instance.lhbench.id}",
    "💡 Use Spot instances for 50-70% discount (add spot_price parameter)",
    "💡 Schedule automatic stop/start with EventBridge",
    "💡 Monitor with CloudWatch for usage optimization",
    "💡 Terminate when done: terraform destroy"
  ]
}

output "setup_status" {
  description = "How to check setup progress"
  value = {
    ssh_logs      = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.lhbench.public_ip} 'tail -f /var/log/lhbench-simple-setup.log'"
    ssm_logs      = "aws ssm start-session --target ${aws_instance.lhbench.id} --document-name AWS-StartInteractiveCommand --parameters command='tail -f /var/log/lhbench-simple-setup.log'"
    status_check  = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.lhbench.public_ip} '/opt/lhbench/check-status.sh'"
    completion    = "Setup complete when file /var/log/lhbench-simple-setup-complete exists"
    estimated_time = "5-10 minutes for full setup"
  }
}