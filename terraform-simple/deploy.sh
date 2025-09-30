#!/bin/bash

# ============================================================================
# LHBench TPC-H Simple Deployment (High Performance)
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
AWS_REGION="us-east-1"
PROJECT_NAME="lhbench-simple"
INSTANCE_TYPE="t3.large"  # For testing
KEY_NAME=""
ACTION="plan"

# Function to print colored output
print_status() {
    echo -e "${CYAN}[INFO]${NC} $1"
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

print_header() {
    echo -e "${PURPLE}=================================================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}=================================================================${NC}"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

LHBench TPC-H Simple Deployment (High Performance, Using AWS Defaults)

OPTIONS:
    -a, --action          Action to perform (plan|apply|destroy|status) [default: plan]
    -r, --region         AWS region [default: us-east-1]
    -k, --key-name       EC2 Key Pair name (required for apply)
    -i, --instance-type  EC2 instance type [default: t3.large]
    -h, --help           Show this help message

EXAMPLES:
    # Plan high-performance deployment
    $0 --action plan --key-name my-key-pair

    # Deploy with full power
    $0 --action apply --key-name my-key-pair

    # Check status
    $0 --action status

    # Stop instance to save money
    aws ec2 stop-instances --instance-ids \$(terraform output -raw instance_id)

    # Destroy everything
    $0 --action destroy

WHAT THIS VERSION DOES:
    ✅ Uses AWS default VPC (no custom networking)
    ✅ Uses existing subnets and resources
    ✅ Uses t3.large for cost-effective testing
    ✅ Optimizes for speed, not cost
    ✅ Full TPC-H benchmark capability
    ✅ All lakehouse formats (Delta, Iceberg, Hudi)
    ✅ AWS SSM access (no SSH key needed)
    ✅ High-performance Spark configuration

PERFORMANCE EXPECTATIONS:
    • TPC-H Scale Factor: 1-50 (vs 1-2 in minimal)
    • Bronze Generation: 1-3 min (vs 5-10 min)
    • Silver Conversion: 2-5 min each (vs 10-15 min)
    • Benchmark Queries: 10-30 min (vs 30-60 min)
    • Total Runtime: 20-45 min (vs 2-3 hours)

COST INFORMATION:
    • Running 24/7: ~\$1,011/month
    • Running 8h/day: ~\$348/month  
    • Stopped: ~\$16/month (storage only)
    
    💡 Same performance as original, simpler setup!

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -a|--action)
            ACTION="$2"
            shift 2
            ;;
        -r|--region)
            AWS_REGION="$2"
            shift 2
            ;;
        -i|--instance-type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        -k|--key-name)
            KEY_NAME="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate action
if [[ ! "$ACTION" =~ ^(plan|apply|destroy|status|cost)$ ]]; then
    print_error "Invalid action: $ACTION"
    show_usage
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "main.tf" ]]; then
    print_error "main.tf not found. Please run this script from the terraform-simple directory."
    exit 1
fi

print_header "LHBench Simple High-Performance Deployment - $ACTION"

print_status "Configuration:"
echo -e "  ${BLUE}Project Name:${NC} $PROJECT_NAME"
echo -e "  ${BLUE}AWS Region:${NC} $AWS_REGION"
echo -e "  ${BLUE}Instance Type:${NC} $INSTANCE_TYPE (2 vCPUs, 8GB RAM)"
echo -e "  ${BLUE}Key Pair:${NC} ${KEY_NAME:-'Not specified'}"
echo -e "  ${BLUE}Action:${NC} $ACTION"
echo -e "  ${BLUE}Network:${NC} Using AWS default VPC and subnets"
echo ""

# Show performance info
print_success "⚡ High-Performance Configuration:"
echo -e "  ${GREEN}✅ t3.large for testing${NC}"
echo -e "  ${GREEN}✅ Optimized Spark settings${NC}"
echo -e "  ${GREEN}✅ NVMe SSD acceleration${NC}"
echo -e "  ${GREEN}✅ High-throughput storage${NC}"
echo -e "  ${GREEN}✅ AWS default networking (simple)${NC}"
echo ""

# Check prerequisites
print_status "Checking prerequisites..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check Terraform
if ! command -v terraform &> /dev/null; then
    print_error "Terraform is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

# Check default VPC exists
print_status "Checking AWS default VPC..."
DEFAULT_VPC=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --region "$AWS_REGION" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo "None")
if [[ "$DEFAULT_VPC" == "None" ]]; then
    print_error "No default VPC found in region $AWS_REGION."
    print_status "You can create one with: aws ec2 create-default-vpc --region $AWS_REGION"
    exit 1
fi

print_success "Prerequisites check passed"
print_success "Default VPC found: $DEFAULT_VPC"

# Validate key pair for apply action
if [[ "$ACTION" == "apply" && -z "$KEY_NAME" ]]; then
    print_error "Key pair name is required for apply action. Use --key-name option."
    exit 1
fi

# Check if key pair exists
if [[ -n "$KEY_NAME" ]]; then
    print_status "Checking if key pair '$KEY_NAME' exists in region '$AWS_REGION'..."
    if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$AWS_REGION" &> /dev/null; then
        print_error "Key pair '$KEY_NAME' not found in region '$AWS_REGION'."
        print_status "You can create one with: aws ec2 create-key-pair --key-name $KEY_NAME --region $AWS_REGION"
        exit 1
    fi
    print_success "Key pair '$KEY_NAME' found"
fi

# Initialize Terraform
print_status "Initializing Terraform..."
terraform init

# Set Terraform variables
export TF_VAR_aws_region="$AWS_REGION"
export TF_VAR_project_name="$PROJECT_NAME"
export TF_VAR_instance_type="$INSTANCE_TYPE"
if [[ -n "$KEY_NAME" ]]; then
    export TF_VAR_key_name="$KEY_NAME"
fi

# Execute action
case $ACTION in
    plan)
        print_status "Planning high-performance deployment..."
        terraform plan -detailed-exitcode -var="key_name=${KEY_NAME:-dummy}"
        exit_code=$?
        
        if [[ $exit_code -eq 0 ]]; then
            print_success "No changes needed"
        elif [[ $exit_code -eq 2 ]]; then
            print_success "Changes planned successfully"
            echo ""
            print_warning "💰 This will create resources costing approximately \$1,011/month"
            print_warning "💡 Stop instance when not in use for major savings!"
        else
            print_error "Terraform plan failed"
            exit 1
        fi
        ;;
        
    apply)
        print_status "Planning high-performance deployment..."
        terraform plan -out=tfplan
        
        echo ""
        print_warning "💰 This will create AWS resources costing ~\$1,011/month"
        print_warning "💰 Cost-effective t3.large instance"
        print_warning "💡 Stop when not in use to reduce costs significantly!"
        echo ""
        read -p "Do you want to proceed? (y/N): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Applying high-performance Terraform configuration..."
            terraform apply tfplan
            
            if [[ $? -eq 0 ]]; then
                print_success "High-performance infrastructure deployed successfully!"
                echo ""
                print_header "Deployment Information"
                terraform output
                
                echo ""
                INSTANCE_IP=$(terraform output -raw public_ip 2>/dev/null || echo "Not available yet")
                INSTANCE_ID=$(terraform output -raw instance_id 2>/dev/null || echo "Not available yet")
                
                if [[ "$INSTANCE_IP" != "Not available yet" ]]; then
                    echo ""
                    print_success "🌐 Access URLs:"
                    echo -e "  ${BLUE}Airflow UI:${NC} http://$INSTANCE_IP:8080"
                    echo -e "  ${BLUE}MinIO Console:${NC} http://$INSTANCE_IP:9001"
                    echo -e "  ${BLUE}Username/Password:${NC} admin / admin123"
                    echo ""
                    print_success "⚡ High-Performance Features:"
                    echo -e "  ${GREEN}✅ 2 vCPUs, 8GB RAM${NC}"
                    echo -e "  ${GREEN}✅ 1.9TB NVMe SSD + 200GB EBS${NC}"
                    echo -e "  ${GREEN}✅ Optimized Spark configuration${NC}"
                    echo -e "  ${GREEN}✅ AWS SSM access enabled${NC}"
                    echo ""
                    print_status "ℹ️  Setup takes 5-10 minutes. Monitor progress:"
                    echo -e "  ${CYAN}SSH:${NC} ssh -i ~/.ssh/$KEY_NAME.pem ubuntu@$INSTANCE_IP 'tail -f /var/log/lhbench-simple-setup.log'"
                    echo -e "  ${CYAN}SSM:${NC} aws ssm start-session --target $INSTANCE_ID"
                    echo ""
                    print_warning "💰 Cost Management Commands:"
                    echo -e "  ${YELLOW}Stop:${NC} aws ec2 stop-instances --instance-ids $INSTANCE_ID"
                    echo -e "  ${YELLOW}Start:${NC} aws ec2 start-instances --instance-ids $INSTANCE_ID"
                    echo -e "  ${YELLOW}Status:${NC} aws ec2 describe-instances --instance-ids $INSTANCE_ID --query 'Reservations[0].Instances[0].State.Name'"
                fi
            else
                print_error "Terraform apply failed"
                exit 1
            fi
        else
            print_status "Deployment cancelled"
        fi
        ;;
        
    destroy)
        print_warning "This will destroy the high-performance infrastructure."
        echo ""
        read -p "Are you sure you want to destroy the infrastructure? (y/N): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Destroying infrastructure..."
            terraform destroy -auto-approve
            
            if [[ $? -eq 0 ]]; then
                print_success "Infrastructure destroyed successfully"
                print_success "💰 No more charges will be incurred!"
            else
                print_error "Terraform destroy failed"
                exit 1
            fi
        else
            print_status "Destroy cancelled"
        fi
        ;;
        
    status)
        print_status "Checking high-performance infrastructure status..."
        
        # Check if Terraform state exists
        if terraform show &> /dev/null; then
            print_success "Infrastructure state found"
            echo ""
            
            # Show outputs
            print_status "Current infrastructure:"
            terraform output
            
            # Try to get instance status
            INSTANCE_ID=$(terraform output -raw instance_id 2>/dev/null || echo "")
            if [[ -n "$INSTANCE_ID" ]]; then
                print_status "Checking EC2 instance status..."
                INSTANCE_INFO=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" \
                    --query 'Reservations[0].Instances[0].{State:State.Name,Type:InstanceType,PublicIP:PublicIpAddress,PrivateIP:PrivateIpAddress}' \
                    --output table 2>/dev/null || echo "Instance info not available")
                
                if [[ "$INSTANCE_INFO" != "Instance info not available" ]]; then
                    echo "$INSTANCE_INFO"
                    
                    INSTANCE_STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" \
                        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "unknown")
                    
                    echo ""
                    if [[ "$INSTANCE_STATE" == "running" ]]; then
                        print_success "✅ Instance is RUNNING (~\$1.38/hour)"
                        print_status "💡 Access via AWS SSM: aws ssm start-session --target $INSTANCE_ID"
                    elif [[ "$INSTANCE_STATE" == "stopped" ]]; then
                        print_warning "⏸️  Instance is STOPPED (~\$0.02/hour for storage)"
                    else
                        print_status "Instance state: $INSTANCE_STATE"
                    fi
                fi
                
                echo ""
                print_status "💰 Cost management commands:"
                echo -e "  ${YELLOW}Stop:${NC} aws ec2 stop-instances --instance-ids $INSTANCE_ID"
                echo -e "  ${YELLOW}Start:${NC} aws ec2 start-instances --instance-ids $INSTANCE_ID"
                echo -e "  ${YELLOW}Monitor:${NC} aws ssm start-session --target $INSTANCE_ID"
            fi
        else
            print_warning "No infrastructure state found. Run with '--action apply' to deploy."
        fi
        ;;
        
    cost)
        print_status "Cost information for high-performance setup..."
        
        cat << EOF

💰 High-Performance Setup Costs (USD):

EC2 Instance (m6gd.8xlarge):
  • Running 24/7:     ~\$995/month
  • Running 8h/day:   ~\$332/month  
  • Stopped:          \$0/hour (only storage costs)

Storage (200GB EBS gp3):
  • High-performance: ~\$16/month
  • 4,000 IOPS included
  • 250 MB/s throughput

NVMe SSD (1.9TB):
  • Included with instance
  • Ultra-high performance
  • Perfect for Spark workloads

Total Monthly Costs:
  • Always running:   ~\$1,011/month
  • 8 hours/day:      ~\$348/month
  • Only storage:     ~\$16/month

🚀 Performance Benefits:
  • 32 vCPUs vs 2 vCPUs (16x more)
  • 128GB RAM vs 8GB RAM (16x more)
  • 1.9TB NVMe vs 75GB NVMe (25x more)
  • TPC-H Scale Factor: 50+ vs 1-2
  • Benchmark runtime: 20-45 min vs 2-3 hours

💡 Cost Optimization:
  • Stop when not in use: 85% savings
  • Use for serious benchmarks only
  • Run multiple scale factors efficiently
  • Perfect for performance comparisons

⚠️  Network & Simplicity Benefits:
  • Uses default VPC (no networking costs)
  • No Load Balancer needed
  • No Lambda functions
  • Simpler setup, same performance

EOF
        ;;
esac

print_success "Operation completed successfully!"

# Show helpful commands for apply
if [[ "$ACTION" == "apply" ]]; then
    echo ""
    print_header "🚀 Next Steps & Performance Tips"
    echo -e "${BLUE}Check status:${NC}"
    echo -e "  $0 --action status"
    echo ""
    echo -e "${BLUE}SSH access:${NC}"
    echo -e "  ssh -i ~/.ssh/$KEY_NAME.pem ubuntu@\$(terraform output -raw public_ip)"
    echo ""
    echo -e "${BLUE}AWS SSM access (no SSH key needed):${NC}"
    echo -e "  aws ssm start-session --target \$(terraform output -raw instance_id)"
    echo ""
    echo -e "${BLUE}Monitor setup progress:${NC}"
    echo -e "  ssh -i ~/.ssh/$KEY_NAME.pem ubuntu@\$(terraform output -raw public_ip) 'tail -f /var/log/lhbench-simple-setup.log'"
    echo ""
    echo -e "${BLUE}Check system performance:${NC}"
    echo -e "  ssh -i ~/.ssh/$KEY_NAME.pem ubuntu@\$(terraform output -raw public_ip) '/opt/lhbench/check-status.sh'"
    echo ""
    echo -e "${YELLOW}💰 Save money when not in use:${NC}"
    echo -e "  aws ec2 stop-instances --instance-ids \$(terraform output -raw instance_id)"
    echo ""
    echo -e "${GREEN}⚡ For maximum performance, try TPC-H Scale Factor 10-50!${NC}"
fi