#!/bin/bash

# AWS Large-Scale Analytics Deployment Script
# This script helps you deploy your large-scale analytics project to AWS

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BUCKET_NAME=""
REGION="us-east-1"
CLUSTER_NAME="large-scale-analytics-cluster"
INSTANCE_TYPE="m5.xlarge"
INSTANCE_COUNT=3
DATA_SIZE="small"  # small, medium, large

# Function to print colored output
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if AWS CLI is installed and configured
check_aws_setup() {
    print_message $BLUE "Checking AWS setup..."
    
    if ! command -v aws &> /dev/null; then
        print_message $RED "AWS CLI is not installed. Please install it first:"
        print_message $YELLOW "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        print_message $RED "AWS CLI is not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    print_message $GREEN "✓ AWS CLI is installed and configured"
}

# Function to create S3 bucket
create_s3_bucket() {
    local bucket_name=$1
    print_message $BLUE "Creating S3 bucket: $bucket_name"
    
    if aws s3 ls "s3://$bucket_name" 2>&1 | grep -q 'NoSuchBucket'; then
        if [ "$REGION" = "us-east-1" ]; then
            aws s3 mb "s3://$bucket_name"
        else
            aws s3 mb "s3://$bucket_name" --region "$REGION"
        fi
        print_message $GREEN "✓ Created S3 bucket: $bucket_name"
    else
        print_message $YELLOW "S3 bucket already exists: $bucket_name"
    fi
    
    # Create folder structure
    aws s3api put-object --bucket "$bucket_name" --key data/
    aws s3api put-object --bucket "$bucket_name" --key scripts/
    aws s3api put-object --bucket "$bucket_name" --key results/
    aws s3api put-object --bucket "$bucket_name" --key logs/
    
    print_message $GREEN "✓ Created folder structure in S3 bucket"
}

# Function to upload scripts to S3
upload_scripts() {
    local bucket_name=$1
    print_message $BLUE "Uploading analysis scripts to S3..."
    
    # Upload Python scripts
    aws s3 cp data_analysis.py "s3://$bucket_name/scripts/"
    aws s3 cp aws_data_analysis.py "s3://$bucket_name/scripts/"
    aws s3 cp aws_glue_analysis.py "s3://$bucket_name/scripts/"
    
    print_message $GREEN "✓ Uploaded analysis scripts to S3"
}

# Function to generate sample data
generate_data() {
    local size=$1
    print_message $BLUE "Generating sample data (size: $size)..."
    
    # Modify simulated.py based on size
    case $size in
        "small")
            TRANSACTIONS=1000000  # 1M transactions
            ;;
        "medium")
            TRANSACTIONS=10000000  # 10M transactions
            ;;
        "large")
            TRANSACTIONS=100000000  # 100M transactions
            ;;
        *)
            TRANSACTIONS=1000000
            ;;
    esac
    
    print_message $YELLOW "Generating $TRANSACTIONS transactions..."
    
    # Create a temporary modified version of simulated.py
    sed "s/total_transactions=100_000_000/total_transactions=$TRANSACTIONS/" simulated.py > temp_simulated.py
    
    python temp_simulated.py
    rm temp_simulated.py
    
    print_message $GREEN "✓ Generated sample data"
}

# Function to upload data to S3
upload_data() {
    local bucket_name=$1
    print_message $BLUE "Uploading generated data to S3..."
    
    if [ -d "./simulated_data" ]; then
        aws s3 sync ./simulated_data/ "s3://$bucket_name/data/" --storage-class STANDARD_IA
        print_message $GREEN "✓ Uploaded data to S3"
    else
        print_message $YELLOW "No data directory found. Skipping data upload."
    fi
}

# Function to create EMR cluster
create_emr_cluster() {
    local bucket_name=$1
    print_message $BLUE "Creating EMR cluster..."
    
    # Check if key pair exists
    if ! aws ec2 describe-key-pairs --key-names "emr-key-pair" &> /dev/null; then
        print_message $YELLOW "Creating EC2 key pair for EMR..."
        aws ec2 create-key-pair --key-name "emr-key-pair" --query 'KeyMaterial' --output text > emr-key-pair.pem
        chmod 400 emr-key-pair.pem
        print_message $GREEN "✓ Created key pair: emr-key-pair.pem"
    fi
    
    # Create EMR cluster
    CLUSTER_ID=$(aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label emr-6.15.0 \
        --instance-type "$INSTANCE_TYPE" \
        --instance-count "$INSTANCE_COUNT" \
        --applications Name=Spark Name=Hadoop \
        --ec2-attributes KeyName=emr-key-pair \
        --log-uri "s3://$bucket_name/logs/" \
        --service-role EMR_DefaultRole \
        --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
        --auto-terminate \
        --query 'ClusterId' --output text)
    
    print_message $GREEN "✓ Created EMR cluster: $CLUSTER_ID"
    print_message $YELLOW "Cluster is starting up. This may take 10-15 minutes..."
    
    # Wait for cluster to be ready
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"
    print_message $GREEN "✓ EMR cluster is ready!"
    
    echo "$CLUSTER_ID" > cluster_id.txt
}

# Function to submit Spark job
submit_spark_job() {
    local bucket_name=$1
    local cluster_id=$2
    print_message $BLUE "Submitting Spark job to EMR..."
    
    STEP_ID=$(aws emr add-steps \
        --cluster-id "$cluster_id" \
        --steps Type=Spark,Name="Large Scale Analytics",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,"s3://$bucket_name/scripts/aws_data_analysis.py",--input-path,"s3://$bucket_name/data/",--output-path,"s3://$bucket_name/results/"] \
        --query 'StepIds[0]' --output text)
    
    print_message $GREEN "✓ Submitted Spark job: $STEP_ID"
    print_message $YELLOW "Job is running. You can monitor progress in the AWS EMR console."
    
    echo "$STEP_ID" > step_id.txt
}

# Function to create Glue job
create_glue_job() {
    local bucket_name=$1
    print_message $BLUE "Creating AWS Glue job..."
    
    # Create Glue service role if it doesn't exist
    if ! aws iam get-role --role-name AWSGlueServiceRole &> /dev/null; then
        print_message $YELLOW "Creating Glue service role..."
        aws iam create-role --role-name AWSGlueServiceRole --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }'
        
        aws iam attach-role-policy --role-name AWSGlueServiceRole --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
        aws iam attach-role-policy --role-name AWSGlueServiceRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
    fi
    
    # Create Glue job
    aws glue create-job \
        --name "large-scale-analytics-job" \
        --role "AWSGlueServiceRole" \
        --command '{
            "Name": "glueetl",
            "ScriptLocation": "s3://'$bucket_name'/scripts/aws_glue_analysis.py"
        }' \
        --default-arguments '{
            "--INPUT_PATH": "s3://'$bucket_name'/data/",
            "--OUTPUT_PATH": "s3://'$bucket_name'/results/"
        }' \
        --max-capacity 10 \
        --timeout 2880
    
    print_message $GREEN "✓ Created Glue job: large-scale-analytics-job"
}

# Function to start Glue job
start_glue_job() {
    print_message $BLUE "Starting Glue job..."
    
    JOB_RUN_ID=$(aws glue start-job-run --job-name "large-scale-analytics-job" --query 'JobRunId' --output text)
    
    print_message $GREEN "✓ Started Glue job: $JOB_RUN_ID"
    print_message $YELLOW "Job is running. You can monitor progress in the AWS Glue console."
    
    echo "$JOB_RUN_ID" > glue_job_run_id.txt
}

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -b, --bucket BUCKET_NAME    S3 bucket name (required)"
    echo "  -r, --region REGION         AWS region (default: us-east-1)"
    echo "  -s, --size SIZE             Data size: small|medium|large (default: small)"
    echo "  -t, --type TYPE             Deployment type: emr|glue (default: emr)"
    echo "  -i, --instance-type TYPE    EC2 instance type (default: m5.xlarge)"
    echo "  -c, --instance-count COUNT  Number of instances (default: 3)"
    echo "  -h, --help                  Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -b my-analytics-bucket -s medium -t emr"
    echo "  $0 -b my-analytics-bucket -t glue"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -s|--size)
            DATA_SIZE="$2"
            shift 2
            ;;
        -t|--type)
            DEPLOYMENT_TYPE="$2"
            shift 2
            ;;
        -i|--instance-type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        -c|--instance-count)
            INSTANCE_COUNT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_message $RED "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$BUCKET_NAME" ]; then
    print_message $RED "Error: S3 bucket name is required"
    usage
    exit 1
fi

# Set default deployment type
if [ -z "$DEPLOYMENT_TYPE" ]; then
    DEPLOYMENT_TYPE="emr"
fi

# Main execution
print_message $GREEN "🚀 Starting AWS Large-Scale Analytics Deployment"
print_message $BLUE "Configuration:"
print_message $BLUE "  Bucket: $BUCKET_NAME"
print_message $BLUE "  Region: $REGION"
print_message $BLUE "  Data Size: $DATA_SIZE"
print_message $BLUE "  Deployment Type: $DEPLOYMENT_TYPE"

# Step 1: Check AWS setup
check_aws_setup

# Step 2: Create S3 bucket
create_s3_bucket "$BUCKET_NAME"

# Step 3: Upload scripts
upload_scripts "$BUCKET_NAME"

# Step 4: Generate and upload data
generate_data "$DATA_SIZE"
upload_data "$BUCKET_NAME"

# Step 5: Deploy based on type
case $DEPLOYMENT_TYPE in
    "emr")
        create_emr_cluster "$BUCKET_NAME"
        if [ -f "cluster_id.txt" ]; then
            CLUSTER_ID=$(cat cluster_id.txt)
            submit_spark_job "$BUCKET_NAME" "$CLUSTER_ID"
        fi
        ;;
    "glue")
        create_glue_job "$BUCKET_NAME"
        start_glue_job
        ;;
    *)
        print_message $RED "Unknown deployment type: $DEPLOYMENT_TYPE"
        exit 1
        ;;
esac

print_message $GREEN "🎉 Deployment completed successfully!"
print_message $BLUE "Next steps:"
print_message $BLUE "1. Monitor your job in the AWS console"
print_message $BLUE "2. Check results in s3://$BUCKET_NAME/results/"
print_message $BLUE "3. View logs in s3://$BUCKET_NAME/logs/"

if [ "$DEPLOYMENT_TYPE" = "emr" ]; then
    print_message $YELLOW "Note: EMR cluster will auto-terminate after job completion to save costs"
fi 