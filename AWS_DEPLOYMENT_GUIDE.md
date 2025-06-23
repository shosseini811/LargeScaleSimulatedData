# Running Large-Scale Data Analysis on AWS

This guide explains how to run your large-scale data analysis project on AWS using various services. The project generates multi-GB simulated business data and performs PySpark analytics.

## Overview of AWS Options

### 1. **Amazon EMR (Recommended for PySpark)**
- **Best for**: Large-scale Spark processing, cost-effective for big data
- **Use case**: When you need full control over Spark clusters
- **Cost**: Pay per hour for cluster resources

### 2. **AWS Glue**
- **Best for**: Serverless ETL jobs, managed Spark environment
- **Use case**: When you want serverless data processing
- **Cost**: Pay per DPU (Data Processing Unit) hour

### 3. **Amazon SageMaker**
- **Best for**: ML workloads with data analysis
- **Use case**: When combining analytics with machine learning
- **Cost**: Pay for notebook instances and processing jobs

### 4. **Amazon EC2 with Docker**
- **Best for**: Custom environments, full control
- **Use case**: When you need specific configurations
- **Cost**: Pay for EC2 instances

## Option 1: Amazon EMR (Recommended)

### Prerequisites
- AWS CLI installed and configured
- AWS account with appropriate permissions
- S3 bucket for data storage

### Step 1: Set Up S3 Storage

```bash
# Create S3 bucket for your project
aws s3 mb s3://your-large-scale-analytics-bucket

# Create directories
aws s3api put-object --bucket your-large-scale-analytics-bucket --key data/
aws s3api put-object --bucket your-large-scale-analytics-bucket --key scripts/
aws s3api put-object --bucket your-large-scale-analytics-bucket --key logs/
```

### Step 2: Generate Data Locally (Optional)

```bash
# Install dependencies
pip install -r requirements.txt

# Generate smaller dataset for testing (adjust size as needed)
python simulated.py
```

### Step 3: Upload Data and Scripts to S3

```bash
# Upload generated data
aws s3 sync ./simulated_data/ s3://your-large-scale-analytics-bucket/data/

# Upload analysis script
aws s3 cp data_analysis.py s3://your-large-scale-analytics-bucket/scripts/
```

### Step 4: Create EMR Cluster

```bash
# Create EMR cluster with Spark
aws emr create-cluster \
    --name "Large-Scale-Analytics-Cluster" \
    --release-label emr-6.15.0 \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --applications Name=Spark Name=Hadoop \
    --ec2-attributes KeyName=your-key-pair \
    --log-uri s3://your-large-scale-analytics-bucket/logs/ \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole
```

### Step 5: Submit Spark Job

```bash
# Submit the analysis job
aws emr add-steps \
    --cluster-id j-XXXXXXXXX \
    --steps Type=Spark,Name="Large Scale Analytics",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://your-large-scale-analytics-bucket/scripts/data_analysis.py]
```

## Option 2: AWS Glue (Serverless)

### Step 1: Create Glue Job Script

Create a modified version of your analysis script for Glue:

```python
# glue_analysis.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Your analytics class (adapted for Glue)
class GlueLargeScaleAnalytics:
    def __init__(self, spark):
        self.spark = spark
        
    def load_transactions(self, data_path):
        self.transactions_df = (
            self.spark.read.parquet(f"{data_path}/transactions_chunk_*.parquet")
            .cache()
        )
        return self.transactions_df.count()
    
    # Add your other analysis methods here...
    def basic_stats(self):
        return self.transactions_df.agg(
            F.count("*").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_value")
        ).collect()[0].asDict()

# Run analysis
analytics = GlueLargeScaleAnalytics(spark)
count = analytics.load_transactions(args['INPUT_PATH'])
print(f"Loaded {count:,} transactions")

# Perform analyses and save results
basic_stats = analytics.basic_stats()
print("Basic Statistics:", basic_stats)

# Save results to S3
results_df = spark.createDataFrame([basic_stats])
results_df.write.mode("overwrite").parquet(f"{args['OUTPUT_PATH']}/basic_stats/")

job.commit()
```

### Step 2: Create and Run Glue Job

```bash
# Create Glue job
aws glue create-job \
    --name "large-scale-analytics-job" \
    --role "AWSGlueServiceRole" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://your-large-scale-analytics-bucket/scripts/glue_analysis.py"
    }' \
    --default-arguments '{
        "--INPUT_PATH": "s3://your-large-scale-analytics-bucket/data/",
        "--OUTPUT_PATH": "s3://your-large-scale-analytics-bucket/results/"
    }' \
    --max-capacity 10

# Start job run
aws glue start-job-run --job-name "large-scale-analytics-job"
```

## Option 3: Amazon SageMaker

### Step 1: Create SageMaker Notebook Instance

```bash
# Create notebook instance
aws sagemaker create-notebook-instance \
    --notebook-instance-name "large-scale-analytics" \
    --instance-type "ml.t3.xlarge" \
    --role-arn "arn:aws:iam::YOUR-ACCOUNT:role/SageMakerExecutionRole"
```

### Step 2: Use SageMaker Processing Job

Create a SageMaker processing script:

```python
# sagemaker_processing.py
import pandas as pd
import numpy as np
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput

# Initialize Spark processor
spark_processor = PySparkProcessor(
    base_job_name="large-scale-analytics",
    framework_version="3.1",
    role="arn:aws:iam::YOUR-ACCOUNT:role/SageMakerExecutionRole",
    instance_count=2,
    instance_type="ml.m5.xlarge",
    max_runtime_in_seconds=3600
)

# Run processing job
spark_processor.run(
    submit_app="s3://your-large-scale-analytics-bucket/scripts/data_analysis.py",
    inputs=[
        ProcessingInput(
            source="s3://your-large-scale-analytics-bucket/data/",
            destination="/opt/ml/processing/input"
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/output",
            destination="s3://your-large-scale-analytics-bucket/results/"
        )
    ]
)
```

## Option 4: EC2 with Docker

### Step 1: Create Dockerfile

```dockerfile
FROM apache/spark-py:v3.4.0

# Install additional dependencies
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy application files
COPY data_analysis.py /app/
COPY simulated.py /app/

WORKDIR /app

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["python", "data_analysis.py"]
```

### Step 2: Launch EC2 Instance and Run

```bash
# Launch EC2 instance (use AWS CLI or Console)
# SSH into instance and run:

# Install Docker
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user

# Build and run container
docker build -t large-scale-analytics .
docker run -v /path/to/data:/app/data large-scale-analytics
```

## Data Generation on AWS

### Option A: Generate Data on EC2

```bash
# Launch large EC2 instance (e.g., m5.4xlarge)
# Install Python and dependencies
pip install -r requirements.txt

# Modify simulated.py to save directly to S3
python simulated.py
```

### Option B: Generate Data with EMR

Create an EMR step to generate data:

```python
# emr_data_generation.py
from pyspark.sql import SparkSession
# Import your LargeScaleDataGenerator class
# Generate data and save to S3 directly
```

## Cost Optimization Tips

### 1. **Use Spot Instances**
```bash
# EMR with Spot instances
aws emr create-cluster \
    --name "Analytics-Spot-Cluster" \
    --release-label emr-6.15.0 \
    --instance-groups '[
        {
            "Name": "Master",
            "InstanceRole": "MASTER",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 1
        },
        {
            "Name": "Workers",
            "InstanceRole": "CORE",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 2,
            "Market": "SPOT",
            "BidPrice": "0.10"
        }
    ]'
```

### 2. **Use S3 Intelligent Tiering**
```bash
# Set up intelligent tiering for cost savings
aws s3api put-bucket-intelligent-tiering-configuration \
    --bucket your-large-scale-analytics-bucket \
    --id EntireBucket \
    --intelligent-tiering-configuration Id=EntireBucket,Status=Enabled,Filter={}
```

### 3. **Auto-terminate EMR Clusters**
```bash
# Add auto-termination to EMR cluster
--auto-terminate
```

## Monitoring and Logging

### CloudWatch Integration
```bash
# Enable detailed monitoring for EMR
--enable-debugging
--log-uri s3://your-large-scale-analytics-bucket/logs/
```

### Cost Monitoring
```python
# Add cost tracking tags
--tags 'Project=LargeScaleAnalytics' 'Environment=Production'
```

## Security Best Practices

### 1. **IAM Roles and Policies**
Create specific IAM roles with minimal permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-large-scale-analytics-bucket/*",
                "arn:aws:s3:::your-large-scale-analytics-bucket"
            ]
        }
    ]
}
```

### 2. **VPC and Security Groups**
- Run EMR clusters in private subnets
- Use security groups to restrict access
- Enable encryption at rest and in transit

## Performance Optimization

### 1. **Spark Configuration for AWS**
```python
spark_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.columnarReaderBatchSize": "4096",
    "spark.sql.parquet.columnarWriterBatchSize": "4096"
}
```

### 2. **Data Partitioning**
```python
# Partition data by date for better performance
df.write.partitionBy("year", "month").parquet("s3://bucket/partitioned-data/")
```

## Troubleshooting

### Common Issues:
1. **Out of Memory**: Increase instance sizes or add more nodes
2. **S3 Access Issues**: Check IAM permissions and bucket policies
3. **Slow Performance**: Optimize Spark configurations and data partitioning
4. **Cost Overruns**: Use Spot instances and auto-termination

### Monitoring Commands:
```bash
# Check EMR cluster status
aws emr describe-cluster --cluster-id j-XXXXXXXXX

# Monitor Glue job
aws glue get-job-run --job-name "large-scale-analytics-job" --run-id jr_xxxxx

# Check S3 usage
aws s3api list-objects-v2 --bucket your-large-scale-analytics-bucket --query 'sum(Contents[].Size)'
```

## Next Steps

1. Choose the AWS service that best fits your needs
2. Set up your AWS environment and permissions
3. Upload your data and scripts
4. Run a small test job first
5. Scale up to your full dataset
6. Monitor costs and performance
7. Set up automated scheduling if needed

This guide provides multiple pathways to run your large-scale data analysis on AWS. Start with the EMR option for the most straightforward PySpark deployment, or choose Glue for a serverless approach. 