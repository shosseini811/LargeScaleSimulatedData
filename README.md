# Large-Scale Data Analysis with PySpark

This repository contains a data generation script and PySpark analytics for large-scale business data analysis, optimized for both AWS and Databricks platforms.

## Project Structure

- `simulated.py`: Generates multi-GB simulated business data in Parquet format
- `data_analysis.py`: Production-ready PySpark analytics for Databricks
- `aws_data_analysis.py`: AWS-optimized PySpark analytics for EMR/EC2
- `aws_glue_analysis.py`: AWS Glue-specific analytics script
- `deploy_to_aws.sh`: Automated deployment script for AWS
- `AWS_DEPLOYMENT_GUIDE.md`: Comprehensive AWS deployment guide

## Data Generation

1. Run the data generator:

```bash
python simulated.py
```

This will create Parquet files in `./simulated_data/` directory.

## Quick Start on AWS 🚀

### Option 1: Automated Deployment (Recommended)

Use the provided deployment script for quick setup:

```bash
# Make the script executable
chmod +x deploy_to_aws.sh

# Deploy with EMR (recommended for large-scale processing)
./deploy_to_aws.sh -b your-unique-bucket-name -s medium -t emr

# Deploy with AWS Glue (serverless option)
./deploy_to_aws.sh -b your-unique-bucket-name -s small -t glue
```

### Option 2: Manual Setup

See the comprehensive [AWS Deployment Guide](AWS_DEPLOYMENT_GUIDE.md) for detailed instructions on:
- Amazon EMR setup
- AWS Glue configuration  
- SageMaker processing
- EC2 with Docker
- Cost optimization strategies

## Databricks Setup and Execution

### 1. Set Up Databricks Environment

1. Log in to your Databricks workspace
2. Create a new cluster with:
   - Runtime: Databricks Runtime 13.3 LTS or later
   - Node type: Memory-optimized instances recommended
   - Enable autoscaling for optimal performance

### 2. Upload Data to Databricks

1. In your Databricks workspace, go to Data > DBFS
2. Create a new directory: `/FileStore/simulated_data`
3. Upload the generated Parquet files:

```bash
databricks fs cp -r ./simulated_data/ dbfs:/FileStore/simulated_data/
```

### 3. Upload Analysis Script

1. In Databricks workspace, create a new directory for the project
2. Upload `data_analysis.py` to this directory
3. Create a new notebook and import the analytics class:

```python
# Import analytics class
from data_analysis import LargeScaleAnalytics

# Initialize analytics
analytics = LargeScaleAnalytics(spark)

# Load data
analytics.load_transactions("/dbfs/FileStore/simulated_data")

# Run analyses
basic_stats = analytics.basic_stats()
print("\nBasic Statistics:")
print(basic_stats)

# More analyses available:
analytics.sales_by_segment().show()           # Customer segment analysis
analytics.product_performance().show()         # Product category performance
analytics.regional_analysis().show()           # Regional sales analysis
analytics.time_series_analysis().show()        # Monthly sales trends
analytics.customer_cohort_analysis().show()    # Customer cohort analysis
```

## Available Analyses

1. **Basic Statistics**
   - Total transactions and revenue
   - Average transaction value
   - Unique customers and active stores

2. **Customer Segment Analysis**
   - Transaction counts and revenue by segment
   - Average transaction value per segment
   - Unique customers per segment

3. **Product Performance**
   - Units sold by category
   - Revenue by category
   - Average unit prices
   - Store coverage

4. **Regional Analysis**
   - Revenue by region and state
   - Store distribution
   - Customer distribution

5. **Time Series Analysis**
   - Monthly revenue trends
   - Transaction volume trends
   - Customer activity patterns

6. **Customer Cohort Analysis**
   - Customer retention patterns
   - Revenue by cohort
   - Monthly customer behavior

## Performance Considerations

- The analysis is optimized for Databricks execution
- Uses caching for frequently accessed data
- Implements Databricks-specific optimizations
- Handles large-scale data processing efficiently