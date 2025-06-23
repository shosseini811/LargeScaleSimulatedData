import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import boto3
import json

class GlueLargeScaleAnalytics:
    """AWS Glue optimized analytics for large-scale data processing"""
    
    def __init__(self, glue_context: GlueContext, spark_context: SparkContext):
        """Initialize with Glue and Spark contexts"""
        self.glue_context = glue_context
        self.spark_context = spark_context
        self.spark = glue_context.spark_session
        self.logger = glue_context.get_logger()
        
        # Initialize AWS clients
        try:
            self.s3_client = boto3.client('s3')
            self.cloudwatch = boto3.client('cloudwatch')
        except Exception as e:
            self.logger.error(f"Could not initialize AWS clients: {e}")
            self.s3_client = None
            self.cloudwatch = None
        
    def load_transactions_from_s3(self, s3_path: str) -> int:
        """Load transaction data from S3 using Glue DynamicFrame"""
        try:
            self.logger.info(f"Loading transaction data from S3: {s3_path}")
            
            # Use Glue DynamicFrame for better schema handling
            transactions_dyf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [s3_path],
                    "recurse": True,
                    "groupFiles": "inPartition"
                },
                format="parquet",
                transformation_ctx="transactions_dyf"
            )
            
            # Convert to DataFrame and cache
            self.transactions_df = transactions_dyf.toDF().cache()
            
            # Get count and log
            count = self.transactions_df.count()
            self.logger.info(f"Successfully loaded {count:,} transactions")
            
            # Send metric to CloudWatch
            self._send_cloudwatch_metric("TransactionsLoaded", count)
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error loading transactions from S3: {str(e)}")
            raise
            
    def load_master_data_from_s3(self, s3_path: str) -> dict:
        """Load stores and products master data from S3"""
        try:
            self.logger.info(f"Loading master data from S3: {s3_path}")
            
            # Load stores data
            stores_dyf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [f"{s3_path}/stores_master.parquet"]},
                format="parquet",
                transformation_ctx="stores_dyf"
            )
            self.stores_df = stores_dyf.toDF().cache()
            stores_count = self.stores_df.count()
            
            # Load products data
            products_dyf = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [f"{s3_path}/products_master.parquet"]},
                format="parquet",
                transformation_ctx="products_dyf"
            )
            self.products_df = products_dyf.toDF().cache()
            products_count = self.products_df.count()
            
            self.logger.info(f"Loaded {stores_count:,} stores and {products_count:,} products")
            
            return {
                "stores_count": stores_count,
                "products_count": products_count
            }
            
        except Exception as e:
            self.logger.error(f"Error loading master data: {str(e)}")
            raise
            
    def basic_stats(self) -> dict:
        """Calculate basic statistics about the dataset"""
        try:
            self.logger.info("Calculating basic statistics...")
            
            # Use Spark's built-in aggregations for better performance
            stats_df = self.transactions_df.agg(
                F.count("*").alias("total_transactions"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_transaction_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("store_id").alias("active_stores")
            )
            
            stats = stats_df.first().asDict()
            
            # Calculate additional metrics safely
            if stats.get("unique_customers") and stats["unique_customers"] > 0:
                stats["revenue_per_customer"] = stats["total_revenue"] / stats["unique_customers"]
            else:
                stats["revenue_per_customer"] = 0
                
            if stats.get("active_stores") and stats["active_stores"] > 0:
                stats["transactions_per_store"] = stats["total_transactions"] / stats["active_stores"]
            else:
                stats["transactions_per_store"] = 0
            
            self.logger.info("Basic statistics calculated successfully")
            
            # Send key metrics to CloudWatch
            if stats.get("total_revenue"):
                self._send_cloudwatch_metric("TotalRevenue", float(stats["total_revenue"]))
            if stats.get("unique_customers"):
                self._send_cloudwatch_metric("UniqueCustomers", stats["unique_customers"])
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error calculating basic stats: {str(e)}")
            raise
            
    def sales_by_segment(self):
        """Analyze sales performance by customer segment"""
        try:
            self.logger.info("Analyzing sales by customer segment...")
            
            return (
                self.transactions_df
                .groupBy("customer_segment")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("total_amount").alias("total_revenue"),
                    F.avg("total_amount").alias("avg_transaction_value"),
                    F.countDistinct("customer_id").alias("unique_customers")
                )
                .orderBy(F.col("total_revenue").desc())
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing customer segments: {str(e)}")
            raise
            
    def regional_analysis(self):
        """Analyze sales performance by region with store data"""
        try:
            self.logger.info("Performing regional analysis...")
            
            # Join with stores data if available
            if hasattr(self, 'stores_df'):
                regional_df = (
                    self.transactions_df
                    .join(self.stores_df, "store_id", "left")
                    .groupBy("region", "state")
                    .agg(
                        F.sum("total_amount").alias("total_revenue"),
                        F.avg("total_amount").alias("avg_transaction_value"),
                        F.count("*").alias("transaction_count"),
                        F.countDistinct("store_id").alias("store_count"),
                        F.countDistinct("customer_id").alias("customer_count")
                    )
                )
            else:
                # Fallback if stores data not loaded
                regional_df = (
                    self.transactions_df
                    .groupBy("store_id")
                    .agg(
                        F.sum("total_amount").alias("total_revenue"),
                        F.avg("total_amount").alias("avg_transaction_value"),
                        F.count("*").alias("transaction_count")
                    )
                )
                
            return regional_df.orderBy(F.col("total_revenue").desc())
            
        except Exception as e:
            self.logger.error(f"Error in regional analysis: {str(e)}")
            raise
            
    def time_series_analysis(self, window_size: str = "1 month"):
        """Analyze sales trends over time"""
        try:
            self.logger.info(f"Performing time series analysis with {window_size} windows...")
            
            return (
                self.transactions_df
                .withColumn("timestamp", F.to_timestamp("timestamp"))
                .groupBy(F.window("timestamp", window_size))
                .agg(
                    F.sum("total_amount").alias("total_revenue"),
                    F.count("*").alias("transaction_count"),
                    F.countDistinct("customer_id").alias("unique_customers"),
                    F.avg("total_amount").alias("avg_transaction_value")
                )
                .orderBy("window")
            )
            
        except Exception as e:
            self.logger.error(f"Error in time series analysis: {str(e)}")
            raise
            
    def save_results_to_s3(self, results_df, s3_path: str, analysis_name: str) -> str:
        """Save analysis results to S3 using Glue DynamicFrame"""
        try:
            output_path = f"{s3_path}/{analysis_name}"
            self.logger.info(f"Saving {analysis_name} results to S3: {output_path}")
            
            # Convert DataFrame to DynamicFrame for Glue optimizations
            results_dyf = DynamicFrame.fromDF(results_df, self.glue_context, analysis_name)
            
            # Write to S3 using Glue
            self.glue_context.write_dynamic_frame.from_options(
                frame=results_dyf,
                connection_type="s3",
                connection_options={
                    "path": output_path,
                    "compression": "snappy"
                },
                format="parquet",
                transformation_ctx=f"write_{analysis_name}"
            )
            
            self.logger.info(f"Results saved successfully to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error saving results to S3: {str(e)}")
            raise
            
    def run_comprehensive_analysis(self, input_s3_path: str, output_s3_path: str) -> dict:
        """Run all analyses and save results to S3"""
        try:
            self.logger.info("Starting comprehensive analysis pipeline...")
            
            # Load data
            transaction_count = self.load_transactions_from_s3(input_s3_path)
            master_data = self.load_master_data_from_s3(input_s3_path)
            
            # Run analyses
            analyses_results = {}
            
            # Basic statistics
            basic_stats = self.basic_stats()
            analyses_results['basic_stats'] = basic_stats
            
            # Customer segment analysis
            segment_df = self.sales_by_segment()
            self.save_results_to_s3(segment_df, output_s3_path, "customer_segments")
            analyses_results['customer_segments'] = f"{output_s3_path}/customer_segments"
            
            # Regional analysis
            regional_df = self.regional_analysis()
            self.save_results_to_s3(regional_df, output_s3_path, "regional_analysis")
            analyses_results['regional_analysis'] = f"{output_s3_path}/regional_analysis"
            
            # Time series analysis
            timeseries_df = self.time_series_analysis()
            self.save_results_to_s3(timeseries_df, output_s3_path, "time_series")
            analyses_results['time_series'] = f"{output_s3_path}/time_series"
            
            # Save comprehensive summary
            summary = {
                "analysis_timestamp": datetime.now().isoformat(),
                "input_path": input_s3_path,
                "output_path": output_s3_path,
                "transaction_count": transaction_count,
                "master_data": master_data,
                "basic_statistics": basic_stats,
                "analysis_results": analyses_results
            }
            
            # Save summary to S3
            if self.s3_client:
                summary_path = f"{output_s3_path}/analysis_summary.json"
                bucket, key = self._parse_s3_path(summary_path)
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=json.dumps(summary, indent=2, default=str),
                    ContentType='application/json'
                )
            
            self.logger.info("Comprehensive analysis completed successfully!")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive analysis: {str(e)}")
            raise
            
    def _send_cloudwatch_metric(self, metric_name: str, value: float, unit: str = 'Count'):
        """Send custom metric to CloudWatch"""
        if self.cloudwatch:
            try:
                self.cloudwatch.put_metric_data(
                    Namespace='LargeScaleAnalytics/Glue',
                    MetricData=[
                        {
                            'MetricName': metric_name,
                            'Value': value,
                            'Unit': unit,
                            'Timestamp': datetime.now()
                        }
                    ]
                )
            except Exception as e:
                self.logger.error(f"Could not send CloudWatch metric: {e}")
                
    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and key"""
        if s3_path.startswith('s3://'):
            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            return bucket, key
        else:
            raise ValueError(f"Invalid S3 path format: {s3_path}")


def main():
    """Main execution function for AWS Glue"""
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
    
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    # Initialize analytics
    analytics = GlueLargeScaleAnalytics(glue_context, sc)
    
    try:
        # Run comprehensive analysis
        results = analytics.run_comprehensive_analysis(
            input_s3_path=args['INPUT_PATH'],
            output_s3_path=args['OUTPUT_PATH']
        )
        
        print("Analysis completed successfully!")
        print(f"Results saved to: {args['OUTPUT_PATH']}")
        print(f"Summary: {json.dumps(results['basic_statistics'], indent=2)}")
        
    except Exception as e:
        print(f"Error running analysis: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main() 