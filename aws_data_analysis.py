from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from typing import Dict, List, Optional
from datetime import datetime
import boto3
import json

class AWSLargeScaleAnalytics:
    """AWS-optimized analytics for large-scale data processing"""
    
    def __init__(self, spark: SparkSession = None, s3_bucket: str = None):
        """Initialize with SparkSession and S3 configuration"""
        if spark is None:
            self.spark = self._create_aws_optimized_spark_session()
        else:
            self.spark = spark
            
        self.s3_bucket = s3_bucket
        self._setup_logging()
        self._setup_aws_clients()
        
    def _create_aws_optimized_spark_session(self) -> SparkSession:
        """Create Spark session optimized for AWS"""
        return SparkSession.builder \
            .appName("AWS Large-Scale Data Analytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .config("spark.sql.parquet.columnarWriterBatchSize", "4096") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()
            
    def _setup_logging(self):
        """Configure logging for the analytics pipeline"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def _setup_aws_clients(self):
        """Initialize AWS service clients"""
        try:
            self.s3_client = boto3.client('s3')
            self.cloudwatch = boto3.client('cloudwatch')
        except Exception as e:
            self.logger.warning(f"Could not initialize AWS clients: {e}")
            self.s3_client = None
            self.cloudwatch = None
            
    def load_transactions_from_s3(self, s3_path: str) -> int:
        """Load transaction data from S3 Parquet files"""
        try:
            self.logger.info(f"Loading transaction data from S3: {s3_path}")
            
            # Load data with optimized settings for S3
            self.transactions_df = (
                self.spark.read
                .option("mergeSchema", "false")  # Faster loading
                .option("pathGlobFilter", "transactions_chunk_*.parquet")
                .parquet(s3_path)
                .cache()  # Cache for multiple analyses
            )
            
            # Get count and log
            count = self.transactions_df.count()
            self.logger.info(f"Successfully loaded {count:,} transactions")
            
            # Send metric to CloudWatch if available
            self._send_cloudwatch_metric("TransactionsLoaded", count)
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error loading transactions from S3: {str(e)}")
            raise
            
    def load_master_data_from_s3(self, s3_path: str) -> Dict:
        """Load stores and products master data from S3"""
        try:
            self.logger.info(f"Loading master data from S3: {s3_path}")
            
            # Load stores data
            stores_path = f"{s3_path}/stores_master.parquet"
            self.stores_df = self.spark.read.parquet(stores_path).cache()
            stores_count = self.stores_df.count()
            
            # Load products data
            products_path = f"{s3_path}/products_master.parquet"
            self.products_df = self.spark.read.parquet(products_path).cache()
            products_count = self.products_df.count()
            
            self.logger.info(f"Loaded {stores_count:,} stores and {products_count:,} products")
            
            return {
                "stores_count": stores_count,
                "products_count": products_count
            }
            
        except Exception as e:
            self.logger.error(f"Error loading master data: {str(e)}")
            raise
            
    def basic_stats(self) -> Dict:
        """Calculate basic statistics about the dataset"""
        try:
            self.logger.info("Calculating basic statistics...")
            
            # Use Spark's built-in aggregations for better performance
            stats_df = self.transactions_df.agg(
                F.count("*").alias("total_transactions"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_transaction_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("store_id").alias("active_stores"),
                F.min("date").alias("date_from"),
                F.max("date").alias("date_to")
            )
            
            stats = stats_df.first().asDict()
            
            # Calculate additional metrics
            stats["revenue_per_customer"] = stats["total_revenue"] / stats["unique_customers"] if stats["unique_customers"] > 0 else 0
            stats["transactions_per_store"] = stats["total_transactions"] / stats["active_stores"] if stats["active_stores"] > 0 else 0
            
            self.logger.info("Basic statistics calculated successfully")
            
            # Send key metrics to CloudWatch
            self._send_cloudwatch_metric("TotalRevenue", stats["total_revenue"])
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
                    F.countDistinct("customer_id").alias("unique_customers"),
                    F.stddev("total_amount").alias("transaction_stddev")
                )
                .withColumn("revenue_share", 
                           F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()))
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
                    .groupBy("store_id")  # Assuming store_id contains region info
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
        """Analyze sales trends over time with AWS optimizations"""
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
                .withColumn("revenue_growth", 
                           F.col("total_revenue") - F.lag("total_revenue").over(
                               Window.orderBy("window")))
                .orderBy("window")
            )
            
        except Exception as e:
            self.logger.error(f"Error in time series analysis: {str(e)}")
            raise
            
    def save_results_to_s3(self, results_df, s3_path: str, 
                          analysis_name: str, partition_cols: List[str] = None) -> str:
        """Save analysis results to S3 in optimized format"""
        try:
            output_path = f"{s3_path}/{analysis_name}"
            self.logger.info(f"Saving {analysis_name} results to S3: {output_path}")
            
            # Configure write options for optimal S3 performance
            writer = (
                results_df
                .coalesce(1)  # Single file for small results
                .write
                .mode("overwrite")
                .option("compression", "snappy")
            )
            
            # Add partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            # Save as Parquet for efficient querying
            writer.parquet(output_path)
            
            # Also save as JSON for easy reading
            json_path = f"{output_path}_summary.json"
            if results_df.count() < 1000:  # Only for small results
                results_json = results_df.toPandas().to_json(orient='records', indent=2)
                if self.s3_client:
                    bucket, key = self._parse_s3_path(json_path)
                    self.s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=results_json,
                        ContentType='application/json'
                    )
            
            self.logger.info(f"Results saved successfully to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error saving results to S3: {str(e)}")
            raise
            
    def run_comprehensive_analysis(self, input_s3_path: str, output_s3_path: str) -> Dict:
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
            self.save_results_to_s3(timeseries_df, output_s3_path, "time_series", ["window"])
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
                    Namespace='LargeScaleAnalytics',
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
                self.logger.warning(f"Could not send CloudWatch metric: {e}")
                
    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and key"""
        if s3_path.startswith('s3://'):
            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            return bucket, key
        else:
            raise ValueError(f"Invalid S3 path format: {s3_path}")
            
    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'transactions_df'):
                self.transactions_df.unpersist()
            if hasattr(self, 'stores_df'):
                self.stores_df.unpersist()
            if hasattr(self, 'products_df'):
                self.products_df.unpersist()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")


def main():
    """Main execution function for AWS deployment"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='AWS Large Scale Analytics')
    parser.add_argument('--input-path', required=True, help='S3 input path')
    parser.add_argument('--output-path', required=True, help='S3 output path')
    parser.add_argument('--s3-bucket', help='S3 bucket name')
    args = parser.parse_args()
    
    # Initialize analytics
    analytics = AWSLargeScaleAnalytics(s3_bucket=args.s3_bucket)
    
    try:
        # Run comprehensive analysis
        results = analytics.run_comprehensive_analysis(
            input_s3_path=args.input_path,
            output_s3_path=args.output_path
        )
        
        print("Analysis completed successfully!")
        print(f"Results saved to: {args.output_path}")
        print(f"Summary: {json.dumps(results['basic_statistics'], indent=2)}")
        
    except Exception as e:
        print(f"Error running analysis: {str(e)}")
        raise
    finally:
        analytics.cleanup()
        analytics.spark.stop()


if __name__ == "__main__":
    main() 