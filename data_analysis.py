from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from typing import Dict, List, Optional
from datetime import datetime

class LargeScaleAnalytics:
    """Production-ready analytics for large-scale data on Databricks"""
    
    def __init__(self, spark: SparkSession):
        """Initialize with SparkSession"""
        self.spark = spark
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the analytics pipeline"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def load_transactions(self, data_path: str) -> None:
        """Load transaction data from Parquet files"""
        try:
            self.logger.info(f"Loading transaction data from: {data_path}")
            self.transactions_df = (
                self.spark.read.parquet(f"{data_path}/transactions_chunk_*.parquet")
                .cache()  # Cache for multiple analyses
            )
            self.logger.info(f"Loaded {self.transactions_df.count():,} transactions")
        except Exception as e:
            self.logger.error(f"Error loading transactions: {str(e)}")
            raise
            
    def basic_stats(self) -> Dict:
        """Calculate basic statistics about the dataset"""
        try:
            stats = {}
            
            # Transaction counts and totals
            agg_df = self.transactions_df.agg(
                F.count("*").alias("total_transactions"),
                F.sum("transaction_total").alias("total_revenue"),
                F.avg("transaction_total").alias("avg_transaction_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("store_id").alias("active_stores")
            )
            stats.update(agg_df.first().asDict())
            
            self.logger.info("Basic statistics calculated successfully")
            return stats
        except Exception as e:
            self.logger.error(f"Error calculating basic stats: {str(e)}")
            raise
            
    def sales_by_segment(self) -> None:
        """Analyze sales performance by customer segment"""
        try:
            return (
                self.transactions_df
                .groupBy("customer_segment")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.sum("transaction_total").alias("total_revenue"),
                    F.avg("transaction_total").alias("avg_transaction_value"),
                    F.countDistinct("customer_id").alias("unique_customers")
                )
                .orderBy(F.col("total_revenue").desc())
            )
        except Exception as e:
            self.logger.error(f"Error analyzing segments: {str(e)}")
            raise
            
    def product_performance(self) -> None:
        """Analyze product category performance"""
        try:
            return (
                self.transactions_df
                .groupBy("product_category")
                .agg(
                    F.sum("quantity").alias("total_units_sold"),
                    F.sum("transaction_total").alias("total_revenue"),
                    F.avg("unit_price").alias("avg_unit_price"),
                    F.countDistinct("store_id").alias("stores_selling")
                )
                .orderBy(F.col("total_revenue").desc())
            )
        except Exception as e:
            self.logger.error(f"Error analyzing products: {str(e)}")
            raise
            
    def regional_analysis(self) -> None:
        """Analyze sales performance by region"""
        try:
            return (
                self.transactions_df
                .groupBy("region", "state")
                .agg(
                    F.sum("transaction_total").alias("total_revenue"),
                    F.avg("transaction_total").alias("avg_transaction_value"),
                    F.countDistinct("store_id").alias("store_count"),
                    F.countDistinct("customer_id").alias("customer_count")
                )
                .orderBy("region", F.col("total_revenue").desc())
            )
        except Exception as e:
            self.logger.error(f"Error analyzing regions: {str(e)}")
            raise
            
    def time_series_analysis(self, window_size: str = "1 month") -> None:
        """Analyze sales trends over time"""
        try:
            return (
                self.transactions_df
                .withColumn("date", F.to_date("transaction_date"))
                .groupBy(F.window("date", window_size))
                .agg(
                    F.sum("transaction_total").alias("total_revenue"),
                    F.count("*").alias("transaction_count"),
                    F.countDistinct("customer_id").alias("unique_customers")
                )
                .orderBy("window")
            )
        except Exception as e:
            self.logger.error(f"Error in time series analysis: {str(e)}")
            raise
            
    def customer_cohort_analysis(self, time_window: str = "1 month") -> None:
        """Perform customer cohort analysis"""
        try:
            # Get first purchase date for each customer
            first_purchase = (
                self.transactions_df
                .groupBy("customer_id")
                .agg(F.min("transaction_date").alias("first_purchase_date"))
            )
            
            # Join with transactions and calculate cohort metrics
            cohort_analysis = (
                self.transactions_df
                .join(first_purchase, "customer_id")
                .withColumn("months_since_first_purchase", 
                    F.months_between("transaction_date", "first_purchase_date"))
                .groupBy(
                    F.date_trunc("month", "first_purchase_date").alias("cohort_month"),
                    F.floor("months_since_first_purchase").alias("month_number")
                )
                .agg(
                    F.countDistinct("customer_id").alias("active_customers"),
                    F.sum("transaction_total").alias("total_revenue")
                )
                .orderBy("cohort_month", "month_number")
            )
            
            return cohort_analysis
        except Exception as e:
            self.logger.error(f"Error in cohort analysis: {str(e)}")
            raise

def main():
    """Main execution function"""
    # Initialize Spark Session with Databricks optimizations
    spark = SparkSession.builder \
        .appName("Large-Scale Data Analytics") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
        
    try:
        # Initialize analytics
        analytics = LargeScaleAnalytics(spark)
        
        # Load data (update path as needed)
        analytics.load_transactions("/dbfs/FileStore/simulated_data")
        
        # Perform analyses
        basic_stats = analytics.basic_stats()
        print("\nBasic Statistics:")
        print(basic_stats)
        
        print("\nSales by Customer Segment:")
        analytics.sales_by_segment().show()
        
        print("\nProduct Category Performance:")
        analytics.product_performance().show()
        
        print("\nRegional Analysis:")
        analytics.regional_analysis().show()
        
        print("\nMonthly Sales Trends:")
        analytics.time_series_analysis().show()
        
        print("\nCustomer Cohort Analysis:")
        analytics.customer_cohort_analysis().show()
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
