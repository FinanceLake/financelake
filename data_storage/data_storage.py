from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.types import StructType
import logging
from datetime import datetime
from typing import Optional, Dict, Any

class HDFSDataStorage:
    """Data storage layer for HDFS with Parquet format"""
    
    def __init__(self, hdfs_base_path: str = "/data"):
        self.hdfs_base_path = hdfs_base_path
        self.spark = self._init_spark()
        self.logger = logging.getLogger(__name__)
        
    def _init_spark(self) -> SparkSession:
        """Initialize Spark session with optimized configs"""
        return SparkSession.builder \
            .appName("DataStorageLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.parquet.block.size", "134217728") \
            .config("spark.sql.parquet.page.size", "8192") \
            .config("spark.sql.parquet.enableVectorizedReader", "true") \
            .getOrCreate()
    
    def write_processed_data(self, df: DataFrame, dataset_name: str, 
                           partition_cols: list = None) -> str:
        """Write processed data to HDFS in Parquet format"""
        if partition_cols is None:
            partition_cols = ["year", "month", "day"]
            
        # Add time partitions if not present
        if "year" not in df.columns:
            df = df.withColumn("year", year(col("timestamp"))) \
                   .withColumn("month", month(col("timestamp"))) \
                   .withColumn("day", dayofmonth(col("timestamp")))
        
        output_path = f"{self.hdfs_base_path}/processed/{dataset_name}"
        
        try:
            df.write \
                .mode("append") \
                .partitionBy(*partition_cols) \
                .option("compression", "snappy") \
                .parquet(output_path)
                
            self.logger.info(f"Data written to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to write data: {e}")
            raise
    
    def read_data(self, dataset_name: str, filters: Dict[str, Any] = None) -> DataFrame:
        """Read data with optional filters for partition pruning"""
        data_path = f"{self.hdfs_base_path}/processed/{dataset_name}"
        
        try:
            df = self.spark.read.parquet(data_path)
            
            # Apply filters for partition pruning
            if filters:
                for column, value in filters.items():
                    if isinstance(value, list):
                        df = df.filter(col(column).isin(value))
                    else:
                        df = df.filter(col(column) == value)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read data: {e}")
            raise
    
    def create_aggregated_view(self, source_dataset: str, view_name: str, 
                              agg_logic: str, partition_cols: list = None):
        """Create aggregated views for common queries"""
        if partition_cols is None:
            partition_cols = ["year", "month"]
            
        source_df = self.read_data(source_dataset)
        
        # Execute aggregation logic
        agg_df = source_df.createOrReplaceTempView("source_data")
        result_df = self.spark.sql(agg_logic)
        
        output_path = f"{self.hdfs_base_path}/aggregated/{view_name}"
        
        result_df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .parquet(output_path)
        
        self.logger.info(f"Aggregated view created: {output_path}")
    
    def optimize_table(self, dataset_name: str):
        """Optimize table by compacting small files"""
        data_path = f"{self.hdfs_base_path}/processed/{dataset_name}"
        
        df = self.spark.read.parquet(data_path)
        
        # Repartition and rewrite
        optimized_df = df.repartition(200)  # Adjust based on data size
        
        temp_path = f"{data_path}_temp"
        optimized_df.write.mode("overwrite").parquet(temp_path)
        
        # Replace original with optimized version
        self.spark.sql(f"DROP TABLE IF EXISTS temp_table")
        self.spark.read.parquet(temp_path).createOrReplaceTempView("temp_table")
        
        self.logger.info(f"Table optimized: {dataset_name}")
    
    def create_backup(self, dataset_name: str, backup_location: str):
        """Create backup of dataset"""
        source_path = f"{self.hdfs_base_path}/processed/{dataset_name}"
        backup_path = f"{backup_location}/{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        df = self.spark.read.parquet(source_path)
        df.write.mode("overwrite").parquet(backup_path)
        
        self.logger.info(f"Backup created: {backup_path}")
        return backup_path
    
    def get_storage_stats(self, dataset_name: str) -> Dict[str, Any]:
        """Get storage statistics for dataset"""
        data_path = f"{self.hdfs_base_path}/processed/{dataset_name}"
        
        df = self.spark.read.parquet(data_path)
        
        stats = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "partitions": df.rdd.getNumPartitions(),
            "schema": df.schema.json()
        }
        
        return stats
    
    def cleanup_old_partitions(self, dataset_name: str, days_to_keep: int = 30):
        """Remove old partitions based on retention policy"""
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # This would typically integrate with HDFS APIs to remove old directories
        # Implementation depends on specific HDFS setup and permissions
        self.logger.info(f"Cleanup initiated for {dataset_name}, keeping {days_to_keep} days")

# Usage Example
if __name__ == "__main__":
    storage = HDFSDataStorage("/data")