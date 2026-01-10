from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, window, count, avg, 
    min as spark_min, max as spark_max, sum as spark_sum, 
    stddev, round as spark_round, coalesce, lit
)
from processing.abstraction import StreamLayer
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

class GoldLayer(StreamLayer):
    def __init__(self, spark, config, schemas):
        super().__init__(spark, config)
        self.schemas = schemas

    @property
    def layer_name(self) -> str:
        return "gold"

    @property
    def output_path(self) -> str:
        return self.config.GOLD_PATH
    
    @property
    def checkpoint_path(self) -> str:
        return f"{self.config.BASE_PATH}/checkpoints/gold"

    def read(self) -> DataFrame:
        logger.info("GOLD - Reading from Silver...")
        return (
            self.spark.readStream
            .format("delta")
            .option("startingVersion", "0")
            .option("ignoreChanges", "true")
            .option("ignoreDeletes", "true")
            .load(self.config.SILVER_PATH)
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df
            .filter(col("is_anomaly") == False)
            .withWatermark("timestamp", self.config.WATERMARK_DELAY)
            
            .groupBy(
                window(col("timestamp"), self.config.WINDOW_DURATION),
                col("symbol")
            )
            .agg(
                count("*").alias("trade_count"),
                spark_round(avg("price"), 2).alias("avg_price"),
                spark_round(spark_min("price"), 2).alias("min_price"),
                spark_round(spark_max("price"), 2).alias("max_price"),
                spark_sum("volume").alias("total_volume"),
                spark_round(coalesce(stddev("price"), lit(0.0)), 4).alias("volatility"),
                spark_round(avg("price_change_pct"), 2).alias("avg_price_change_pct")
            )
            
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol"),
                col("avg_price"),
                col("min_price"),
                col("max_price"),
                col("volatility"),
                col("total_volume"),
                col("trade_count"),
                col("avg_price_change_pct")
            )
            .withColumn("gold_computed_at", current_timestamp())
        )