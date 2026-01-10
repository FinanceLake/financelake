from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, LongType, TimestampType
)

class StockSchemas:
    
    @staticmethod
    def get_input_schema():
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("timestamp", TimestampType(), False),
        ])
    
    @staticmethod
    def get_aggregated_schema():
        return StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("symbol", StringType(), False),
            StructField("avg_price", DoubleType(), True),
            StructField("min_price", DoubleType(), True),
            StructField("max_price", DoubleType(), True),
            StructField("volatility", DoubleType(), True),
            StructField("total_volume", LongType(), True),
            StructField("trade_count", LongType(), True),
            StructField("price_change_pct", DoubleType(), True)
        ])