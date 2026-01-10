from pyspark.sql import DataFrame
from pyspark.sql.functions import col, log, lag, lit, when, coalesce
from pyspark.sql.window import Window

def transform_data(df: DataFrame) -> DataFrame:
    """
    Calculates features for Random Forest.
    """
    w = Window.partitionBy("symbol").orderBy("window_start")
    
    # 1. Calculate Technical Indicators
    df = df \
        .withColumn("prev_price", lag("avg_price", 1).over(w)) \
        .withColumn("log_returns", log(col("avg_price") / col("prev_price"))) \
        .withColumn("log_volume", log(col("total_volume") + 1.0)) \
        .withColumn("volatility", coalesce(col("volatility"), lit(0.0)))
        
    # 2. Cleanup
    df = df.na.drop(subset=["log_returns", "log_volume", "avg_price"])
    return df