from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when
from processing.abstraction import StreamLayer
from processing.spark_streaming_utils import setup_logging
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType, StringType, DoubleType, LongType, BooleanType, StructType, StructField


logger = setup_logging()

class SilverLayer(StreamLayer):
    def __init__(self, spark, config, schemas):
        super().__init__(spark, config)
        self.schemas = schemas

    @property
    def layer_name(self) -> str:
        return "silver"

    @property
    def output_path(self) -> str:
        return self.config.SILVER_PATH
    
    @property
    def checkpoint_path(self) -> str:
        return f"{self.config.BASE_PATH}/checkpoints/silver"

    def read(self) -> DataFrame:
        logger.info("SILVER - Reading from Bronze...")
        return (
            self.spark.readStream
            .format("delta")
            .option("startingVersion", "0")
            .option("ignoreChanges", "true")
            .option("ignoreDeletes", "true")
            .load(self.config.BRONZE_PATH)
        )

    def bootstrap(self) -> bool:
        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            logger.info(f"Bootstrapping Silver Table at {self.output_path}...")
            try:
                
                schema = StructType([
                    StructField("symbol", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("open", DoubleType(), True),
                    StructField("high", DoubleType(), True),
                    StructField("low", DoubleType(), True),
                    StructField("volume", LongType(), True),
                    # timestamp converted to TimestampType
                    StructField("timestamp", TimestampType(), True),
                    StructField("source", StringType(), True), # from Bronze record.*
                    
                    # Bronze metadata
                    StructField("kafka_timestamp", StringType(), True), # kept as string in bronze? no
                    StructField("ingestion_timestamp", TimestampType(), True),
                    StructField("bronze_inserted_at", TimestampType(), True),

                    # Silver Calculations
                    StructField("spread", DoubleType(), True),
                    StructField("price_change", DoubleType(), True),
                    StructField("price_change_pct", DoubleType(), True),
                    StructField("is_anomaly", BooleanType(), True),
                    StructField("silver_processed_at", TimestampType(), True)
                ])
                
                # Create empty DF
                empty_df = self.spark.createDataFrame([], schema)
                
                # Write to Delta
                (
                    empty_df.write
                    .format("delta")
                    .mode("ignore")
                    .partitionBy("symbol")
                    .option("mergeSchema", "true")
                    .save(self.output_path)
                )
                logger.info("Silver Table bootstrapped successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to bootstrap Silver: {e}")
                return False
        return True

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            
            # Derived metrics
            .withColumn("spread", col("high") - col("low"))
            .withColumn("price_change", col("price") - col("open"))
            .withColumn(
                "price_change_pct",
                when(col("open") > 0, (col("price_change") / col("open") * 100))
                .otherwise(0.0)
            )
            
            # Anomaly detection
            .withColumn(
                "is_anomaly",
                when(
                    (col("volume") == 0) |
                    (col("price") <= 0) |
                    (col("open") <= 0) |
                    (col("high") < col("low")),
                    True
                ).otherwise(False)
            )
            
            .withColumn("silver_processed_at", current_timestamp())
            .withWatermark("timestamp", "10 minutes")
            .dropDuplicates(["symbol", "timestamp"])
        )