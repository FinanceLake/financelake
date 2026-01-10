from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp
from processing.abstraction import StreamLayer
from processing.spark_streaming_utils import setup_logging
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

logger = setup_logging()

class BronzeLayer(StreamLayer):
    def __init__(self, spark, config, schemas):
        super().__init__(spark, config)
        self.schemas = schemas

    @property
    def layer_name(self) -> str:
        return "bronze"
    
    @property
    def output_path(self) -> str:
        return self.config.BRONZE_PATH
    
    @property
    def checkpoint_path(self) -> str:
        return f"{self.config.BASE_PATH}/checkpoints/bronze"

    def read(self) -> DataFrame:
        logger.info("BRONZE - Reading from Kafka...")
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", self.config.KAFKA_STARTING_OFFSETS)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self.config.MAX_OFFSETS_PER_TRIGGER)
            .load()
        )

    def bootstrap(self) -> bool:
        if not DeltaTable.isDeltaTable(self.spark, self.output_path):
            logger.info(f"Bootstrapping Bronze Table at {self.output_path}...")
            try:
                
                # Get base schema from schemas.py
                schema = self.schemas.get_input_schema()
                
                # Add extra columns added in transform
                schema = schema.add("ingestion_timestamp", TimestampType())
                schema = schema.add("bronze_inserted_at", TimestampType())
                
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
                logger.info("Bronze Table bootstrapped successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to bootstrap Bronze: {e}")
                return False
        return True

    def transform(self, df: DataFrame) -> DataFrame:
        input_schema = self.schemas.get_input_schema()
        
        return (
            df
            .selectExpr(
                "CAST(value AS STRING) as json_data",
                "timestamp as kafka_timestamp"
            )
            .select(
                from_json(col("json_data"), input_schema).alias("record"),
                col("kafka_timestamp")
            )
            .select(
                "record.*",
                col("kafka_timestamp").alias("ingestion_timestamp")
            )
            .withColumn("bronze_inserted_at", current_timestamp())
            .withWatermark("timestamp", "1 minute")
            .dropDuplicates(["symbol", "timestamp"])
        )