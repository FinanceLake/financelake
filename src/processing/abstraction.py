from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from processing.spark_streaming_utils import setup_logging
from typing import Any

logger = setup_logging()

class StreamLayer(ABC):
    
    def __init__(self, spark: SparkSession, config: Any):
        self.spark = spark
        self.config = config

    @property
    @abstractmethod
    def layer_name(self) -> str:
        pass

    @property
    @abstractmethod
    def output_path(self) -> str:
        pass
    
    @property
    @abstractmethod
    def checkpoint_path(self) -> str:
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    def write(self, df: DataFrame) -> StreamingQuery:
        logger.info(f"Configuring usage of Delta sink for {self.layer_name} -> {self.output_path}")
        
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .partitionBy("symbol")
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .queryName(f"{self.layer_name}_ingestion")
            .start(self.output_path)
        )

    def bootstrap(self) -> bool:
        return True

    def run(self) -> StreamingQuery:
        logger.info(f"Starting {self.layer_name} Layer...")
        
        df_source = self.read()
        df_transformed = self.transform(df_source)
        query = self.write(df_transformed)
        
        logger.info(f"{self.layer_name} Layer started successfully.")
        return query
