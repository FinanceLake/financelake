from pyspark.sql import SparkSession
from processing.config import SparkConfig
from processing.schemas import StockSchemas
from processing.spark_streaming_utils import setup_logging
from delta.tables import DeltaTable
from typing import List, Dict
from processing.abstraction import StreamLayer
from processing.delta_lake.delta_lake_bronze import BronzeLayer
from processing.delta_lake.delta_lake_silver import SilverLayer
from processing.delta_lake.delta_lake_gold import GoldLayer

logger = setup_logging()

class StockStreamingProcessor:
    def __init__(self, config=None):
        self.config = config or SparkConfig()
        self.spark = None
        self.schemas = StockSchemas()
        self.layers: List[StreamLayer] = [] 
        self.queries: Dict[str, Any] = {}

    def create_spark_session(self):
        logger.info("Initializing Spark Session...")
        builder = SparkSession.builder.appName(self.config.APP_NAME)
        
        for k, v in self.config.get_spark_configs().items():
            builder = builder.config(k, v)
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.LOG_LEVEL)
        logger.info(f"Spark {self.spark.version} + Delta Lake activated")

        self.layers = [
            BronzeLayer(self.spark, self.config, self.schemas),
            SilverLayer(self.spark, self.config, self.schemas),
            GoldLayer(self.spark, self.config, self.schemas)
        ]
        
        return self.spark

    def start_streaming(self):
        self.create_spark_session()
        
        logger.info("=" * 70)
        logger.info("SOLID STREAMING PIPELINE - STARTUP")
        logger.info("=" * 70)

        try:
            for layer in self.layers:
                layer.bootstrap()
                query = layer.run()
                self.queries[layer.layer_name] = query

            logger.info("\n" + "=" * 70)
            logger.info("ALL STREAMS STARTED")
            logger.info("=" * 70)
            
            self.spark.streams.awaitAnyTermination()
            
        except KeyboardInterrupt:
            logger.info("\nGraceful shutdown requested...")
            self.stop_all_streams()
            logger.info("Pipeline stopped clean")
            
        except Exception as e:
            logger.error(f"Critical pipeline error: {e}", exc_info=True)
            self.stop_all_streams()
            raise

    def stop_all_streams(self):
        logger.info("Stopping all streams...")
        for name, query in self.queries.items():
            try:
                if query.isActive:
                    query.stop()
                    logger.info(f"{name} stopped")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")