from pyspark.sql import DataFrame, SparkSession

class ParquetStorage:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def save(self, df: DataFrame, output_path: str = None, partition_cols=None, mode="overwrite"):
        """
        Save DataFrame as Parquet file(s).
        """
        if output_path is None:
            output_path = self.config['parquet_path']
        if partition_cols:
            df.write.mode(mode).partitionBy(partition_cols).parquet(output_path)
        else:
            df.write.mode(mode).parquet(output_path)
        return output_path

    def load(self, path: str = None):
        """
        Load DataFrame from Parquet file(s).
        """
        if path is None:
            path = self.config['parquet_path']
        return self.spark.read.parquet(path)

    def validate(self, path: str = None):
        """
        Validate Parquet data (basic: check if readable and not empty).
        """
        df = self.load(path)
        return df.count() > 0 