from pyspark.sql import DataFrame, SparkSession

class DeltaStorage:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def save(self, df: DataFrame, output_path: str = None, partition_cols=None, mode="overwrite"):
        """
        Save DataFrame as Delta Lake table.
        """
        if output_path is None:
            output_path = self.config['delta_path']
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        writer.save(output_path)
        return output_path

    def load(self, path: str = None):
        """
        Load DataFrame from Delta Lake table.
        """
        if path is None:
            path = self.config['delta_path']
        return self.spark.read.format("delta").load(path)

    def validate(self, path: str = None):
        """
        Validate Delta data (basic: check if readable and not empty).
        """
        df = self.load(path)
        return df.count() > 0 