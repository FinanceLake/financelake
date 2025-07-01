from data_processing.data_storage.storage_manager import StorageManager
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("SaveCleanedData").getOrCreate()
# df = spark.read.parquet("/data/cleaned/input.parquet")
# For demonstration, df is mocked
class DummyDF:
    def write(self):
        return self
    def mode(self, m):
        return self
    def partitionBy(self, cols):
        return self
    def parquet(self, path):
        print(f"Saved Parquet to {path}")
    def format(self, f):
        return self
    def save(self, path):
        print(f"Saved {f} to {path}")
    def count(self):
        return 1

df = DummyDF()

storage = StorageManager()
# Save as Parquet
storage.save(df, format='parquet')
# Save as Delta
storage.save(df, format='delta') 