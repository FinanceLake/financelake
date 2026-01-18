from pyspark.sql.types import StructType, StringType, DoubleType

# Define the stock schema once
STOCK_SCHEMA = (
    StructType()
    .add("symbol", StringType())
    .add("price", DoubleType())
    .add("timestamp", StringType())
)