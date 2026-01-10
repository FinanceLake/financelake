from pyspark.sql import SparkSession

def load_tickers_dim(spark):
    data = [
        ("AAPL", "Apple Inc", "Technology"),
        ("GOOGL", "Alphabet Inc", "Technology"),
        ("MSFT", "Microsoft Corporation", "Technology"),
        ("AMZN", "Amazon.com Inc", "E-commerce"),
    ]
    schema = ["ticker", "company_name", "sector"]
    return spark.createDataFrame(data, schema)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestTickers").master("local[*]").getOrCreate()
    df = load_tickers_dim(spark)
    df.show()
    spark.stop()
