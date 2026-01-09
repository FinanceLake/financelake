from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("FinanceLake-MLlib").getOrCreate()

df = spark.read.format("delta").load("/opt/spark-data/delta/stocks")

# Features simples : open, high, low, volume → predict close
assembler = VectorAssembler(inputCols=["open", "high", "low", "volume"], outputCol="features")
data = assembler.transform(df)

lr = LinearRegression(featuresCol="features", labelCol="close", maxIter=10)
model = lr.fit(data)

predictions = model.transform(data)
predictions.select("symbol", "date", "close", "prediction").show(20)

model.save("/opt/models/stock_price_lr")
