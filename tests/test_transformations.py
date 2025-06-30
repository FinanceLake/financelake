
from pyspark.sql import SparkSession
from utils.indicators import calculate_ema
from utils.normalization import min_max_normalize
from utils.filters import remove_outliers

spark = SparkSession.builder.appName("TestTransformations").getOrCreate()

# Créer un DataFrame de test
data = [
    (1, "2023-01-01", 100),
    (2, "2023-01-02", 110),
    (3, "2023-01-03", 120),
    (4, "2023-01-04", 500)  # Valeur aberrante
]
df = spark.createDataFrame(data, ["id", "date", "price"])


# Tester "calculate_ema"
ema_df = calculate_ema(df, "price", span=3)
ema_df.show()

# Tester "min_max_normalize"
norm_df = min_max_normalize(df, "price")
norm_df.show()

# Tester "remove_outliers"
clean_df = remove_outliers(df, "price", threshold=3)
clean_df.show()