from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("FinanceLakeTransformations").getOrCreate()

def calculate_ema(df, column_name, span=7):
       """Calcule une moyenne mobile exponentielle sur un Spark DataFrame.
       
       Args:
           df: Spark DataFrame avec une colonne de dates et une colonne de valeurs.
           column_name: Nom de la colonne contenant les valeurs (ex. 'price').
           span: Période pour l’EMA (par défaut 7).
       
       Returns:
           Spark DataFrame avec une nouvelle colonne 'ema'.
       """
       window_spec = Window.orderBy("date").rowsBetween(-span + 1, 0)
       df = df.withColumn("ema", avg(col(column_name)).over(window_spec))
       return df
<<<<<<< HEAD


=======
>>>>>>> 6983711d69a56d536a4b462422f614a57e19e1c9
