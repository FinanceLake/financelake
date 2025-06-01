from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

spark = SparkSession.builder.appName("FinanceLakeTransformations").getOrCreate()

def min_max_normalize(df, column_name):
       """Normalise les données d’une colonne entre 0 et 1.
       
       Args:
           df: Spark DataFrame avec une colonne de valeurs.
           column_name: Nom de la colonne à normaliser.
       
       Returns:
           Spark DataFrame avec une nouvelle colonne 'normalized'.
       """
       # Calculer min et max
       min_val = df.select(min(col(column_name))).first()[0]
       max_val = df.select(max(col(column_name))).first()[0]
       # Normalisation : (x - min) / (max - min)
       df = df.withColumn("normalized", (col(column_name) - min_val) / (max_val - min_val))
       return df
<<<<<<< HEAD

=======
>>>>>>> 6983711d69a56d536a4b462422f614a57e19e1c9
