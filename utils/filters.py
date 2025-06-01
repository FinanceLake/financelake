from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev

spark = SparkSession.builder.appName("FinanceLakeTransformations").getOrCreate()

def remove_outliers(df, column_name, threshold=3):
       """Supprime les valeurs aberrantes en utilisant le Z-score.
       
       Args:
           df: Spark DataFrame avec une colonne de valeurs.
           column_name: Nom de la colonne à filtrer.
           threshold: Seuil du Z-score (par défaut 3).
       
       Returns:
           Spark DataFrame sans les valeurs aberrantes.
       """
       # Calculer la moyenne et l’écart-type
       stats = df.select(mean(col(column_name)).alias("mean"), stddev(col(column_name)).alias("stddev")).first()
       mean_val = stats["mean"]
       stddev_val = stats["stddev"]
       # Filtrer les valeurs avec un Z-score supérieur au seuil
       df = df.filter(abs((col(column_name) - mean_val) / stddev_val) < threshold)
       return df

<<<<<<< HEAD

=======
>>>>>>> 6983711d69a56d536a4b462422f614a57e19e1c9
