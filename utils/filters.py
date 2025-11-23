from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, abs as spark_abs

spark = SparkSession.builder.appName("FinanceLakeTransformations").getOrCreate()

def remove_outliers(df, column_name, threshold=2):
    """Supprime les valeurs aberrantes en utilisant le Z-score.
    
    Args:
        df: Spark DataFrame avec une colonne de valeurs.
        column_name: Nom de la colonne à filtrer.
        threshold: Seuil du Z-score (par défaut 3).
    
    Returns:
        Spark DataFrame sans les valeurs aberrantes.
    """
    # Calculer la moyenne et l’écart-type
    stats = df.select(
        mean(col(column_name)).alias("mean"),
        stddev(col(column_name)).alias("stddev")
    ).first()
    mean_val = stats["mean"]
    stddev_val = stats["stddev"]

    # Ajouter une colonne "zscore" pour debug
    df = df.withColumn("zscore", spark_abs((col(column_name) - mean_val) / stddev_val))

    # Afficher les z-scores (debug)
    print(" Vérification des Z-scores :")
    df.select("id", "date", column_name, "zscore").show()

    # Filtrer les lignes avec un Z-score inférieur au seuil
    df = df.filter(col("zscore") < threshold)

    return df
