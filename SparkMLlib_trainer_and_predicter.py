# ============================================================
#  FinanceLake Prediction Pipeline
# ============================================================

import os
import json
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lead
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

# ============================================================
# 1. CONFIGURATION SPARK
# ============================================================


spark = SparkSession.builder \
    .appName("FinanceLake_Prediction") \
    .getOrCreate()


# ============================================================
# 2. CHARGEMENT DES FICHIERS JSON
# ============================================================

# Définir l'ordre des clés à lire dans le fihier JSON

REPERTOIRE_DES_FICHIERS = "./data_output/gold"   # Chemin vers les fichiers JSON

KEYS_ORDER = [
    "symbol",
    "avg_price",
    "min_price",
    "max_price",
    "total_volume",
    "record_count",
    "price_volatility",
    "window_start",
    "window_end",
    "price_momentum",
    "bb_position"
]
# Définir la liste des noms de colonnes finales
HEADER = ["window_id"] + KEYS_ORDER

# Dictionnaires pour mémoriser les ID
compteurs_symboles = {}
window_id_map = {}

# La liste finale qui contiendra tous les tuples
data = []

# Trouver et trier les fichiers
chemin = os.path.join(REPERTOIRE_DES_FICHIERS, "*.json")
fichiers = sorted(glob.glob(chemin))

if not fichiers:
    print("AUCUN JSON TROUVÉ !")
    exit()

# Boucler sur chaque fichier
for fichier in fichiers:
    print(f"Traitement : {fichier}")
    with open(fichier, "r", encoding="utf-8") as f:
        liste = json.load(f)

# Boucler sur chaque enregistrement
        for record in liste:

            sym = record["symbol"]
            key = (sym, record["window_start"], record["window_end"])

            if key not in window_id_map:
                compteur = compteurs_symboles.get(sym, 0) + 1
                compteurs_symboles[sym] = compteur
                window_id_map[key] = compteur

            window_id = window_id_map[key]
            tuple_base = tuple(record[k] for k in KEYS_ORDER)
            data.append((window_id,) + tuple_base)

print("\nTotal enregistrements =", len(data))


# ============================================================
# 3. CONVERSION EN DATAFRAME
# ============================================================

df_raw = spark.createDataFrame(data, HEADER)
print("\n--- APERÇU DES DONNÉES BRUTES (AVANT AGRÉGATION) ")
df_raw.orderBy("window_id", "symbol").show(200)

df_noNull = df_raw.dropna()
df_filtered = df_noNull.filter(col("record_count") > 13)

gold_df = df_filtered.select(HEADER)

print("\n--- DONNÉES FINALES (AGRÉGÉES,SANS Null ) ---")
gold_df.show(20)


# ============================================================
# 4. TRANSFORMATION symbol → index
# ============================================================
# Transformer les Données Qualitatives (`symbol`)
indexer = StringIndexer(inputCol="symbol", outputCol="symbol_indexee")
indexed_df = indexer.fit(gold_df).transform(gold_df)


# ============================================================
# 5. CRÉATION DU LABEL (future price)
# ============================================================

# Créer le label
# Le label est 1 si le PRIX FUTUR > PRIX ACTUEL

w = Window.partitionBy("symbol").orderBy("window_id")

df_next = indexed_df.withColumn("next_avg_price", lead("avg_price").over(w))

df_label = df_next.withColumn(
    "label",
    when(col("next_avg_price") > col("avg_price"), 1.0).otherwise(0.0)
)


# ============================================================
# 6. FEATURES
# ============================================================

feature_cols = [
    "avg_price","min_price","max_price","total_volume","record_count",
    "price_volatility","price_momentum","bb_position","symbol_indexee"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

df_train_ready = df_label.filter(col("next_avg_price").isNotNull())
df_final = assembler.transform(df_train_ready).select("features", "label")

df_final.show(10, truncate=False)


# ============================================================
# 7. TRAIN / TEST SPLIT
# ============================================================

train, test = df_final.randomSplit([0.9, 0.1], seed=42)


# ============================================================
# 8. MODÈLE MLlib
# ============================================================

lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)

predictions = model.transform(test)


# ============================================================
# 9. EXTRACTION PROBABILITÉ PREDITE
# ============================================================

get_prob = udf(lambda prob, pred: float(prob[int(pred)]), DoubleType())
predictions = predictions.withColumn("predicted_probability",
                                     get_prob(col("probability"), col("prediction")))

predictions.select("label","prediction","predicted_probability").show()


# ============================================================
# 9.5. ÉVALUATION DU MODÈLE
# ============================================================

# Évaluation des Probabilités pour l'AUC (Area Under ROC)
# Ceci est l'équivalent moderne et recommandé de BinaryClassificationMetrics
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


#multiclass_evaluator_acc = MulticlassClassificationEvaluator(
multiclass_evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

multiclass_evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

accuracy = multiclass_evaluator_acc.evaluate(predictions)
f1_score = multiclass_evaluator_f1.evaluate(predictions)

print(f"Accuracy = {accuracy:.4f}")
print(f"F1-score = {f1_score:.4f}")


# ============================================================
# 10. PRÉDIRE LES FENÊTRES FUTURES
# ============================================================

future_raw = df_label.filter(col("next_avg_price").isNull())

assembler2 = VectorAssembler(inputCols=feature_cols, outputCol="features")
future_features = assembler2.transform(future_raw)

future_predictions = model.transform(future_features)
future_predictions = future_predictions.withColumn(
    "predicted_probability",
    get_prob(col("probability"), col("prediction"))
)

print("\n--- PRÉDICTIONS FUTURES ---")
future_predictions.select(
    "symbol", "avg_price", "prediction", "predicted_probability"
).show(truncate=False)


# ============================================================
# FIN
# ============================================================
print("\nPrediction terminé avec succès ✔")



