"""
Streaming ML Scoring - MLlib intégré dans le streaming
Real-time scoring sur flux de données avec entraînement continu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, when, abs as spark_abs, current_timestamp,
    struct, array, udf
)
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from delta.tables import DeltaTable
from config import DELTA_SILVER_PATH, DELTA_GOLD_PATH, MODEL_CHECKPOINT_DIR
import os

class StreamingMLScoring:
    """
    Scoring ML en temps réel sur flux de données
    """
    
    def __init__(self, spark_session):
        """
        Initialise le système de scoring ML
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.model = None
        self.scaler_model = None
        self.feature_assembler = None
        
        os.makedirs(MODEL_CHECKPOINT_DIR, exist_ok=True)
        
        print("✅ Système de scoring ML en streaming initialisé")
    
    def prepare_features_from_silver(self, df):
        """
        Prépare les features depuis les données Silver pour le ML
        
        Args:
            df: DataFrame depuis Silver
            
        Returns:
            DataFrame avec features préparées
        """
        # Créer des features pour le ML
        feature_df = df \
            .withColumn(
                "price_increase_target",
                when(col("avg_price_change") > 0, 1.0).otherwise(0.0)
            ) \
            .withColumn(
                "price_momentum",
                col("avg_price_change") / (col("avg_price") + 1e-10)
            ) \
            .withColumn(
                "volume_ratio",
                col("total_volume") / (col("transaction_count") + 1e-10)
            ) \
            .withColumn(
                "volatility_normalized",
                col("volatility") / (col("avg_price") + 1e-10)
            ) \
            .withColumn(
                "price_range_ratio",
                (col("max_price") - col("min_price")) / (col("avg_price") + 1e-10)
            )
        
        return feature_df
    
    def prepare_features_from_gold(self, df):
        """
        Prépare les features depuis les données Gold pour le ML
        
        Args:
            df: DataFrame depuis Gold
            
        Returns:
            DataFrame avec features préparées
        """
        feature_df = df \
            .withColumn(
                "trend_score",
                when(col("price_trend") == "STRONG_UP", 2.0)
                .when(col("price_trend") == "UP", 1.0)
                .when(col("price_trend") == "STRONG_DOWN", -2.0)
                .when(col("price_trend") == "DOWN", -1.0)
                .otherwise(0.0)
            ) \
            .withColumn(
                "volatility_score",
                when(col("volatility_category") == "HIGH", 2.0)
                .when(col("volatility_category") == "MEDIUM", 1.0)
                .otherwise(0.0)
            ) \
            .withColumn(
                "volume_score",
                when(col("volume_category") == "HIGH", 2.0)
                .when(col("volume_category") == "MEDIUM", 1.0)
                .otherwise(0.0)
            ) \
            .withColumn(
                "price_range_score",
                when(col("price_range_percent") > 10, 2.0)
                .when(col("price_range_percent") > 5, 1.0)
                .otherwise(0.0)
            )
        
        return feature_df
    
    def train_model_batch(self, input_path=None, use_silver=True):
        """
        Entraîne un modèle ML sur des données batch
        
        Args:
            input_path: Chemin vers les données (optionnel, utilise Delta par défaut)
            use_silver: Si True, utilise Silver, sinon Gold
        """
        print("\n" + "="*60)
        print("🤖 ENTRAÎNEMENT DU MODÈLE ML (BATCH)")
        print("="*60 + "\n")
        
        # Lire les données
        if input_path:
            df = self.spark.read.format("delta").load(input_path)
        elif use_silver:
            if not DeltaTable.isDeltaTable(self.spark, DELTA_SILVER_PATH):
                print("⚠️  Table Silver n'existe pas encore")
                return None
            df = self.spark.read.format("delta").load(DELTA_SILVER_PATH)
        else:
            if not DeltaTable.isDeltaTable(self.spark, DELTA_GOLD_PATH):
                print("⚠️  Table Gold n'existe pas encore")
                return None
            df = self.spark.read.format("delta").load(DELTA_GOLD_PATH)
        
        print(f"📊 Données chargées: {df.count()} enregistrements")
        
        # Préparer les features
        if use_silver:
            feature_df = self.prepare_features_from_silver(df)
            feature_cols = [
                "avg_price", "volatility", "total_volume", "transaction_count",
                "price_momentum", "volume_ratio", "volatility_normalized", "price_range_ratio"
            ]
            target_col = "price_increase_target"
        else:
            feature_df = self.prepare_features_from_gold(df)
            feature_cols = [
                "overall_avg_price", "overall_volatility", "cumulative_volume",
                "trend_score", "volatility_score", "volume_score", "price_range_score"
            ]
            target_col = "trend_score"
        
        # Assembler les features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Normaliser
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Créer le modèle (Random Forest pour meilleures performances)
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol=target_col,
            numTrees=50,
            maxDepth=10,
            seed=42
        )
        
        # Pipeline
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Entraîner
        print("🔄 Entraînement en cours...")
        pipeline_model = pipeline.fit(feature_df)
        
        # Sauvegarder le modèle
        model_path = f"{MODEL_CHECKPOINT_DIR}/streaming_ml_model"
        pipeline_model.write().overwrite().save(model_path)
        
        self.model = pipeline_model
        self.feature_assembler = assembler
        self.scaler_model = scaler
        
        print(f"✅ Modèle entraîné et sauvegardé: {model_path}")
        
        # Évaluer le modèle
        predictions = pipeline_model.transform(feature_df)
        evaluator = BinaryClassificationEvaluator(
            labelCol=target_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(predictions)
        print(f"📊 AUC du modèle: {auc:.4f}")
        
        return pipeline_model
    
    def load_model(self, model_path=None):
        """
        Charge un modèle ML pré-entraîné
        
        Args:
            model_path: Chemin vers le modèle (optionnel)
        """
        if model_path is None:
            model_path = f"{MODEL_CHECKPOINT_DIR}/streaming_ml_model"
        
        if not os.path.exists(model_path):
            print(f"⚠️  Modèle non trouvé: {model_path}")
            return None
        
        print(f"📂 Chargement du modèle: {model_path}")
        self.model = PipelineModel.load(model_path)
        print("✅ Modèle chargé")
        return self.model
    
    def score_streaming_data(self, streaming_df, use_silver=True):
        """
        Applique le scoring ML sur un flux de données
        
        Args:
            streaming_df: DataFrame en streaming
            use_silver: Si True, prépare les features comme Silver
            
        Returns:
            DataFrame avec prédictions
        """
        if self.model is None:
            print("⚠️  Modèle non chargé. Entraînement d'un nouveau modèle...")
            self.train_model_batch(use_silver=use_silver)
        
        if self.model is None:
            print("❌ Impossible de charger ou entraîner le modèle")
            return None
        
        # Préparer les features selon le type de données
        if use_silver:
            feature_df = self.prepare_features_from_silver(streaming_df)
        else:
            feature_df = self.prepare_features_from_gold(streaming_df)
        
        # Appliquer le modèle
        predictions = self.model.transform(feature_df)
        
        # Ajouter des colonnes utiles
        scored_df = predictions \
            .withColumn(
                "prediction_label",
                when(col("prediction") == 1.0, "INCREASE")
                .otherwise("DECREASE")
            ) \
            .withColumn(
                "confidence",
                col("probability")[1]  # Probabilité de classe positive
            ) \
            .withColumn("scoring_timestamp", current_timestamp())
        
        return scored_df
    
    def create_streaming_scoring_query(self, input_path=None, use_silver=True):
        """
        Crée une query de streaming avec scoring ML en temps réel
        
        Args:
            input_path: Chemin vers les données en streaming (optionnel)
            use_silver: Si True, lit depuis Silver, sinon Gold
        """
        print("\n" + "="*60)
        print("🚀 CRÉATION DU STREAMING SCORING ML")
        print("="*60 + "\n")
        
        # Charger ou entraîner le modèle
        if self.model is None:
            self.load_model()
            if self.model is None:
                self.train_model_batch(use_silver=use_silver)
        
        if self.model is None:
            print("❌ Impossible d'initialiser le modèle")
            return None
        
        # Lire depuis Delta en streaming
        if input_path:
            streaming_df = self.spark.readStream.format("delta").load(input_path)
        elif use_silver:
            streaming_df = self.spark.readStream.format("delta").load(DELTA_SILVER_PATH)
        else:
            streaming_df = self.spark.readStream.format("delta").load(DELTA_GOLD_PATH)
        
        # Fonction pour appliquer le scoring sur chaque batch
        def score_batch(batch_df, batch_id):
            """
            Applique le scoring ML sur chaque batch
            """
            if batch_df.count() > 0:
                scored_df = self.score_streaming_data(batch_df, use_silver=use_silver)
                
                if scored_df is not None:
                    # Afficher les prédictions
                    print(f"\n📊 Batch {batch_id} - Prédictions:")
                    scored_df.select(
                        "symbol", "prediction_label", "confidence", "scoring_timestamp"
                    ).show(truncate=False)
        
        # Créer la query de streaming
        query = streaming_df.writeStream \
            .foreachBatch(score_batch) \
            .outputMode("update") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        print("✅ Streaming scoring ML démarré")
        return query
    
    def write_scored_predictions(self, scored_df, output_path="./delta/ml_predictions"):
        """
        Écrit les prédictions scorées vers Delta
        
        Args:
            scored_df: DataFrame avec prédictions
            output_path: Chemin de sortie
        """
        os.makedirs(output_path, exist_ok=True)
        
        # Sélectionner les colonnes importantes
        predictions_df = scored_df.select(
            "symbol",
            "prediction",
            "prediction_label",
            "confidence",
            "probability",
            "scoring_timestamp"
        )
        
        # Écrire vers Delta
        if DeltaTable.isDeltaTable(self.spark, output_path):
            delta_table = DeltaTable.forPath(self.spark, output_path)
            delta_table.alias("target").merge(
                predictions_df.alias("source"),
                "target.symbol = source.symbol AND target.scoring_timestamp = source.scoring_timestamp"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            predictions_df.write \
                .format("delta") \
                .mode("append") \
                .save(output_path)
        
        print(f"✅ Prédictions sauvegardées: {output_path}")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("StreamingMLScoring") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    scorer = StreamingMLScoring(spark)
    
    # Entraîner le modèle
    scorer.train_model_batch(use_silver=True)
    
    # Créer le streaming scoring
    query = scorer.create_streaming_scoring_query(use_silver=True)
    
    if query:
        query.awaitTermination(timeout=60)
    
    spark.stop()

