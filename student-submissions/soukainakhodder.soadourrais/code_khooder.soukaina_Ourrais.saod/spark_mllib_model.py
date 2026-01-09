"""
Spark MLlib Model - Tâche 3
Modélisation prédictive pour prédire si le prix d'une action va augmenter
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import os

class StockPricePredictor:
    """
    Modèle de prédiction utilisant Spark MLlib
    Prédit si le prix d'une action va augmenter ou diminuer
    """
    
    def __init__(self, spark_session):
        """
        Initialise le prédicteur avec une session Spark
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.model_lr = None
        self.model_rf = None
        print("✅ Prédicteur MLlib initialisé")
    
    def prepare_training_data(self, input_path="./output/stock_data"):
        """
        Prépare les données d'entraînement à partir des fichiers Parquet
        
        Crée des features:
        - price_lag1, price_lag2: Prix aux temps t-1, t-2
        - volume_lag1: Volume au temps t-1
        - price_change_lag1: Changement de prix au temps t-1
        - hour, day_of_week: Features temporelles
        
        Variable cible:
        - price_increase: 1 si le prix augmente, 0 sinon
        
        Args:
            input_path: Chemin vers les données Parquet
            
        Returns:
            DataFrame préparé pour l'entraînement
        """
        print("\n" + "="*60)
        print("📊 PRÉPARATION DES DONNÉES D'ENTRAÎNEMENT")
        print("="*60 + "\n")
        
        # Vérifier si les données existent
        if not os.path.exists(input_path):
            print(f"⚠️  Dossier {input_path} introuvable.")
            print("   Création de données synthétiques pour la démonstration...")
            return self._create_synthetic_data()
        
        # Lire les données Parquet
        try:
            df = self.spark.read.parquet(input_path)
            print(f"✅ Données chargées depuis {input_path}")
        except:
            print("⚠️  Erreur de lecture des données Parquet.")
            print("   Création de données synthétiques...")
            return self._create_synthetic_data()
        
        # Définir la fenêtre de partition par symbole, ordonnée par timestamp
        window_spec = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Créer les features avec décalage temporel
        df = df.withColumn("price_lag1", lag("price", 1).over(window_spec))
        df = df.withColumn("price_lag2", lag("price", 2).over(window_spec))
        df = df.withColumn("volume_lag1", lag("volume", 1).over(window_spec))
        df = df.withColumn("price_change_lag1", lag("price_change", 1).over(window_spec))
        
        # Features temporelles
        from pyspark.sql.functions import hour, dayofweek
        df = df.withColumn("hour", hour("timestamp"))
        df = df.withColumn("day_of_week", dayofweek("timestamp"))
        
        # Variable cible: 1 si le prix augmente, 0 sinon
        df = df.withColumn(
            "price_increase",
            when(col("price_change") > 0, 1.0).otherwise(0.0)
        )
        
        # Supprimer les lignes avec valeurs nulles
        df = df.na.drop()
        
        print("📋 Features créées:")
        print("   - price_lag1, price_lag2: Prix historiques")
        print("   - volume_lag1: Volume historique")
        print("   - price_change_lag1: Changement de prix historique")
        print("   - hour, day_of_week: Features temporelles")
        print("\n🎯 Variable cible: price_increase (1=augmentation, 0=diminution)")
        
        print(f"\n📊 Nombre d'enregistrements: {df.count()}")
        df.select("symbol", "price", "price_lag1", "price_increase").show(10)
        
        return df
    
    def _create_synthetic_data(self):
        """
        Crée des données synthétiques pour la démonstration
        """
        from pyspark.sql.functions import rand, randn, when
        from config import STOCK_SYMBOLS, BASE_PRICES
        
        # Créer des données synthétiques
        data = []
        for symbol in STOCK_SYMBOLS:
            base_price = BASE_PRICES[symbol]
            for i in range(1000):
                price = base_price * (1 + (i % 20 - 10) * 0.01)
                data.append((
                    symbol,
                    price,
                    int(100000 + (i % 500000)),
                    (i % 24),  # hour
                    (i % 7),   # day_of_week
                ))
        
        df = self.spark.createDataFrame(
            data,
            ["symbol", "price", "volume", "hour", "day_of_week"]
        )
        
        # Créer les features lag
        window_spec = Window.partitionBy("symbol").orderBy("price")
        df = df.withColumn("price_lag1", lag("price", 1).over(window_spec))
        df = df.withColumn("price_lag2", lag("price", 2).over(window_spec))
        df = df.withColumn("volume_lag1", lag("volume", 1).over(window_spec))
        
        # Calculer price_change_lag1
        df = df.withColumn(
            "price_change_lag1",
            ((col("price") - col("price_lag1")) / col("price_lag1") * 100)
        )
        
        # Variable cible
        df = df.withColumn(
            "price_increase",
            when(rand() > 0.5, 1.0).otherwise(0.0)
        )
        
        df = df.na.drop()
        
        print("✅ Données synthétiques créées pour la démonstration")
        return df
    
    def build_features(self, df):
        """
        Construit le vecteur de features pour MLlib
        
        Args:
            df: DataFrame avec les features
            
        Returns:
            DataFrame avec colonne 'features'
        """
        feature_columns = [
            "price_lag1", "price_lag2", "volume_lag1",
            "price_change_lag1", "hour", "day_of_week"
        ]
        
        # Assembler les features en un vecteur
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features_raw"
        )
        
        # Normaliser les features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Appliquer la transformation
        df = assembler.transform(df)
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)
        
        print("✅ Features assemblées et normalisées")
        
        return df, feature_columns
    
    def train_logistic_regression(self, train_df, test_df):
        """
        Entraîne un modèle de régression logistique
        
        Args:
            train_df: Données d'entraînement
            test_df: Données de test
            
        Returns:
            Modèle entraîné et métriques
        """
        print("\n" + "="*60)
        print("🤖 ENTRAÎNEMENT: Régression Logistique")
        print("="*60 + "\n")
        
        # Créer le modèle
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="price_increase",
            maxIter=100,
            regParam=0.01
        )
        
        # Entraîner
        self.model_lr = lr.fit(train_df)
        
        # Prédictions
        predictions = self.model_lr.transform(test_df)
        
        # Évaluation
        metrics = self._evaluate_model(predictions, "Régression Logistique")
        
        # Coefficients
        print("\n📊 Coefficients du modèle:")
        print(f"   Intercept: {self.model_lr.intercept:.4f}")
        print(f"   Coefficients: {self.model_lr.coefficients.toArray()}")
        
        return self.model_lr, metrics
    
    def train_random_forest(self, train_df, test_df):
        """
        Entraîne un modèle Random Forest
        
        Args:
            train_df: Données d'entraînement
            test_df: Données de test
            
        Returns:
            Modèle entraîné et métriques
        """
        print("\n" + "="*60)
        print("🌲 ENTRAÎNEMENT: Random Forest")
        print("="*60 + "\n")
        
        # Créer le modèle
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="price_increase",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Entraîner
        self.model_rf = rf.fit(train_df)
        
        # Prédictions
        predictions = self.model_rf.transform(test_df)
        
        # Évaluation
        metrics = self._evaluate_model(predictions, "Random Forest")
        
        # Importance des features
        print("\n📊 Importance des features:")
        feature_importance = self.model_rf.featureImportances.toArray()
        for i, importance in enumerate(feature_importance):
            print(f"   Feature {i}: {importance:.4f}")
        
        return self.model_rf, metrics
    
    def _evaluate_model(self, predictions, model_name):
        """
        Évalue les performances d'un modèle
        
        Args:
            predictions: DataFrame avec les prédictions
            model_name: Nom du modèle
            
        Returns:
            Dictionnaire avec les métriques
        """
        print(f"\n📊 ÉVALUATION: {model_name}")
        print("-" * 60)
        
        # Afficher quelques prédictions
        predictions.select(
            "price_increase", "prediction", "probability"
        ).show(10, truncate=False)
        
        # Évaluateur pour classification binaire
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="price_increase",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        # Évaluateur pour métriques multiclasse
        evaluator_accuracy = MulticlassClassificationEvaluator(
            labelCol="price_increase",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="price_increase",
            predictionCol="prediction",
            metricName="weightedPrecision"
        )
        
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="price_increase",
            predictionCol="prediction",
            metricName="weightedRecall"
        )
        
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="price_increase",
            predictionCol="prediction",
            metricName="f1"
        )
        
        # Calculer les métriques
        auc = evaluator_auc.evaluate(predictions)
        accuracy = evaluator_accuracy.evaluate(predictions)
        precision = evaluator_precision.evaluate(predictions)
        recall = evaluator_recall.evaluate(predictions)
        f1 = evaluator_f1.evaluate(predictions)
        
        print(f"\n✅ Métriques pour {model_name}:")
        print(f"   - AUC (Area Under ROC): {auc:.4f}")
        print(f"   - Accuracy: {accuracy:.4f}")
        print(f"   - Precision: {precision:.4f}")
        print(f"   - Recall: {recall:.4f}")
        print(f"   - F1-Score: {f1:.4f}")
        
        return {
            "auc": auc,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1
        }
    
    def compare_models(self, metrics_lr, metrics_rf):
        """
        Compare les performances des deux modèles
        
        Args:
            metrics_lr: Métriques de la régression logistique
            metrics_rf: Métriques du Random Forest
        """
        print("\n" + "="*60)
        print("⚖️  COMPARAISON DES MODÈLES")
        print("="*60 + "\n")
        
        print(f"{'Métrique':<20} {'Logistic Regression':<25} {'Random Forest':<25}")
        print("-" * 70)
        
        for metric in ["auc", "accuracy", "precision", "recall", "f1"]:
            lr_val = metrics_lr[metric]
            rf_val = metrics_rf[metric]
            winner = "🏆 LR" if lr_val > rf_val else "🏆 RF"
            print(f"{metric.upper():<20} {lr_val:<25.4f} {rf_val:<25.4f} {winner}")
        
        # Déterminer le meilleur modèle global
        lr_avg = sum(metrics_lr.values()) / len(metrics_lr)
        rf_avg = sum(metrics_rf.values()) / len(metrics_rf)
        
        print("\n" + "="*60)
        if rf_avg > lr_avg:
            print("🏆 GAGNANT: Random Forest")
            print(f"   Score moyen: {rf_avg:.4f} vs {lr_avg:.4f}")
        else:
            print("🏆 GAGNANT: Logistic Regression")
            print(f"   Score moyen: {lr_avg:.4f} vs {rf_avg:.4f}")
        print("="*60)
    
    def run_full_training(self):
        """
        Exécute le pipeline complet d'entraînement
        """
        print("\n" + "="*70)
        print("🚀 DÉMARRAGE DE L'ENTRAÎNEMENT MLLIB")
        print("="*70)
        
        # Préparer les données
        df = self.prepare_training_data()
        
        # Construire les features
        df, feature_columns = self.build_features(df)
        
        # Split train/test
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"\n📊 Split des données:")
        print(f"   - Entraînement: {train_df.count()} enregistrements")
        print(f"   - Test: {test_df.count()} enregistrements")
        
        # Entraîner les modèles
        model_lr, metrics_lr = self.train_logistic_regression(train_df, test_df)
        model_rf, metrics_rf = self.train_random_forest(train_df, test_df)
        
        # Comparer les modèles
        self.compare_models(metrics_lr, metrics_rf)
        
        print("\n✅ Entraînement MLlib terminé!")
        
        return {
            "lr_model": model_lr,
            "rf_model": model_rf,
            "lr_metrics": metrics_lr,
            "rf_metrics": metrics_rf
        }

if __name__ == "__main__":
    # Ce module peut être exécuté indépendamment pour tester
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("MLlibTest") \
        .master("local[*]") \
        .getOrCreate()
    
    predictor = StockPricePredictor(spark)
    results = predictor.run_full_training()
    
    spark.stop()

