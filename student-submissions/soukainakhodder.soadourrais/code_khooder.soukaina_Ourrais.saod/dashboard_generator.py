"""
Dashboard Generator - Génération automatique de graphiques
Génère des graphiques depuis les tables Delta et sauvegarde avec interprétations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, min as spark_min
from delta.tables import DeltaTable
from config import DELTA_BRONZE_PATH, DELTA_SILVER_PATH, DELTA_GOLD_PATH, DASHBOARD_OUTPUT_DIR
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
from datetime import datetime

class DashboardGenerator:
    """
    Générateur de dashboard avec graphiques et interprétations automatiques
    """
    
    def __init__(self, spark_session):
        """
        Initialise le générateur de dashboard
        
        Args:
            spark_session: Session Spark active
        """
        self.spark = spark_session
        self.output_dir = DASHBOARD_OUTPUT_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Configuration du style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (14, 8)
        plt.rcParams['font.size'] = 10
        
        print(f"✅ Générateur de dashboard initialisé")
        print(f"📁 Dossier de sortie: {self.output_dir}")
    
    def load_delta_table(self, table_path, table_name):
        """
        Charge une table Delta et retourne un DataFrame Pandas
        
        Args:
            table_path: Chemin de la table Delta
            table_name: Nom de la table (pour les messages)
            
        Returns:
            DataFrame Pandas ou None
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                print(f"⚠️  Table {table_name} n'existe pas encore: {table_path}")
                return None
            
            spark_df = self.spark.read.format("delta").load(table_path)
            pandas_df = spark_df.toPandas()
            
            print(f"✅ {table_name} chargée: {len(pandas_df)} enregistrements")
            return pandas_df
        except Exception as e:
            print(f"❌ Erreur lors du chargement de {table_name}: {e}")
            return None
    
    def plot_price_evolution(self, df, interpretation=True):
        """
        Graphique: Évolution du prix moyen par symbole
        
        Args:
            df: DataFrame Pandas
            interpretation: Si True, génère une interprétation
        """
        if df is None or len(df) == 0:
            print("⚠️  Pas de données pour le graphique d'évolution des prix")
            return
        
        plt.figure(figsize=(16, 8))
        
        # Trier par timestamp si disponible
        if 'window_start' in df.columns:
            df = df.sort_values('window_start')
            x_col = 'window_start'
        elif 'last_window_time' in df.columns:
            df = df.sort_values('last_window_time')
            x_col = 'last_window_time'
        else:
            x_col = None
        
        if x_col:
            for symbol in df['symbol'].unique():
                symbol_data = df[df['symbol'] == symbol]
                price_col = 'avg_price' if 'avg_price' in symbol_data.columns else 'overall_avg_price'
                if price_col in symbol_data.columns:
                    plt.plot(
                        symbol_data[x_col],
                        symbol_data[price_col],
                        marker='o',
                        label=symbol,
                        linewidth=2,
                        markersize=4
                    )
        else:
            # Graphique en barres si pas de timestamp
            price_col = 'avg_price' if 'avg_price' in df.columns else 'overall_avg_price'
            if price_col in df.columns:
                price_avg = df.groupby('symbol')[price_col].mean().sort_values(ascending=False)
                plt.bar(price_avg.index, price_avg.values, color=sns.color_palette("husl", len(price_avg)))
        
        plt.title('Évolution du Prix Moyen par Action', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Temps' if x_col else 'Symbole', fontsize=12)
        plt.ylabel('Prix Moyen ($)', fontsize=12)
        plt.legend(title='Symbole', bbox_to_anchor=(1.05, 1), loc='upper left')
        if x_col:
            plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'price_evolution.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Graphique sauvegardé: {filepath}")
        
        if interpretation:
            self._generate_interpretation(
                filepath,
                "Évolution des Prix",
                "Ce graphique montre l'évolution du prix moyen de chaque action dans le temps. "
                "Les tendances haussières indiquent une croissance, tandis que les tendances baissières "
                "suggèrent une décroissance. Les variations importantes peuvent indiquer de la volatilité."
            )
    
    def plot_volatility_analysis(self, df, interpretation=True):
        """
        Graphique: Analyse de la volatilité
        
        Args:
            df: DataFrame Pandas
            interpretation: Si True, génère une interprétation
        """
        if df is None or len(df) == 0:
            print("⚠️  Pas de données pour l'analyse de volatilité")
            return
        
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # Graphique 1: Volatilité moyenne par symbole
        volatility_col = 'volatility' if 'volatility' in df.columns else 'overall_volatility'
        if volatility_col in df.columns:
            volatility_avg = df.groupby('symbol')[volatility_col].mean().sort_values(ascending=False)
            colors = sns.color_palette("RdYlGn_r", len(volatility_avg))
            bars = axes[0].bar(volatility_avg.index, volatility_avg.values, color=colors)
            
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    axes[0].text(
                        bar.get_x() + bar.get_width()/2.,
                        height,
                        f'{height:.2f}',
                        ha='center',
                        va='bottom',
                        fontsize=9
                    )
            
            axes[0].set_title('Volatilité Moyenne par Action', fontsize=14, fontweight='bold')
            axes[0].set_xlabel('Symbole', fontsize=11)
            axes[0].set_ylabel('Volatilité (Écart-Type)', fontsize=11)
            axes[0].grid(True, alpha=0.3, axis='y')
        
        # Graphique 2: Distribution de la volatilité
        if volatility_col in df.columns:
            axes[1].hist(
                df[volatility_col].dropna(),
                bins=20,
                color='steelblue',
                edgecolor='black',
                alpha=0.7
            )
            axes[1].axvline(
                df[volatility_col].mean(),
                color='red',
                linestyle='--',
                linewidth=2,
                label=f'Moyenne: {df[volatility_col].mean():.2f}'
            )
            axes[1].set_title('Distribution de la Volatilité', fontsize=14, fontweight='bold')
            axes[1].set_xlabel('Volatilité', fontsize=11)
            axes[1].set_ylabel('Fréquence', fontsize=11)
            axes[1].legend()
            axes[1].grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'volatility_analysis.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Graphique sauvegardé: {filepath}")
        
        if interpretation:
            self._generate_interpretation(
                filepath,
                "Analyse de Volatilité",
                "La volatilité mesure l'ampleur des variations de prix. Une volatilité élevée indique "
                "un risque plus important mais aussi des opportunités de trading. Les actions avec une "
                "volatilité faible sont généralement plus stables mais offrent moins d'opportunités de profit."
            )
    
    def plot_volume_analysis(self, df, interpretation=True):
        """
        Graphique: Analyse du volume de transactions
        
        Args:
            df: DataFrame Pandas
            interpretation: Si True, génère une interprétation
        """
        if df is None or len(df) == 0:
            print("⚠️  Pas de données pour l'analyse de volume")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        # Graphique 1: Volume total par symbole
        volume_col = 'total_volume' if 'total_volume' in df.columns else 'cumulative_volume'
        if volume_col in df.columns:
            volume_total = df.groupby('symbol')[volume_col].sum().sort_values(ascending=False)
            colors = sns.color_palette("YlOrRd", len(volume_total))
            axes[0].bar(volume_total.index, volume_total.values, color=colors)
            axes[0].set_title('Volume Total par Action', fontsize=14, fontweight='bold')
            axes[0].set_xlabel('Symbole', fontsize=11)
            axes[0].set_ylabel('Volume Total', fontsize=11)
            axes[0].tick_params(axis='x', rotation=45)
            axes[0].grid(True, alpha=0.3, axis='y')
        
        # Graphique 2: Heatmap du volume (si données temporelles disponibles)
        if 'window_start' in df.columns and len(df) > 0:
            pivot_df = df.pivot_table(
                values=volume_col,
                index='symbol',
                columns=df.groupby('symbol').cumcount(),
                fill_value=0
            )
            if len(pivot_df) > 0:
                sns.heatmap(
                    pivot_df,
                    cmap='YlOrRd',
                    annot=False,
                    fmt='d',
                    cbar_kws={'label': 'Volume'},
                    ax=axes[1]
                )
                axes[1].set_title('Heatmap du Volume par Fenêtre', fontsize=14, fontweight='bold')
                axes[1].set_xlabel('Fenêtre Temporelle', fontsize=11)
                axes[1].set_ylabel('Symbole', fontsize=11)
        
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'volume_analysis.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Graphique sauvegardé: {filepath}")
        
        if interpretation:
            self._generate_interpretation(
                filepath,
                "Analyse du Volume",
                "Le volume de transactions indique l'activité du marché. Un volume élevé suggère un fort "
                "intérêt des investisseurs et peut confirmer les tendances de prix. Les variations de volume "
                "peuvent signaler des changements de sentiment du marché."
            )
    
    def plot_trend_analysis(self, df, interpretation=True):
        """
        Graphique: Analyse des tendances (depuis Gold)
        
        Args:
            df: DataFrame Pandas depuis Gold
            interpretation: Si True, génère une interprétation
        """
        if df is None or len(df) == 0 or 'price_trend' not in df.columns:
            print("⚠️  Pas de données de tendance disponibles")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        # Graphique 1: Distribution des tendances
        trend_counts = df['price_trend'].value_counts()
        colors_map = {
            'STRONG_UP': 'darkgreen',
            'UP': 'lightgreen',
            'STABLE': 'gray',
            'DOWN': 'lightcoral',
            'STRONG_DOWN': 'darkred'
        }
        colors = [colors_map.get(t, 'blue') for t in trend_counts.index]
        axes[0].bar(trend_counts.index, trend_counts.values, color=colors)
        axes[0].set_title('Distribution des Tendances de Prix', fontsize=14, fontweight='bold')
        axes[0].set_xlabel('Tendance', fontsize=11)
        axes[0].set_ylabel('Nombre d\'Actions', fontsize=11)
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].grid(True, alpha=0.3, axis='y')
        
        # Graphique 2: Tendances par symbole
        if 'symbol' in df.columns:
            trend_by_symbol = df.groupby(['symbol', 'price_trend']).size().unstack(fill_value=0)
            trend_by_symbol.plot(kind='bar', stacked=True, ax=axes[1], 
                                color=[colors_map.get(c, 'blue') for c in trend_by_symbol.columns])
            axes[1].set_title('Tendances par Action', fontsize=14, fontweight='bold')
            axes[1].set_xlabel('Symbole', fontsize=11)
            axes[1].set_ylabel('Nombre de Fenêtres', fontsize=11)
            axes[1].legend(title='Tendance', bbox_to_anchor=(1.05, 1), loc='upper left')
            axes[1].tick_params(axis='x', rotation=45)
            axes[1].grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'trend_analysis.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Graphique sauvegardé: {filepath}")
        
        if interpretation:
            self._generate_interpretation(
                filepath,
                "Analyse des Tendances",
                "Les tendances de prix indiquent la direction générale du marché. STRONG_UP et STRONG_DOWN "
                "représentent des mouvements significatifs, tandis que STABLE indique une stabilité relative. "
                "Cette analyse aide à identifier les opportunités d'investissement et les risques."
            )
    
    def plot_business_metrics(self, df, interpretation=True):
        """
        Graphique: Métriques business agrégées (depuis Gold)
        
        Args:
            df: DataFrame Pandas depuis Gold
            interpretation: Si True, génère une interprétation
        """
        if df is None or len(df) == 0:
            print("⚠️  Pas de données de métriques business")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Graphique 1: Prix moyen vs Volatilité
        if 'overall_avg_price' in df.columns and 'overall_volatility' in df.columns:
            axes[0, 0].scatter(
                df['overall_avg_price'],
                df['overall_volatility'],
                s=100,
                alpha=0.6,
                c=df.index if 'symbol' not in df.columns else None
            )
            if 'symbol' in df.columns:
                for idx, row in df.iterrows():
                    axes[0, 0].annotate(row['symbol'], (row['overall_avg_price'], row['overall_volatility']))
            axes[0, 0].set_title('Prix Moyen vs Volatilité', fontsize=12, fontweight='bold')
            axes[0, 0].set_xlabel('Prix Moyen ($)', fontsize=10)
            axes[0, 0].set_ylabel('Volatilité', fontsize=10)
            axes[0, 0].grid(True, alpha=0.3)
        
        # Graphique 2: Volume cumulatif
        if 'cumulative_volume' in df.columns:
            volume_sorted = df.sort_values('cumulative_volume', ascending=False)
            axes[0, 1].barh(
                volume_sorted['symbol'] if 'symbol' in df.columns else range(len(volume_sorted)),
                volume_sorted['cumulative_volume'],
                color=sns.color_palette("viridis", len(volume_sorted))
            )
            axes[0, 1].set_title('Volume Cumulatif par Action', fontsize=12, fontweight='bold')
            axes[0, 1].set_xlabel('Volume Cumulatif', fontsize=10)
            axes[0, 1].set_ylabel('Symbole', fontsize=10)
            axes[0, 1].grid(True, alpha=0.3, axis='x')
        
        # Graphique 3: Plage de prix
        if 'absolute_min_price' in df.columns and 'absolute_max_price' in df.columns:
            price_range = df['absolute_max_price'] - df['absolute_min_price']
            axes[1, 0].bar(
                df['symbol'] if 'symbol' in df.columns else range(len(df)),
                price_range,
                color='coral'
            )
            axes[1, 0].set_title('Plage de Prix (Min-Max)', fontsize=12, fontweight='bold')
            axes[1, 0].set_xlabel('Symbole', fontsize=10)
            axes[1, 0].set_ylabel('Plage de Prix ($)', fontsize=10)
            if 'symbol' in df.columns:
                axes[1, 0].tick_params(axis='x', rotation=45)
            axes[1, 0].grid(True, alpha=0.3, axis='y')
        
        # Graphique 4: Catégories de volatilité
        if 'volatility_category' in df.columns:
            vol_cat_counts = df['volatility_category'].value_counts()
            axes[1, 1].pie(
                vol_cat_counts.values,
                labels=vol_cat_counts.index,
                autopct='%1.1f%%',
                startangle=90,
                colors=sns.color_palette("Set2", len(vol_cat_counts))
            )
            axes[1, 1].set_title('Distribution des Catégories de Volatilité', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'business_metrics.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Graphique sauvegardé: {filepath}")
        
        if interpretation:
            self._generate_interpretation(
                filepath,
                "Métriques Business",
                "Ce dashboard présente une vue d'ensemble des métriques business clés. La corrélation "
                "entre prix et volatilité aide à identifier les actions à risque. Le volume cumulatif "
                "indique la liquidité du marché, tandis que la plage de prix montre l'amplitude des variations."
            )
    
    def _generate_interpretation(self, image_path, title, description):
        """
        Génère un fichier texte avec l'interprétation du graphique
        
        Args:
            image_path: Chemin de l'image
            title: Titre du graphique
            description: Description/interprétation
        """
        interpretation_path = image_path.replace('.png', '_interpretation.txt')
        
        with open(interpretation_path, 'w', encoding='utf-8') as f:
            f.write(f"INTERPRÉTATION DU GRAPHIQUE\n")
            f.write(f"{'='*60}\n\n")
            f.write(f"Titre: {title}\n")
            f.write(f"Fichier: {os.path.basename(image_path)}\n")
            f.write(f"Date de génération: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Description:\n")
            f.write(f"{description}\n\n")
            f.write(f"{'='*60}\n")
        
        print(f"📝 Interprétation sauvegardée: {interpretation_path}")
    
    def generate_all_dashboards(self):
        """
        Génère tous les graphiques du dashboard
        """
        print("\n" + "="*70)
        print("📊 GÉNÉRATION COMPLÈTE DU DASHBOARD")
        print("="*70 + "\n")
        
        # Charger les données
        silver_df = self.load_delta_table(DELTA_SILVER_PATH, "Silver")
        gold_df = self.load_delta_table(DELTA_GOLD_PATH, "Gold")
        
        # Générer les graphiques depuis Silver
        if silver_df is not None and len(silver_df) > 0:
            print("\n📈 Génération des graphiques depuis Silver...")
            self.plot_price_evolution(silver_df)
            self.plot_volatility_analysis(silver_df)
            self.plot_volume_analysis(silver_df)
        
        # Générer les graphiques depuis Gold
        if gold_df is not None and len(gold_df) > 0:
            print("\n📈 Génération des graphiques depuis Gold...")
            self.plot_trend_analysis(gold_df)
            self.plot_business_metrics(gold_df)
        
        print(f"\n✅ Tous les graphiques générés dans: {self.output_dir}/")
        print("📝 Les interprétations sont disponibles dans les fichiers *_interpretation.txt")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DashboardGenerator") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    generator = DashboardGenerator(spark)
    generator.generate_all_dashboards()
    
    spark.stop()

