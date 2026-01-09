"""
Visualisation et Extension - Indicateurs Additionnels
Crée des graphiques et des indicateurs supplémentaires
"""

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, sum as spark_sum
import pandas as pd
import os

class StockVisualizer:
    """
    Visualiseur de données boursières avec Matplotlib et Seaborn
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.output_dir = "./screenshots"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Configuration du style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 6)
        
        print("✅ Visualiseur initialisé")
    
    def load_aggregated_data(self, table_name="stock_aggregates"):
        """
        Charge les données agrégées depuis la table en mémoire
        
        Args:
            table_name: Nom de la table
            
        Returns:
            Pandas DataFrame
        """
        try:
            spark_df = self.spark.table(table_name)
            pandas_df = spark_df.toPandas()
            print(f"✅ Données chargées: {len(pandas_df)} enregistrements")
            return pandas_df
        except Exception as e:
            print(f"❌ Erreur de chargement: {e}")
            return None
    
    def plot_price_evolution(self, df):
        """
        Graphique: Évolution du prix moyen par symbole
        
        Args:
            df: Pandas DataFrame
        """
        if df is None or len(df) == 0:
            print("⚠️  Pas de données à visualiser")
            return
        
        plt.figure(figsize=(14, 7))
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].sort_values('window_start')
            plt.plot(
                symbol_data['window_start'],
                symbol_data['avg_price'],
                marker='o',
                label=symbol,
                linewidth=2
            )
        
        plt.title('Évolution du Prix Moyen par Action', fontsize=16, fontweight='bold')
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('Prix Moyen ($)', fontsize=12)
        plt.legend(title='Symbole', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'price_evolution.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"📊 Graphique sauvegardé: {filepath}")
        plt.close()
    
    def plot_volatility_comparison(self, df):
        """
        Graphique: Comparaison de la volatilité entre actions
        
        Args:
            df: Pandas DataFrame
        """
        if df is None or len(df) == 0:
            return
        
        # Calculer la volatilité moyenne par symbole
        volatility_avg = df.groupby('symbol')['volatility'].mean().sort_values(ascending=False)
        
        plt.figure(figsize=(10, 6))
        colors = sns.color_palette("husl", len(volatility_avg))
        bars = plt.bar(volatility_avg.index, volatility_avg.values, color=colors)
        
        # Ajouter les valeurs sur les barres
        for bar in bars:
            height = bar.get_height()
            if height > 0:  # Éviter les valeurs NaN
                plt.text(
                    bar.get_x() + bar.get_width()/2.,
                    height,
                    f'{height:.2f}',
                    ha='center',
                    va='bottom',
                    fontsize=10
                )
        
        plt.title('Volatilité Moyenne par Action', fontsize=16, fontweight='bold')
        plt.xlabel('Symbole', fontsize=12)
        plt.ylabel('Volatilité (Écart-Type)', fontsize=12)
        plt.xticks(rotation=0)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'volatility_comparison.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"📊 Graphique sauvegardé: {filepath}")
        plt.close()
    
    def plot_volume_heatmap(self, df):
        """
        Graphique: Heatmap du volume de transactions
        
        Args:
            df: Pandas DataFrame
        """
        if df is None or len(df) == 0:
            return
        
        # Créer une matrice pivot
        df['window_id'] = df.groupby('symbol').cumcount()
        pivot_df = df.pivot_table(
            values='total_volume',
            index='symbol',
            columns='window_id',
            fill_value=0
        )
        
        plt.figure(figsize=(14, 6))
        sns.heatmap(
            pivot_df,
            cmap='YlOrRd',
            annot=False,
            fmt='d',
            cbar_kws={'label': 'Volume Total'}
        )
        
        plt.title('Heatmap du Volume de Transactions', fontsize=16, fontweight='bold')
        plt.xlabel('Fenêtre Temporelle', fontsize=12)
        plt.ylabel('Symbole', fontsize=12)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'volume_heatmap.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"📊 Graphique sauvegardé: {filepath}")
        plt.close()
    
    def plot_price_change_distribution(self, df):
        """
        Graphique: Distribution des changements de prix
        
        Args:
            df: Pandas DataFrame
        """
        if df is None or len(df) == 0:
            return
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        
        # Histogramme
        axes[0].hist(
            df['avg_price_change'].dropna(),
            bins=30,
            color='steelblue',
            edgecolor='black',
            alpha=0.7
        )
        axes[0].axvline(0, color='red', linestyle='--', linewidth=2, label='Aucun changement')
        axes[0].set_title('Distribution des Changements de Prix', fontsize=14, fontweight='bold')
        axes[0].set_xlabel('Changement de Prix Moyen (%)', fontsize=11)
        axes[0].set_ylabel('Fréquence', fontsize=11)
        axes[0].legend()
        
        # Boxplot par symbole
        df_clean = df.dropna(subset=['avg_price_change'])
        if len(df_clean) > 0:
            sns.boxplot(
                data=df_clean,
                x='symbol',
                y='avg_price_change',
                palette='Set2',
                ax=axes[1]
            )
            axes[1].axhline(0, color='red', linestyle='--', linewidth=2)
            axes[1].set_title('Changement de Prix par Action (Boxplot)', fontsize=14, fontweight='bold')
            axes[1].set_xlabel('Symbole', fontsize=11)
            axes[1].set_ylabel('Changement de Prix Moyen (%)', fontsize=11)
        
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'price_change_distribution.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"📊 Graphique sauvegardé: {filepath}")
        plt.close()
    
    def calculate_rsi(self, df, period=14):
        """
        EXTENSION: Calcul du RSI (Relative Strength Index)
        
        Le RSI est un indicateur technique qui mesure la vitesse et 
        l'amplitude des mouvements de prix
        
        Args:
            df: Pandas DataFrame
            period: Période pour le calcul du RSI
            
        Returns:
            DataFrame avec colonne RSI
        """
        print(f"\n📊 EXTENSION: Calcul du RSI (période={period})")
        
        results = []
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].sort_values('window_start').copy()
            
            if len(symbol_data) < period:
                continue
            
            # Calculer les gains et pertes
            symbol_data['price_diff'] = symbol_data['avg_price'].diff()
            symbol_data['gain'] = symbol_data['price_diff'].apply(lambda x: x if x > 0 else 0)
            symbol_data['loss'] = symbol_data['price_diff'].apply(lambda x: -x if x < 0 else 0)
            
            # Moyenne mobile des gains et pertes
            symbol_data['avg_gain'] = symbol_data['gain'].rolling(window=period).mean()
            symbol_data['avg_loss'] = symbol_data['loss'].rolling(window=period).mean()
            
            # RSI
            rs = symbol_data['avg_gain'] / symbol_data['avg_loss'].replace(0, 1e-10)
            symbol_data['rsi'] = 100 - (100 / (1 + rs))
            
            results.append(symbol_data)
        
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            print(f"✅ RSI calculé pour {len(results)} symboles")
            return combined_df
        else:
            print("⚠️  Pas assez de données pour calculer le RSI")
            return df
    
    def plot_rsi(self, df):
        """
        Graphique: RSI (Relative Strength Index)
        
        Args:
            df: Pandas DataFrame avec colonne RSI
        """
        if 'rsi' not in df.columns or df['rsi'].isna().all():
            print("⚠️  RSI non disponible")
            return
        
        plt.figure(figsize=(14, 7))
        
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].dropna(subset=['rsi']).sort_values('window_start')
            if len(symbol_data) > 0:
                plt.plot(
                    symbol_data['window_start'],
                    symbol_data['rsi'],
                    marker='o',
                    label=symbol,
                    linewidth=2
                )
        
        # Lignes de référence
        plt.axhline(70, color='red', linestyle='--', linewidth=1, label='Suracheté (70)')
        plt.axhline(30, color='green', linestyle='--', linewidth=1, label='Survendu (30)')
        plt.axhline(50, color='gray', linestyle='-', linewidth=0.5, alpha=0.5)
        
        plt.title('RSI (Relative Strength Index) par Action', fontsize=16, fontweight='bold')
        plt.xlabel('Temps', fontsize=12)
        plt.ylabel('RSI', fontsize=12)
        plt.ylim(0, 100)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        filepath = os.path.join(self.output_dir, 'rsi_indicator.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"📊 Graphique sauvegardé: {filepath}")
        plt.close()
    
    def generate_all_visualizations(self):
        """
        Génère toutes les visualisations
        """
        print("\n" + "="*60)
        print("📊 GÉNÉRATION DES VISUALISATIONS")
        print("="*60 + "\n")
        
        # Charger les données
        df = self.load_aggregated_data()
        
        if df is None or len(df) == 0:
            print("❌ Aucune donnée disponible pour la visualisation")
            return
        
        # Générer les graphiques
        self.plot_price_evolution(df)
        self.plot_volatility_comparison(df)
        self.plot_volume_heatmap(df)
        self.plot_price_change_distribution(df)
        
        # Extension: RSI
        df_with_rsi = self.calculate_rsi(df)
        self.plot_rsi(df_with_rsi)
        
        print(f"\n✅ Toutes les visualisations générées dans {self.output_dir}/")

if __name__ == "__main__":
    # Ce module doit être utilisé avec un pipeline actif
    print("⚠️  Ce module doit être utilisé avec un pipeline Spark actif")
    print("    Utilisez main.py pour exécuter le pipeline complet.")

