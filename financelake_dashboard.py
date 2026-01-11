import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
import pandas as pd

# Configuration Spark
spark = (
    SparkSession.builder
    .appName("FinanceLake-Dashboard")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)

# Chemins Gold
gold_trading_volume = "hdfs://localhost:9000/delta/gold/trading_volume"
gold_fraud_scores = "hdfs://localhost:9000/delta/gold/fraud_scores"
silver_trades = "hdfs://localhost:9000/delta/silver/trades_clean"
silver_transactions = "hdfs://localhost:9000/delta/silver/transactions_clean"

# Style
sns.set_style("darkgrid")
plt.rcParams['figure.figsize'] = (16, 10)

# ============================================
# GRAPHIQUE 1 : Volume de trading par symbole
# ============================================
def plot_trading_volume():
    print("📊 Génération : Volume de trading par symbole...")
    
    df = spark.read.format("delta").load(gold_trading_volume) \
        .groupBy("symbol") \
        .agg({"total_volume_usd": "sum", "num_trades": "sum"}) \
        .orderBy("sum(total_volume_usd)", ascending=False) \
        .limit(10) \
        .toPandas()
    
    plt.subplot(2, 3, 1)
    plt.bar(df['symbol'], df['sum(total_volume_usd)'] / 1e6, color='steelblue')
    plt.xlabel('Symbole')
    plt.ylabel('Volume (millions USD)')
    plt.title('Top 10 Symboles par Volume')
    plt.xticks(rotation=45)

# ============================================
# GRAPHIQUE 2 : Distribution des prix
# ============================================
def plot_price_distribution():
    print("📊 Génération : Distribution des prix...")
    
    df = spark.read.format("delta").load(silver_trades) \
        .select("symbol", "price") \
        .toPandas()
    
    plt.subplot(2, 3, 2)
    for symbol in df['symbol'].unique()[:5]:
        data = df[df['symbol'] == symbol]['price']
        plt.hist(data, alpha=0.5, label=symbol, bins=20)
    
    plt.xlabel('Prix (USD)')
    plt.ylabel('Fréquence')
    plt.title('Distribution des Prix par Symbole')
    plt.legend()

# ============================================
# GRAPHIQUE 3 : Volume horaire
# ============================================
def plot_hourly_volume():
    print("📊 Génération : Volume horaire...")
    
    df = spark.read.format("delta").load(gold_trading_volume) \
        .groupBy("trade_hour") \
        .agg({"total_volume_usd": "sum"}) \
        .orderBy("trade_hour") \
        .toPandas()
    
    plt.subplot(2, 3, 3)
    plt.plot(df['trade_hour'], df['sum(total_volume_usd)'] / 1e6, 
             marker='o', linewidth=2, color='darkgreen')
    plt.xlabel('Heure')
    plt.ylabel('Volume (millions USD)')
    plt.title('Volume de Trading par Heure')
    plt.grid(True, alpha=0.3)

# ============================================
# GRAPHIQUE 4 : Détection de fraude
# ============================================
def plot_fraud_detection():
    print("📊 Génération : Analyse des fraudes...")
    
    df = spark.read.format("delta").load(gold_fraud_scores).toPandas()
    
    plt.subplot(2, 3, 4)
    
    # Matrice de confusion simplifiée
    confusion = df.groupby(['risk_level', 'is_fraud']).size().unstack(fill_value=0)
    sns.heatmap(confusion, annot=True, fmt='d', cmap='Reds', cbar=False)
    plt.xlabel('Fraude Réelle')
    plt.ylabel('Niveau de Risque Prédit')
    plt.title('Matrice de Confusion - Détection Fraude')

# ============================================
# GRAPHIQUE 5 : Distribution du risque
# ============================================
def plot_risk_distribution():
    print("📊 Génération : Distribution du risque...")
    
    df = spark.read.format("delta").load(gold_fraud_scores).toPandas()
    
    plt.subplot(2, 3, 5)
    
    risk_counts = df['risk_level'].value_counts()
    colors = {'HIGH': 'red', 'MEDIUM': 'orange', 'LOW': 'green'}
    
    plt.pie(risk_counts.values, 
            labels=risk_counts.index, 
            autopct='%1.1f%%',
            colors=[colors.get(x, 'gray') for x in risk_counts.index],
            startangle=90)
    plt.title('Répartition des Niveaux de Risque')

# ============================================
# GRAPHIQUE 6 : Montants vs Risque
# ============================================
def plot_amount_vs_risk():
    print("📊 Génération : Montants vs Score de risque...")
    
    df = spark.read.format("delta").load(gold_fraud_scores) \
        .sample(0.1) \
        .toPandas()  # Échantillon pour performance
    
    plt.subplot(2, 3, 6)
    
    colors = {'HIGH': 'red', 'MEDIUM': 'orange', 'LOW': 'green'}
    
    for risk_level in df['risk_level'].unique():
        subset = df[df['risk_level'] == risk_level]
        plt.scatter(subset['amount'], subset['risk_score'], 
                   label=risk_level, alpha=0.5, 
                   color=colors.get(risk_level, 'gray'))
    
    plt.xlabel('Montant Transaction (EUR)')
    plt.ylabel('Score de Risque')
    plt.title('Montant vs Score de Risque')
    plt.legend()
    plt.xscale('log')

# ============================================
# GÉNÉRATION DU DASHBOARD COMPLET
# ============================================
def generate_dashboard():
    print("\n" + "=" * 80)
    print("📊 GÉNÉRATION DU DASHBOARD FINANCELAKE")
    print("=" * 80 + "\n")
    
    plt.figure(figsize=(18, 10))
    
    try:
        plot_trading_volume()
        plot_price_distribution()
        plot_hourly_volume()
        plot_fraud_detection()
        plot_risk_distribution()
        plot_amount_vs_risk()
        
        plt.tight_layout()
        plt.savefig('financelake_dashboard.png', dpi=300, bbox_inches='tight')
        print("\n✅ Dashboard sauvegardé : financelake_dashboard.png")
        
        plt.show()
        
    except Exception as e:
        print(f"❌ Erreur génération dashboard : {e}")
    
    finally:
        spark.stop()

# ============================================
# MÉTRIQUES TEXTUELLES
# ============================================
def print_kpi_summary():
    print("\n" + "=" * 80)
    print("📈 RÉSUMÉ DES KPIs")
    print("=" * 80)
    
    # Total trades
    total_trades = spark.read.format("delta").load(silver_trades).count()
    print(f"✓ Total Trades : {total_trades:,}")
    
    # Volume total
    total_volume = spark.read.format("delta").load(silver_trades) \
        .selectExpr("sum(notional) as total").collect()[0]['total']
    print(f"✓ Volume Total : ${total_volume:,.2f} USD")
    
    # Symboles uniques
    unique_symbols = spark.read.format("delta").load(silver_trades) \
        .select("symbol").distinct().count()
    print(f"✓ Symboles Tradés : {unique_symbols}")
    
    # Transactions bancaires
    total_transactions = spark.read.format("delta").load(silver_transactions).count()
    print(f"✓ Total Transactions : {total_transactions:,}")
    
    # Fraudes détectées
    fraud_count = spark.read.format("delta").load(gold_fraud_scores) \
        .filter("is_fraud = 1").count()
    fraud_rate = (fraud_count / total_transactions * 100) if total_transactions > 0 else 0
    print(f"✓ Fraudes Détectées : {fraud_count} ({fraud_rate:.2f}%)")
    
    # High risk
    high_risk = spark.read.format("delta").load(gold_fraud_scores) \
        .filter("risk_level = 'HIGH'").count()
    print(f"✓ Transactions High Risk : {high_risk}")
    
    print("=" * 80 + "\n")

if __name__ == "__main__":
    print_kpi_summary()
    generate_dashboard()
