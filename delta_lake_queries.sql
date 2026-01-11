-- ============================================
-- REQUÊTES SQL POUR FINANCELAKE
-- À exécuter dans spark-sql ou notebook PySpark
-- ============================================

-- Créer des vues temporaires pour requêter facilement
CREATE OR REPLACE TEMPORARY VIEW trades_silver
USING delta
LOCATION 'hdfs://localhost:9000/delta/silver/trades_clean';

CREATE OR REPLACE TEMPORARY VIEW transactions_silver
USING delta
LOCATION 'hdfs://localhost:9000/delta/silver/transactions_clean';

CREATE OR REPLACE TEMPORARY VIEW trading_volume_gold
USING delta
LOCATION 'hdfs://localhost:9000/delta/gold/trading_volume';

CREATE OR REPLACE TEMPORARY VIEW fraud_scores_gold
USING delta
LOCATION 'hdfs://localhost:9000/delta/gold/fraud_scores';

-- ============================================
-- ANALYSES TRADING
-- ============================================

-- Top 5 des symboles les plus tradés
SELECT 
    symbol,
    COUNT(*) as num_trades,
    ROUND(SUM(notional), 2) as total_volume_usd,
    ROUND(AVG(price), 2) as avg_price
FROM trades_silver
GROUP BY symbol
ORDER BY total_volume_usd DESC
LIMIT 5;

-- Volume horaire du jour
SELECT 
    trade_hour,
    COUNT(*) as trades,
    ROUND(SUM(notional), 2) as volume
FROM trades_silver
WHERE trade_date = CURRENT_DATE()
GROUP BY trade_hour
ORDER BY trade_hour;

-- Répartition BUY vs SELL
SELECT 
    side,
    symbol,
    COUNT(*) as count,
    ROUND(SUM(notional), 2) as total_volume
FROM trades_silver
GROUP BY side, symbol
ORDER BY total_volume DESC;

-- ============================================
-- DÉTECTION DE FRAUDE
-- ============================================

-- Top 10 transactions suspectes
SELECT 
    transaction_id,
    client_id,
    amount,
    type,
    risk_score,
    risk_level,
    is_fraud
FROM fraud_scores_gold
WHERE risk_level = 'HIGH'
ORDER BY risk_score DESC
LIMIT 10;

-- Taux de fraude par type de transaction
SELECT 
    type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as fraud_rate_pct
FROM transactions_silver
GROUP BY type
ORDER BY fraud_rate_pct DESC;

-- Clients à risque (3+ transactions high risk)
SELECT 
    client_id,
    COUNT(*) as high_risk_transactions,
    ROUND(AVG(risk_score), 2) as avg_risk_score,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as confirmed_frauds
FROM fraud_scores_gold
WHERE risk_level = 'HIGH'
GROUP BY client_id
HAVING COUNT(*) >= 3
ORDER BY confirmed_frauds DESC, high_risk_transactions DESC;

-- ============================================
-- TIME TRAVEL
-- ============================================

-- Comparer les versions de Silver trades
SELECT 
    'Version actuelle' as version,
    COUNT(*) as row_count,
    ROUND(SUM(notional), 2) as total_volume
FROM trades_silver

UNION ALL

SELECT 
    'Version 0' as version,
    COUNT(*) as row_count,
    ROUND(SUM(notional), 2) as total_volume
FROM delta.`hdfs://localhost:9000/delta/silver/trades_clean@v0`;

-- Voir l'historique complet d'une table
DESCRIBE HISTORY delta.`hdfs://localhost:9000/delta/silver/trades_clean`;

-- ============================================
-- MÉTRIQUES MÉTIER (KPIs)
-- ============================================

-- Dashboard résumé
SELECT 
    CURRENT_DATE() as report_date,
    (SELECT COUNT(*) FROM trades_silver) as total_trades,
    (SELECT COUNT(DISTINCT symbol) FROM trades_silver) as unique_symbols,
    (SELECT ROUND(SUM(notional), 2) FROM trades_silver) as total_volume_usd,
    (SELECT COUNT(*) FROM transactions_silver WHERE is_fraud = 1) as total_frauds,
    (SELECT COUNT(*) FROM fraud_scores_gold WHERE risk_level = 'HIGH') as high_risk_count;

-- Performance du modèle de fraude (matrice de confusion simplifiée)
SELECT 
    CASE 
        WHEN risk_level = 'HIGH' AND is_fraud = 1 THEN 'True Positive'
        WHEN risk_level = 'HIGH' AND is_fraud = 0 THEN 'False Positive'
        WHEN risk_level != 'HIGH' AND is_fraud = 1 THEN 'False Negative'
        ELSE 'True Negative'
    END as prediction_category,
    COUNT(*) as count
FROM fraud_scores_gold
GROUP BY prediction_category;

-- Volume par catégorie
SELECT 
    volume_category,
    COUNT(*) as num_hours,
    ROUND(AVG(total_volume_usd), 2) as avg_hourly_volume,
    ROUND(MAX(total_volume_usd), 2) as max_hourly_volume
FROM trading_volume_gold
GROUP BY volume_category
ORDER BY 
    CASE volume_category
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
    END;

-- ============================================
-- OPTIMISATION
-- ============================================

-- Statistiques de fichiers Delta (à exécuter en Python)
/*
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "hdfs://localhost:9000/delta/silver/trades_clean")

# Voir les fichiers
delta_table.detail().select("numFiles", "sizeInBytes").show()

# Optimiser (compaction)
delta_table.optimize().executeCompaction()

# Z-Ordering pour améliorer les jointures
delta_table.optimize().executeZOrderBy("symbol", "client_id")

# Vacuum (supprimer anciennes versions > 7 jours)
delta_table.vacuum(168)  # 168 heures = 7 jours
*/
