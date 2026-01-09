# Mini-Rapport: Comment Spark Combine Batch, Streaming et ML

## Introduction

Apache Spark est une plateforme unifiée de traitement de données qui intègre de manière transparente le traitement batch, le streaming en temps réel et l'apprentissage automatique. Cette intégration repose sur une architecture cohérente centrée autour des DataFrames et d'un moteur d'exécution optimisé.

## Architecture Unifiée

### 1. Fondation Commune: Les DataFrames

Spark utilise les DataFrames comme abstraction unifiée pour tous les types de traitement. Que ce soit pour le batch (Spark SQL), le streaming (Structured Streaming) ou le machine learning (MLlib), l'API reste cohérente. Cette uniformité permet aux développeurs d'appliquer les mêmes opérations de transformation (select, filter, groupBy, etc.) quel que soit le contexte d'exécution.

### 2. Catalyst Optimizer: Optimisation Transparente

Le Catalyst Optimizer analyse et optimise les plans d'exécution pour toutes les requêtes Spark, qu'elles soient en batch ou streaming. Il applique des optimisations comme le predicate pushdown, la projection pruning et la fusion d'opérations. Cela garantit des performances élevées sans nécessiter de modifications manuelles du code.

### 3. Tungsten Execution Engine

Le moteur d'exécution Tungsten assure l'efficacité au niveau bas avec la gestion de la mémoire hors-tas, la génération de code et la vectorisation des opérations. Cette couche garantit des performances optimales pour le batch, le streaming et les algorithmes ML.

## Intégration Pratique

### Streaming + Batch

Dans notre pipeline, Spark Structured Streaming lit des flux JSON en temps réel et applique des agrégations fenêtrées. Simultanément, ces données sont persistées en Parquet pour analyse batch ultérieure. La même logique de transformation s'applique aux deux modes grâce à l'API unifiée.

### Streaming + SQL

Les données en streaming peuvent être interrogées avec Spark SQL en créant des vues temporaires. Le code SQL reste identique qu'il s'agisse de données statiques ou en temps réel. Le cache() améliore les performances des requêtes répétées.

### Batch + MLlib

Pour l'entraînement de modèles, nous chargeons les données Parquet (générées par le streaming) et appliquons des transformations ML avec VectorAssembler et StandardScaler. Les modèles entraînés (Logistic Regression, Random Forest) peuvent ensuite être appliqués sur des flux en temps réel.

## Avantages de cette Unification

1. **Réduction de la complexité**: Un seul framework pour tous les besoins
2. **Réutilisabilité du code**: Les transformations sont portables entre batch et streaming
3. **Performances optimisées**: Le Catalyst Optimizer s'applique partout
4. **Évolutivité**: Le même code s'exécute localement ou sur un cluster

## Conclusion

L'architecture unifiée de Spark réduit considérablement la complexité opérationnelle tout en offrant des performances élevées. En combinant batch, streaming et ML dans un seul écosystème cohérent, Spark permet de construire des pipelines de données complets et maintenables, comme démontré dans notre projet Real-Time Stock Insight.


