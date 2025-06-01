

# Documentation des modules de transformation

## Aperçu

Ce document décrit les **modules réutilisables** pour les **transformations de données financières** dans le projet **FinanceLake**.  
Ces modules sont situés dans le dossier `utils/` et sont conçus pour être utilisés avec **Spark DataFrames**.

---

## `calculate_ema()`

> Calcule une **moyenne mobile exponentielle (EMA)** sur une série de données.

- **Fichier** : `utils/indicators.py`

### Paramètres

- `df` : Spark DataFrame avec une colonne de dates (`date`) et une colonne de valeurs (ex. `price`).
- `column_name` : Nom de la colonne contenant les valeurs (ex. `price`).
- `span` *(optionnel)* : Période pour l’EMA *(par défaut : 7)*.

### Sortie

- Spark DataFrame avec une **nouvelle colonne `ema`**.

### Exemple

```python
from utils.indicators import calculate_ema
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [(1, "2023-01-01", 100), (2, "2023-01-02", 110), (3, "2023-01-03", 120)]
df = spark.createDataFrame(data, ["id", "date", "price"])

result = calculate_ema(df, "price", span=3)
result.show()

min_max_normalize()

    Normalise les données entre 0 et 1.

    Fichier : utils/normalization.py

Paramètres

    df : Spark DataFrame avec une colonne de valeurs.

    column_name : Nom de la colonne à normaliser.

Sortie

    Spark DataFrame avec une nouvelle colonne normalized.

Exemple

from utils.normalization import min_max_normalize

result = min_max_normalize(df, "price")
result.show()

remove_outliers()

    Supprime les valeurs aberrantes en utilisant la méthode du Z-score.

    Fichier : utils/filters.py

Paramètres

    df : Spark DataFrame avec une colonne de valeurs.

    column_name : Nom de la colonne à filtrer.

    threshold (optionnel) : Seuil du Z-score (par défaut : 3).

Sortie

    Spark DataFrame sans les valeurs aberrantes.

Exemple

from utils.filters import remove_outliers

result = remove_outliers(df, "price", threshold=3)
result.show()

Utilisation

Tous les modules sont importables via :

from utils import calculate_ema, min_max_normalize, remove_outliers


