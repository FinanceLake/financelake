# Documentation of Transformation Modules

## Overview

This document describes the **reusable data transformation modules** used in the **FinanceLake** project.  
These modules are located in the `utils/` folder and are designed to work with **Spark DataFrames**.

---

## `calculate_ema()`

> Computes **Exponential Moving Average (EMA)** over a value column.

- **File**: `utils/indicators.py`

### Parameters

- `df`: Spark DataFrame with a `date` column and a numeric column (e.g. `price`)
- `column_name`: Column containing the values to compute EMA
- `span` *(optional)*: EMA period *(default is 7)*.

### Output

- Spark DataFrame with a **new column `ema`**.

### Example

```python
from utils.indicators import calculate_ema
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [(1, "2023-01-01", 100), (2, "2023-01-02", 110), (3, "2023-01-03", 120)]
df = spark.createDataFrame(data, ["id", "date", "price"])

result = calculate_ema(df, "price", span=3)
result.show() 
```

## `min_max_normalize()`

    Normalizes values between 0 and 1.

    File: utils/normalization.py

### Parameters

    df: Spark DataFrame

    column_name: Column to normalize

### Output

    Spark DataFrame with new column normalized.

### Example

```python
from utils.normalization import min_max_normalize

result = min_max_normalize(df, "price")
result.show()
```

## `remove_outliers()`

    Removes outliers using the Z-score method.

    File: utils/filters.py

### Parameters

    df: Spark DataFrame

    column_name: Column to clean

    threshold (optional): Z-score threshold (default is 3)

### Output

     Spark DataFrame without outliers

### Example

```python
from utils.filters import remove_outliers

result = remove_outliers(df, "price", threshold=3)
result.show()
```
### Usage

You can import all modules like this:

```python
from utils import calculate_ema, min_max_normalize, remove_outliers
```

