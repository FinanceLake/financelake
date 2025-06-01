import pandas as pd

def calculate_ema(series: pd.Series, span: int = 14) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


