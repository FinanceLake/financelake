import sys
import os

# Ajoute le dossier parent au chemin Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import pandas as pd
from stock_ingestion import fetch_stock_data

def test_fetch_stock_data_mocked(mocker):
    # Mock DataFrame qui ressemble à ce que retourne yfinance
    mock_df = pd.DataFrame({
        "Open": [150.0],
        "Close": [155.0],
        "Volume": [1000000],
    }, index=[pd.Timestamp("2025-05-18")])

    # Patch la classe Ticker dans le module stock_ingestion
    mock_ticker = mocker.patch("stock_ingestion.yf.Ticker")
    instance = mock_ticker.return_value
    instance.history.return_value = mock_df

    data = fetch_stock_data("AAPL")

    # Vérifications
    assert not data.empty
    for col in ["Open", "Close", "Volume"]:
        assert col in data.columns
