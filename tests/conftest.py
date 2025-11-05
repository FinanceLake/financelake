import pytest
import requests
from unittest.mock import MagicMock

@pytest.fixture
def mock_requests_get(monkeypatch):
    """Fixture pour simuler les requêtes HTTP"""
    mock = MagicMock()
    mock.return_value.status_code = 200
    mock.return_value.json.return_value = {
        "chart": {
            "result": [{
                "meta": {"symbol": "AAPL"},
                "timestamp": [1620000000],
                "indicators": {
                    "quote": [{
                        "open": [150.0],
                        "close": [151.0],
                        "high": [152.0],
                        "low": [149.0],
                        "volume": [1000000]
                    }]
                }
            }]
        }
    }
    monkeypatch.setattr(requests, "get", mock)
    return mock

@pytest.fixture
def mock_yfinance_ticker(monkeypatch):
    """Fixture pour simuler yfinance.Ticker"""
    import pandas as pd
    import yfinance as yf
    
    mock_ticker = MagicMock()
    mock_data = pd.DataFrame({
        'Open': [150.0],
        'Close': [151.0],
        'High': [152.0],
        'Low': [149.0],
        'Volume': [1000000]
    })
    mock_ticker.return_value.history.return_value = mock_data
    
    monkeypatch.setattr(yf, "Ticker", mock_ticker)
    return mock_ticker