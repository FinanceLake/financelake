import pytest
import pandas as pd
from datetime import datetime
from app.stock_dashboard import StockDashboard
from app.data_fetcher import fetch_stock_data
from unittest.mock import patch
import os

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'Date': pd.date_range(start='2024-01-01', end='2024-01-10', freq='D'),
        'Close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        'Volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
    })

@pytest.fixture
def dashboard():
    return StockDashboard()

def test_fetch_stock_data(dashboard, monkeypatch):
    # Mock environment variable
    monkeypatch.setenv('API_BASE_URL', 'http://backend:8000/api/v1')
    
    with patch('app.data_fetcher.requests.get') as mock_get:
        mock_get.return_value.json.return_value = [
            {'date': '2024-01-01', 'close': 100, 'volume': 1000},
            {'date': '2024-01-02', 'close': 101, 'volume': 1100}
        ]
        mock_get.return_value.raise_for_status.return_value = None
        
        df = dashboard._fetch_stock_data('AAPL', '2024-01-01', '2024-01-02')
        assert df is not None
        assert not df.empty
        assert list(df.columns) == ['Date', 'Close', 'Volume']
        assert len(df) == 2
        assert df['Date'].dtype == 'datetime64[ns]'

def test_calculate_indicators(dashboard, sample_data):
    df = dashboard._calculate_indicators(sample_data)
    assert 'MA50' in df.columns
    assert 'MA200' in df.columns
    assert 'Daily_Return' in df.columns
    assert 'Volatility' in df.columns
    assert round(df['Daily_Return'].iloc[1], 2) == 1.0  # (101-100)/100 * 100
    assert df['Volatility'].iloc[-1] > 0

def test_create_price_chart(dashboard, sample_data):
    fig = dashboard._create_price_chart(sample_data, 'AAPL', ['ma50', 'ma200', 'volatility'])
    assert len(fig.data) >= 3  # Close, MA50, MA200, Volatility (if all present)
    assert fig.layout.yaxis2.title.text == 'Volatility (%)'

def test_create_stats_table(dashboard, sample_data):
    table = dashboard._create_stats_table(sample_data, 'AAPL')
    assert table is not None
    assert 'dash_table.DataTable' in str(type(table))

def test_error_handling(dashboard):
    alert = dashboard._create_error_alert("Test error")
    assert "Test error" in str(alert)
    assert "alert-danger" in str(alert)