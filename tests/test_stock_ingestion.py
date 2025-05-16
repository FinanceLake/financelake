import pytest
import pandas as pd
import yfinance as yf
import requests
from unittest.mock import patch, MagicMock

# Helper functions that would be in your main code
def fetch_stock_data_with_requests(ticker, period="1d"):
    """Fetch stock data using the requests library"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "range": period,
        "interval": "1d",
    }
    response = requests.get(url, params=params)
    return response


def fetch_stock_data_with_yfinance(ticker, period="1d"):
    """Fetch stock data using the yfinance library"""
    ticker_obj = yf.Ticker(ticker)
    data = ticker_obj.history(period=period)
    return data


class TestStockIngestion:
    """Test suite for stock data ingestion"""

    def test_requests_stock_data_fetching(self):
        """Test fetching stock data using requests"""
        ticker = "AAPL"
        
        # Make the actual API call (for development/debugging)
        # Comment this out when running in CI
        # real_response = fetch_stock_data_with_requests(ticker)
        # assert real_response.status_code == 200
        
        # For CI, use mocked responses
        with patch('requests.get') as mock_get:
            # Configure the mock
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "chart": {
                    "result": [{
                        "meta": {"symbol": "AAPL"},
                        "timestamp": [1620000000, 1620086400],
                        "indicators": {
                            "quote": [{
                                "open": [130.85, 131.25],
                                "close": [131.15, 130.95],
                                "high": [131.45, 131.65],
                                "low": [130.25, 130.05],
                                "volume": [8765400, 9876500]
                            }]
                        }
                    }]
                }
            }
            mock_get.return_value = mock_response
            
            # Call the function with the mock
            response = fetch_stock_data_with_requests(ticker)
            
            # Assertions
            assert response.status_code == 200
            data = response.json()
            
            # Validate data structure and content
            assert "chart" in data
            assert "result" in data["chart"]
            assert len(data["chart"]["result"]) > 0
            
            result = data["chart"]["result"][0]
            assert "indicators" in result
            assert "quote" in result["indicators"]
            assert len(result["indicators"]["quote"]) > 0
            
            quote = result["indicators"]["quote"][0]
            expected_fields = ["open", "close", "high", "low", "volume"]
            for field in expected_fields:
                assert field in quote
                assert len(quote[field]) > 0

    def test_yfinance_stock_data_fetching(self):
        """Test fetching stock data using yfinance"""
        ticker = "AAPL"
        
        # For CI, use mocked objects
        with patch('yfinance.Ticker') as mock_ticker_class:
            # Configure the mock
            mock_ticker = MagicMock()
            mock_data = pd.DataFrame({
                'Open': [130.85, 131.25],
                'High': [131.45, 131.65],
                'Low': [130.25, 130.05],
                'Close': [131.15, 130.95],
                'Volume': [8765400, 9876500]
            }, index=pd.DatetimeIndex(['2025-05-03', '2025-05-04']))
            
            mock_ticker.history.return_value = mock_data
            mock_ticker_class.return_value = mock_ticker
            
            # Call the function with the mock
            data = fetch_stock_data_with_yfinance(ticker)
            
            # Assertions
            assert not data.empty
            expected_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in expected_columns:
                assert col in data.columns
            
            # Validate data types
            assert all(isinstance(val, (float, int)) for val in data['Open'])
            assert all(isinstance(val, (float, int)) for val in data['Close'])
            assert all(isinstance(val, (int, float)) for val in data['Volume'])


@pytest.fixture
def sample_stock_data():
    """Fixture providing sample stock data for testing"""
    return pd.DataFrame({
        'Open': [130.85, 131.25],
        'High': [131.45, 131.65],
        'Low': [130.25, 130.05],
        'Close': [131.15, 130.95],
        'Volume': [8765400, 9876500]
    }, index=pd.DatetimeIndex(['2025-05-03', '2025-05-04']))


class TestDataProcessing:
    """Additional tests for data processing functions"""
    
    def test_data_validity(self, sample_stock_data):
        """Test that sample data meets our quality standards"""
        data = sample_stock_data
        
        # Check that data is properly formatted
        assert not data.empty
        assert all(col in data.columns for col in ['Open', 'High', 'Low', 'Close', 'Volume'])
        
        # Check for missing values
        assert not data.isnull().any().any()
        
        # Basic sanity checks on data values
        assert all(data['High'] >= data['Low'])  # High should be >= Low
        assert all(data['Volume'] >= 0)  # Volume should be non-negative