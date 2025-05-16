import pytest
import requests

# Define fixtures for mocked responses
@pytest.fixture
def mock_yahoo_response():
    """Fixture that returns a sample Yahoo Finance API response"""
    return {
        "chart": {
            "result": [{
                "meta": {
                    "currency": "USD",
                    "symbol": "AAPL",
                    "exchangeName": "NMS",
                    "instrumentType": "EQUITY",
                    "regularMarketPrice": 172.62
                },
                "timestamp": [1683561600, 1683648000, 1683734400],
                "indicators": {
                    "quote": [{
                        "open": [173.16, 173.05, 173.52],
                        "high": [173.95, 174.03, 174.23],
                        "low": [172.31, 171.91, 172.11],
                        "close": [172.97, 173.75, 172.69],
                        "volume": [42790100, 45498700, 49219300]
                    }],
                    "adjclose": [{
                        "adjclose": [172.97, 173.75, 172.69]
                    }]
                }
            }],
            "error": None
        }
    }


@pytest.fixture
def mock_session():
    """Fixture that provides a mock requests session for testing"""
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            
        def json(self):
            return self.json_data
    
    class MockSession:
        def __init__(self, mock_response, status_code=200):
            self.mock_response = MockResponse(mock_response, status_code)
            
        def get(self, url, params=None, headers=None):
            return self.mock_response
    
    return MockSession