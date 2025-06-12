import pytest
import time
from unittest.mock import patch, MagicMock
from src.cache.cache_manager import CacheManager
from src.services.data_service import StockDataService

class TestCacheManager:
    
    def test_memory_cache_init(self):
        """Test memory cache initialization"""
        cache = CacheManager(cache_type="memory", ttl=3600)
        assert cache.cache_type == "memory"
        assert cache.ttl == 3600
    
    def test_cache_key_generation(self):
        """Test cache key generation"""
        cache = CacheManager(cache_type="memory")
        key1 = cache._generate_key("AAPL", "2023-01-01", "2023-01-31")
        key2 = cache._generate_key("AAPL", "2023-01-01", "2023-01-31")
        key3 = cache._generate_key("GOOGL", "2023-01-01", "2023-01-31")
        
        assert key1 == key2  # Same inputs should generate same key
        assert key1 != key3  # Different inputs should generate different keys
    
    def test_memory_cache_operations(self):
        """Test memory cache set/get operations"""
        cache = CacheManager(cache_type="memory", ttl=1)
        
        test_data = {"price": 150.0, "volume": 1000}
        
        # Test cache miss
        result = cache.get("AAPL", "2023-01-01", "2023-01-31")
        assert result is None
        assert cache.stats["misses"] == 1
        
        # Test cache set and hit
        cache.set("AAPL", "2023-01-01", "2023-01-31", test_data)
        result = cache.get("AAPL", "2023-01-01", "2023-01-31")
        
        assert result == test_data
        assert cache.stats["hits"] == 1
    
    def test_cache_ttl(self):
        """Test cache TTL expiration"""
        cache = CacheManager(cache_type="memory", ttl=1)
        
        test_data = {"price": 150.0}
        cache.set("AAPL", "2023-01-01", "2023-01-31", test_data)
        
        # Should be available immediately
        result = cache.get("AAPL", "2023-01-01", "2023-01-31")
        assert result == test_data
        
        # Wait for TTL expiration
        time.sleep(2)
        result = cache.get("AAPL", "2023-01-01", "2023-01-31")
        assert result is None
    
    def test_cache_invalidation(self):
        """Test cache invalidation for specific ticker"""
        cache = CacheManager(cache_type="memory")
        
        # Set data for multiple tickers
        cache.set("AAPL", "2023-01-01", "2023-01-31", {"price": 150})
        cache.set("GOOGL", "2023-01-01", "2023-01-31", {"price": 2800})
        
        # Invalidate AAPL cache
        cache.invalidate_ticker("AAPL")
        
        # AAPL should be gone, GOOGL should remain
        assert cache.get("AAPL", "2023-01-01", "2023-01-31") is None
        assert cache.get("GOOGL", "2023-01-01", "2023-01-31") is not None

class TestStockDataService:
    
    @patch('yfinance.Ticker')
    def test_stock_data_service_cache_miss(self, mock_ticker):
        """Test stock data service on cache miss"""
        # Mock yfinance response
        mock_hist = MagicMock()
        mock_hist.empty = False
        mock_hist.to_dict.return_value = [{"Open": 150, "Close": 155}]
        mock_hist.columns = ["Open", "High", "Low", "Close", "Volume"]
        
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_hist
        mock_ticker.return_value = mock_ticker_instance
        
        service = StockDataService()
        result = service.get_stock_data("AAPL", "2023-01-01", "2023-01-31")
        
        assert result is not None
        assert result["ticker"] == "AAPL"
        assert "data" in result
        assert "fetched_at" in result
    
    @patch('yfinance.Ticker')
    def test_stock_data_service_cache_hit(self, mock_ticker):
        """Test stock data service on cache hit"""
        service = StockDataService()
        
        # First call - cache miss
        mock_hist = MagicMock()
        mock_hist.empty = False
        mock_hist.to_dict.return_value = [{"Open": 150, "Close": 155}]
        mock_hist.columns = ["Open", "High", "Low", "Close", "Volume"]
        
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_hist
        mock_ticker.return_value = mock_ticker_instance
        
        result1 = service.get_stock_data("AAPL", "2023-01-01", "2023-01-31")
        
        # Second call - should be cache hit
        result2 = service.get_stock_data("AAPL", "2023-01-01", "2023-01-31")
        
        assert result1 == result2
        # Ticker should only be called once due to caching
        mock_ticker.assert_called_once()

class TestCacheMonitoring:
    
    def test_cache_stats(self):
        """Test cache statistics tracking"""
        cache = CacheManager(cache_type="memory")
        
        # Initial stats
        stats = cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate"] == 0
        
        # Generate some cache activity
        cache.get("AAPL", "2023-01-01", "2023-01-31")  # Miss
        cache.set("AAPL", "2023-01-01", "2023-01-31", {"price": 150})
        cache.get("AAPL", "2023-01-01", "2023-01-31")  # Hit
        
        stats = cache.get_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 50.0

if __name__ == "__main__":
    pytest.main([__file__])