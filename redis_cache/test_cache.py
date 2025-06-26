# test_cache.py
from redis_cache import get_stock_data, invalidate_cache, get_cache_stats, get_redis_health
from prometheus_client import start_http_server
import time
import threading
import requests

def dummy_fetch_api(ticker, start_date, end_date):
    """Mock API function for testing."""
    print(f"Fetching data from API for {ticker} {start_date} to {end_date}")
    time.sleep(0.1)  # Simulate API delay
    return {
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "price_data": [100, 101, 102],
        "timestamp": time.time()
    }

def check_metrics_server():
    """Check if metrics server is already running."""
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=2)
        if response.status_code == 200:
            print("✓ Prometheus metrics server already running at http://localhost:8000/metrics")
            return True
    except requests.exceptions.RequestException:
        pass
    return False

def start_metrics_server_if_needed():
    """Start Prometheus metrics server only if not already running."""
    if check_metrics_server():
        return True
    
    try:
        start_http_server(8000)
        print("✓ Started Prometheus metrics server at http://localhost:8000/metrics")
        return True
    except Exception as e:
        print(f"⚠ Failed to start metrics server: {e}")
        print("  (This is normal if redis_cache.py already started it)")
        return False

def test_cache_behavior():
    """Test various cache scenarios."""
    ticker = "AAPL"
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    print("=== Testing Cache Behavior ===")
    
    # Clear cache before starting the test
    invalidate_cache(ticker, start_date, end_date)
    
    # Test 1: First call - expect cache miss and API fetch
    print("\n1. First call (cache miss expected):")
    start_time = time.time()
    data1 = get_stock_data(ticker, start_date, end_date, dummy_fetch_api)
    time1 = time.time() - start_time
    print(f"   Time taken: {time1:.3f}s")
    
    # Test 2: Second call - expect cache hit, no API fetch
    print("\n2. Second call (cache hit expected):")
    start_time = time.time()
    data2 = get_stock_data(ticker, start_date, end_date, dummy_fetch_api)
    time2 = time.time() - start_time
    print(f"   Time taken: {time2:.3f}s")
    print(f"   Speed improvement: {((time1 - time2) / time1 * 100):.1f}%")
    
    # Test 3: Force refresh - expect cache deleted, API fetch again
    print("\n3. Force refresh (cache miss expected):")
    data3 = get_stock_data(ticker, start_date, end_date, dummy_fetch_api, force_refresh=True)
    
    # Test 4: After invalidation - expect cache miss and API fetch again
    print("\n4. Manual invalidation test:")
    invalidate_cache(ticker, start_date, end_date)
    data4 = get_stock_data(ticker, start_date, end_date, dummy_fetch_api)
    
    # Test 5: Test different ticker to show cache isolation
    print("\n5. Different ticker test:")
    data5 = get_stock_data("GOOGL", start_date, end_date, dummy_fetch_api)
    
    # Display cache statistics
    stats = get_cache_stats()
    health = get_redis_health()
    
    print(f"\n=== Cache Statistics ===")
    print(f"Redis cache hits: {stats['redis_cache_hits']}")
    print(f"Memory cache hits: {stats['memory_cache_hits']}")
    print(f"Total cache hits: {stats['total_cache_hits']}")
    print(f"Cache misses: {stats['cache_misses']}")
    print(f"Cache errors: {stats['cache_errors']}")
    print(f"Hit rate: {stats['hit_rate']:.2%}")
    print(f"Total operations: {stats['total_operations']}")
    print(f"Memory cache size: {stats['memory_cache_size']}")
    print(f"Memory cache max size: {stats['memory_cache_maxsize']}")
    
    print(f"\n=== Redis Health ===")
    print(f"Status: {health['status']}")
    if health['status'] == 'healthy':
        print(f"Response time: {health['response_time_ms']}ms")
        print(f"Host: {health['host']}:{health['port']}")
    elif health['status'] == 'unhealthy':
        print(f"Error: {health['error']}")
    else:
        print(f"Message: {health['message']}")
    
    # Additional Redis info if available
    if 'redis_version' in stats:
        print(f"\n=== Redis Info ===")
        print(f"Redis version: {stats['redis_version']}")
        print(f"Used memory: {stats['used_memory']}")
        print(f"Connected clients: {stats['connected_clients']}")

def test_memory_fallback():
    """Test memory cache fallback when Redis is unavailable."""
    print("\n=== Testing Memory Cache Fallback ===")
    
    # This test would require temporarily disabling Redis
    # For now, just demonstrate that memory cache works
    ticker = "MSFT"
    start_date = "2024-02-01"
    end_date = "2024-02-28"
    
    print("Testing with a new ticker to ensure fresh cache miss...")
    data = get_stock_data(ticker, start_date, end_date, dummy_fetch_api)
    
    # Second call should hit cache
    print("Second call should hit cache...")
    data2 = get_stock_data(ticker, start_date, end_date, dummy_fetch_api)

def test_data_validation():
    """Test data validation functionality."""
    print("\n=== Testing Data Validation ===")
    
    def bad_api_response(ticker, start_date, end_date):
        """Mock API that returns invalid data."""
        print(f"Mock API returning invalid data for {ticker}")
        return "invalid_data_not_dict"  # This should fail validation
    
    def empty_api_response(ticker, start_date, end_date):
        """Mock API that returns empty dict."""
        print(f"Mock API returning empty dict for {ticker}")
        return {}  # This should fail validation (no ticker field)
    
    # Test with invalid data
    print("1. Testing with invalid data type:")
    try:
        data = get_stock_data("INVALID", "2024-01-01", "2024-01-31", bad_api_response)
        print("   Data returned (should not be cached)")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test with empty data
    print("2. Testing with empty data:")
    try:
        data = get_stock_data("EMPTY", "2024-01-01", "2024-01-31", empty_api_response)
        print("   Data returned (should not be cached)")
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    print("=== Stock Data Cache Testing Suite ===\n")
    
    # Check if metrics server is needed and start if necessary
    start_metrics_server_if_needed()
    time.sleep(1)  # Give server time to start if needed
    
    # Run cache tests
    test_cache_behavior()
    
    # Test memory fallback
    test_memory_fallback()
    
    # Test data validation
    test_data_validation()
    
    # Final stats
    print("\n=== Final Statistics ===")
    final_stats = get_cache_stats()
    print(f"Total operations: {final_stats['total_operations']}")
    print(f"Overall hit rate: {final_stats['hit_rate']:.2%}")
    
    print("\n=== Test completed. Monitoring metrics... Press Ctrl+C to exit ===")
    
    # Keep program alive for monitoring (only if we started our own server)
    try:
        while True:
            time.sleep(5)
            # Print periodic stats
            stats = get_cache_stats()
            print(f"[PERIODIC] Hits: {stats['total_cache_hits']}, Misses: {stats['cache_misses']}, Hit Rate: {stats['hit_rate']:.2%}")
    except KeyboardInterrupt:
        print("\nShutting down...")