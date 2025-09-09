# redis_cache.py
import os
import json
import redis
import time
import threading
from typing import Callable, Any
from cachetools import TTLCache
from prometheus_client import Counter, Histogram, start_http_server
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 86400))  # Default: 24 hours

# Prometheus metrics
CACHE_HIT_COUNTER = Counter("cache_hits_total", "Total cache hits", ["cache_type"])
CACHE_MISS_COUNTER = Counter("cache_misses_total", "Total cache misses")
CACHE_ERROR_COUNTER = Counter("cache_errors_total", "Total cache errors")
CACHE_LOOKUP_DURATION = Histogram("cache_lookup_duration_seconds", "Time spent handling cache lookups")

# Global variables
redis_client = None
cache_enabled = False

# In-memory cache fallback
memory_cache = TTLCache(maxsize=1000, ttl=CACHE_TTL_SECONDS)
memory_cache_lock = threading.Lock()

def start_metrics_server(port=8000):
    # Start Prometheus metrics server
    try:
        start_http_server(port, addr="0.0.0.0")
        print(f"Prometheus metrics server started at http://localhost:{port}/metrics")
    except Exception as e:
        logger.error("Failed to start metrics server: %s", e)

def get_from_memory_cache(cache_key):
    """Get data from in-memory cache."""
    with memory_cache_lock:
        return memory_cache.get(cache_key)

def set_memory_cache(cache_key, data):
    """Set data in in-memory cache."""
    with memory_cache_lock:
        memory_cache[cache_key] = data

def validate_stock_data(data):
    """Validate stock data structure before caching."""
    if not isinstance(data, dict):
        return False
    
    # Add your required fields here based on your stock data structure
    required_fields = ['ticker']  # Minimum required field
    optional_fields = ['start_date', 'end_date', 'price_data', 'timestamp']
    
    # Check if at least the required fields are present
    has_required = all(field in data for field in required_fields)
    
    # Additional validation - ensure ticker is not empty
    if has_required and data.get('ticker'):
        return True
    
    logger.error(f"[VALIDATION ERROR] Invalid stock data structure: {data}")
    return False

def ensure_redis_connection():
    """Ensure Redis connection is active, attempt reconnection if needed."""
    global cache_enabled
    if not cache_enabled:
        logger.warning("[REDIS] Connection lost, attempting to reconnect...")
        return initialize_redis()
    
    # Test existing connection
    try:
        redis_client.ping()
        return True
    except (redis.exceptions.RedisError, AttributeError):
        print("[REDIS] Connection lost, attempting to reconnect...")
        cache_enabled = False
        return initialize_redis()

def initialize_redis():
    """Initialize Redis connection with multiple host fallbacks."""
    global redis_client, cache_enabled
    
    # Try different Redis host configurations
    redis_hosts = [
        REDIS_HOST,  # From environment variable
        'localhost',
        'redis',  # Docker service name
        '127.0.0.1'
    ]
    
    for host in redis_hosts:
        try:
            logger.info(f"[REDIS] Attempting to connect to {host}:{REDIS_PORT}")
            
            # Create Redis client with connection timeout
            redis_client = redis.Redis(
                host=host,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=30,
                #retry_on_timeout=True,
                retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError]
            )
            
            # Test the connection
            redis_client.ping()
            
            logger.info(f"[REDIS SUCCESS] Connected to Redis at {host}:{REDIS_PORT}")
            
            cache_enabled = True
            return True
            
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"[REDIS] Failed to connect to {host}:{REDIS_PORT} - {e}")
            continue
        except Exception as e:
            logger.error(f"[REDIS] Unexpected error connecting to {host}:{REDIS_PORT} - {e}")

            continue
    
    # If we get here, all connection attempts failed
    logger.error("[REDIS ERROR] Could not connect to Redis on any host")
    logger.warning("[WARNING] Redis is not available. Falling back to in-memory cache.")
    cache_enabled = False
    redis_client = None
    return False

def test_redis_connection():
    """Test Redis connection and return status."""
    global cache_enabled
    
    if not cache_enabled or redis_client is None:
        return False
    
    try:
        redis_client.ping()
        logger.info("[REDIS] Connection test successful")
        return True
    except redis.exceptions.RedisError as e:
        logger.error("[REDIS ERROR] Connection test failed: %s", str(e))
        cache_enabled = False
        return False

def get_cache_key(ticker: str, start_date: str, end_date: str) -> str:
    """Generate a consistent cache key for stock data."""
    return f"stock:{ticker.upper()}:{start_date}:{end_date}"

@CACHE_LOOKUP_DURATION.time()
def get_stock_data(
    ticker: str,
    start_date: str,
    end_date: str,
    fetch_from_api_fn: Callable[[str, str, str], dict],
    force_refresh: bool = False
) -> dict:
    """
    Get stock data with Redis caching and in-memory fallback.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date for data range
        end_date: End date for data range
        fetch_from_api_fn: Function to fetch data from API
        force_refresh: If True, bypass cache and refresh data
    
    Returns:
        dict: Stock data
    """
    cache_key = get_cache_key(ticker, start_date, end_date)
    cached_data = None
    cache_source = None

    # Try Redis first if available
    if cache_enabled and redis_client and not force_refresh:
        if not ensure_redis_connection():
            logger.warning("[REDIS] Connection failed, falling back to memory cache")
        else:
            try:
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    cache_source = "redis"
            except redis.exceptions.RedisError as e:
                logger.error("[REDIS ERROR] Cache fetch failed: %s", str(e))
                CACHE_ERROR_COUNTER.inc()

    # Fallback to memory cache if Redis failed or is unavailable
    if not cached_data and not force_refresh:
        cached_data = get_from_memory_cache(cache_key)
        if cached_data:
            cache_source = "memory"

    # Handle force refresh - clear both caches
    if force_refresh:
        if cache_enabled and redis_client:
            try:
                redis_client.delete(cache_key)
            except redis.exceptions.RedisError:
                pass
        
        # Clear from memory cache
        with memory_cache_lock:
            memory_cache.pop(cache_key, None)

    if cached_data:
        CACHE_HIT_COUNTER.labels(cache_type=cache_source).inc()
        logger.info("[CACHE HIT - %s] %s", cache_source.upper(), cache_key)

        try:
            if isinstance(cached_data, str):
                return json.loads(cached_data)
            else:
                return cached_data  # Already a dict from memory cache
        except json.JSONDecodeError as e:
            logger.error("[CACHE ERROR] Invalid JSON in cache: %s", str(e))

            # Fall through to fetch from API

    CACHE_MISS_COUNTER.inc()
    logger.info("[CACHE MISS] %s", cache_key)

    # Fetch from API
    stock_data = fetch_from_api_fn(ticker, start_date, end_date)

    # Validate data before caching
    if not validate_stock_data(stock_data):
        logger.warning("Invalid stock data received, not caching")
        return stock_data

    # Try to cache in Redis first
    redis_cached = False
    if cache_enabled and ensure_redis_connection():
        try:
            redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(stock_data))
            logger.info("[CACHE SAVED - REDIS] %s", cache_key)
            redis_cached = True
        except redis.exceptions.RedisError as e:
            logger.error("[REDIS ERROR] Cache save failed: %s", str(e))
            CACHE_ERROR_COUNTER.inc()
        except (TypeError, ValueError) as e:
            logger.error("[CACHE ERROR] Failed to serialize data: %s", str(e))
            CACHE_ERROR_COUNTER.inc()

    # Always save to memory cache as backup
    if not redis_cached:
        set_memory_cache(cache_key, stock_data)
        logger.info("[CACHE SAVED - MEMORY] %s", cache_key)

    return stock_data

def invalidate_cache(ticker: str, start_date: str, end_date: str):
    """Invalidate cache for specific stock data from both Redis and memory."""
    cache_key = get_cache_key(ticker, start_date, end_date)
    
    # Invalidate from Redis
    if cache_enabled and ensure_redis_connection():
        try:
            result = redis_client.delete(cache_key)
            if result:
                logger.info("[CACHE INVALIDATED - REDIS] %s", cache_key)
            else:
                logger.info("[CACHE] Redis key not found: %s", cache_key)
        except redis.exceptions.RedisError as e:
            logger.error("[REDIS ERROR] Failed to invalidate cache: %s", str(e))
            CACHE_ERROR_COUNTER.inc()
    
    # Invalidate from memory cache
    with memory_cache_lock:
        if memory_cache.pop(cache_key, None):
            logger.info("[CACHE INVALIDATED - MEMORY] %s", cache_key)
        else:
            logger.info("[CACHE] Memory key not found: %s", cache_key)

def invalidate_ticker_cache(ticker: str):
    """Invalidate all cache entries for a specific ticker from both Redis and memory."""
    
    # Invalidate from Redis
    if cache_enabled and ensure_redis_connection():
        try:
            pattern = f"stock:{ticker.upper()}:*"
            keys = redis_client.keys(pattern)
            if keys:
                redis_client.delete(*keys)
                logger.info("[CACHE INVALIDATED - REDIS] %d entries for ticker %s", len(keys), ticker)
            else:
                logger.info("[CACHE] No Redis entries found for ticker %s", ticker)
        except redis.exceptions.RedisError as e:
            logger.error("[REDIS ERROR] Failed to invalidate ticker cache: %s", str(e))
            CACHE_ERROR_COUNTER.inc()
    
    # Invalidate from memory cache
    with memory_cache_lock:
        pattern = f"stock:{ticker.upper()}:"
        keys_to_remove = [key for key in memory_cache.keys() if key.startswith(pattern)]
        for key in keys_to_remove:
            memory_cache.pop(key, None)
        
        if keys_to_remove:
            logger.info("[CACHE INVALIDATED - MEMORY] %d entries for ticker %s", len(keys_to_remove), ticker)
        else:
            logger.info("[CACHE] No memory entries found for ticker %s", ticker)

def get_cache_stats():
    """Get cache statistics including both Redis and memory cache."""
    # Calculate Redis cache stats
    redis_hits = CACHE_HIT_COUNTER.labels(cache_type="redis")._value._value
    memory_hits = CACHE_HIT_COUNTER.labels(cache_type="memory")._value._value
    total_hits = redis_hits + memory_hits
    total_misses = CACHE_MISS_COUNTER._value._value
    total_operations = total_hits + total_misses
    
    hit_rate = total_hits / max(1, total_operations) if total_operations > 0 else 0
    
    stats = {
        "cache_enabled": cache_enabled,
        "redis_cache_hits": redis_hits,
        "memory_cache_hits": memory_hits,
        "total_cache_hits": total_hits,
        "cache_misses": total_misses,
        "cache_errors": CACHE_ERROR_COUNTER._value._value,
        "hit_rate": round(hit_rate, 4),
        "total_operations": total_operations,
        "memory_cache_size": len(memory_cache),
        "memory_cache_maxsize": memory_cache.maxsize
    }
    
    # Add Redis info if available
    if cache_enabled and redis_client:
        try:
            info = redis_client.info()
            stats.update({
                "redis_version": info.get('redis_version', 'unknown'),
                "used_memory": info.get('used_memory_human', 'unknown'),
                "connected_clients": info.get('connected_clients', 0)
            })
        except redis.exceptions.RedisError:
            pass
    
    return stats

def get_redis_health():
    """Get Redis health status."""
    if not cache_enabled or not redis_client:
        return {"status": "disabled", "message": "Redis cache is disabled, using memory cache"}
    
    try:
        start_time = time.time()
        redis_client.ping()
        response_time = time.time() - start_time
        
        return {
            "status": "healthy",
            "response_time_ms": round(response_time * 1000, 2),
            "host": REDIS_HOST,
            "port": REDIS_PORT
        }
    except redis.exceptions.RedisError as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "host": REDIS_HOST,
            "port": REDIS_PORT
        }

# Initialize Redis connection when module is imported
initialize_redis()

if __name__ == "__main__":
    logger.info("Starting Redis Cache Service...")
    logger.info("Redis Status: %s", "Enabled" if cache_enabled else "Disabled (using memory cache)")
    
    # Display initial stats
    stats = get_cache_stats()
    logger.info("Initial stats: %s", stats)
    
    # Start Prometheus metrics server
    start_metrics_server(8000)

    # Keep the service running and periodically report stats
    try:
        while True:
            time.sleep(30)  # Report every 30 seconds
            stats = get_cache_stats()
            health = get_redis_health()
            print(f"[STATS] {stats}")
            print(f"[HEALTH] {health}")
    except KeyboardInterrupt:
        print("\nShutting down Redis Cache Service...")

 
    