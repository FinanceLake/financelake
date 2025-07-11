from functools import wraps, lru_cache
import hashlib
import json
import logging
from typing import Callable, Any
from src.cache.cache_manager import cache

logger = logging.getLogger(__name__)

def cached_stock_data(ttl: int = 86400):
    """Decorator for caching stock data functions"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key from function args
            cache_key = _generate_function_key(func.__name__, args, kwargs)
            
            # Try to get from cache
            try:
                if cache.cache_type == "redis":
                    cached_result = cache.redis_client.get(cache_key)
                    if cached_result:
                        cache.stats["hits"] += 1
                        return json.loads(cached_result)
                else:
                    cached_result = cache.memory_cache.get(cache_key)
                    if cached_result:
                        cache.stats["hits"] += 1
                        return cached_result
            except Exception as e:
                logger.error(f"Cache retrieval error: {e}")
            
            # Cache miss - execute function
            cache.stats["misses"] += 1
            result = func(*args, **kwargs)
            
            # Store in cache
            try:
                if cache.cache_type == "redis":
                    cache.redis_client.setex(cache_key, ttl, json.dumps(result))
                else:
                    cache.memory_cache[cache_key] = result
            except Exception as e:
                logger.error(f"Cache storage error: {e}")
            
            return result
        return wrapper
    return decorator

def _generate_function_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Generate cache key for function calls"""
    key_data = f"{func_name}:{str(args)}:{json.dumps(kwargs, sort_keys=True)}"
    return hashlib.md5(key_data.encode()).hexdigest()

# Simple LRU cache for frequently used calculations
@lru_cache(maxsize=100)
def cached_calculation(ticker: str, calculation_type: str, data_hash: str):
    """Cache for expensive calculations like technical indicators"""
    # This would be used by technical analysis functions
    pass

def cache_technical_indicator(maxsize: int = 128):
    """Decorator for caching technical indicators"""
    def decorator(func: Callable) -> Callable:
        cached_func = lru_cache(maxsize=maxsize)(func)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return cached_func(*args, **kwargs)
            except TypeError:
                # Handle unhashable types by falling back to direct execution
                logger.warning(f"Cannot cache {func.__name__} due to unhashable arguments")
                return func(*args, **kwargs)
        
        # Add cache info method
        wrapper.cache_info = cached_func.cache_info
        wrapper.cache_clear = cached_func.cache_clear
        
        return wrapper
    return decorator