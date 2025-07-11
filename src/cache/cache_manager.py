import redis
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Dict, Union
from functools import wraps
from cachetools import TTLCache
import os

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, cache_type: str = "redis", ttl: int = 86400):
        self.cache_type = cache_type.lower()
        self.ttl = ttl  # 24 hours default
        self.stats = {"hits": 0, "misses": 0}
        
        if self.cache_type == "redis":
            self._init_redis()
        else:
            self._init_memory_cache()
    
    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Redis cache initialized")
        except Exception as e:
            logger.warning(f"Redis unavailable, falling back to memory: {e}")
            self._init_memory_cache()
    
    def _init_memory_cache(self):
        """Initialize in-memory cache"""
        self.memory_cache = TTLCache(maxsize=1000, ttl=self.ttl)
        self.cache_type = "memory"
        logger.info("Memory cache initialized")
    
    def _generate_key(self, ticker: str, start_date: str, end_date: str, **kwargs) -> str:
        """Generate cache key"""
        key_data = f"{ticker}:{start_date}:{end_date}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, ticker: str, start_date: str, end_date: str, **kwargs) -> Optional[Dict]:
        """Get data from cache"""
        key = self._generate_key(ticker, start_date, end_date, **kwargs)
        
        try:
            if self.cache_type == "redis":
                data = self.redis_client.get(key)
                if data:
                    self.stats["hits"] += 1
                    return json.loads(data)
            else:
                data = self.memory_cache.get(key)
                if data:
                    self.stats["hits"] += 1
                    return data
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        
        self.stats["misses"] += 1
        return None
    
    def set(self, ticker: str, start_date: str, end_date: str, data: Dict, **kwargs):
        """Set data in cache"""
        key = self._generate_key(ticker, start_date, end_date, **kwargs)
        
        try:
            if self.cache_type == "redis":
                self.redis_client.setex(key, self.ttl, json.dumps(data))
            else:
                self.memory_cache[key] = data
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    def invalidate_ticker(self, ticker: str):
        """Invalidate all cache entries for a ticker"""
        try:
            if self.cache_type == "redis":
                pattern = f"*{ticker}*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
            else:
                keys_to_remove = [k for k in self.memory_cache.keys() if ticker in k]
                for key in keys_to_remove:
                    del self.memory_cache[key]
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total = self.stats["hits"] + self.stats["misses"]
        hit_rate = (self.stats["hits"] / total * 100) if total > 0 else 0
        return {
            "hits": self.stats["hits"],
            "misses": self.stats["misses"],
            "hit_rate": round(hit_rate, 2)
        }

# Global cache instance
cache = CacheManager(
    cache_type=os.getenv("CACHE_TYPE", "redis"),
    ttl=int(os.getenv("CACHE_TTL", "86400"))
)