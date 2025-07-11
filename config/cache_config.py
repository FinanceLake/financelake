import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class CacheConfig:
    """Cache configuration settings"""
    
    # Cache type: 'redis' or 'memory'
    CACHE_TYPE: str = os.getenv("CACHE_TYPE", "redis")
    
    # Redis settings
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
    
    # Cache TTL settings (in seconds)
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "86400"))  # 24 hours
    STOCK_DATA_TTL: int = int(os.getenv("STOCK_DATA_TTL", "3600"))  # 1 hour
    TECHNICAL_INDICATOR_TTL: int = int(os.getenv("TECHNICAL_INDICATOR_TTL", "1800"))  # 30 minutes
    
    # Memory cache settings
    MEMORY_CACHE_SIZE: int = int(os.getenv("MEMORY_CACHE_SIZE", "1000"))
    
    # Cache monitoring
    ENABLE_CACHE_METRICS: bool = os.getenv("ENABLE_CACHE_METRICS", "true").lower() == "true"
    METRICS_LOG_INTERVAL: int = int(os.getenv("METRICS_LOG_INTERVAL", "300"))  # 5 minutes
    
    # Cache invalidation settings
    AUTO_INVALIDATE_ON_MARKET_CLOSE: bool = os.getenv("AUTO_INVALIDATE_ON_MARKET_CLOSE", "true").lower() == "true"
    MARKET_CLOSE_TIME: str = os.getenv("MARKET_CLOSE_TIME", "16:00")  # EST
    
    # Prefetch settings
    ENABLE_PREFETCH: bool = os.getenv("ENABLE_PREFETCH", "false").lower() == "true"
    PREFETCH_POPULAR_STOCKS: list = os.getenv("PREFETCH_POPULAR_STOCKS", "AAPL,GOOGL,MSFT,TSLA,AMZN").split(",")

# Global config instance
cache_config = CacheConfig()