#_init__.py
"""
Redis Cache Module for Stock Data

This module provides a Redis-backed caching layer to optimize frequent 
stock data queries by caching results based on stock ticker and date range.
"""

from .redis_cache import (
    get_stock_data,
    invalidate_cache,
    invalidate_ticker_cache,
    get_cache_stats,
    get_cache_key,
    start_metrics_server
)

__version__ = "1.0.0"
__all__ = [
    "get_stock_data",
    "invalidate_cache", 
    "invalidate_ticker_cache",
    "get_cache_stats",
    "get_cache_key",
    "start_metrics_server"
]