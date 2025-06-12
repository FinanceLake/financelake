import yfinance as yf
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Optional
from src.cache.cache_manager import cache

logger = logging.getLogger(__name__)

class StockDataService:
    def __init__(self):
        self.cache = cache
    
    def get_stock_data(self, ticker: str, start_date: str, end_date: str, 
                      interval: str = "1d") -> Optional[Dict]:
        """Get stock data with caching"""
        # Check cache first
        cached_data = self.cache.get(ticker, start_date, end_date, interval=interval)
        if cached_data:
            logger.info(f"Cache HIT for {ticker} ({start_date} to {end_date})")
            return cached_data
        
        logger.info(f"Cache MISS for {ticker} ({start_date} to {end_date})")
        
        # Fetch from API
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date, interval=interval)
            
            if hist.empty:
                logger.warning(f"No data found for {ticker}")
                return None
            
            # Convert to dict for caching
            data = {
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
                "interval": interval,
                "data": hist.to_dict("records"),
                "columns": list(hist.columns),
                "fetched_at": datetime.now().isoformat()
            }
            
            # Cache the result
            self.cache.set(ticker, start_date, end_date, data, interval=interval)
            logger.info(f"Data cached for {ticker}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
            return None
    
    def get_multiple_stocks(self, tickers: list, start_date: str, end_date: str) -> Dict:
        """Get data for multiple stocks"""
        results = {}
        for ticker in tickers:
            results[ticker] = self.get_stock_data(ticker, start_date, end_date)
        return results
    
    def invalidate_stock_cache(self, ticker: str):
        """Invalidate cache for a specific stock"""
        self.cache.invalidate_ticker(ticker)
        logger.info(f"Cache invalidated for {ticker}")
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        return self.cache.get_stats()

# Global service instance
stock_service = StockDataService()