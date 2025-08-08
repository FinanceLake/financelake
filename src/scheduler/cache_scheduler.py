import schedule
import time
import threading
import logging
from datetime import datetime, time as dt_time
from src.cache.cache_manager import cache
from src.services.data_service import stock_service
from config.cache_config import cache_config

logger = logging.getLogger(__name__)

class CacheScheduler:
    def __init__(self):
        self.scheduler_thread = None
        self.running = False
    
    def start(self):
        """Start the cache scheduler"""
        if not self.running:
            self.running = True
            self._setup_schedules()
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()
            logger.info("Cache scheduler started")
    
    def stop(self):
        """Stop the cache scheduler"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        logger.info("Cache scheduler stopped")
    
    def _setup_schedules(self):
        """Setup scheduled tasks"""
        # Daily cache cleanup at market close
        if cache_config.AUTO_INVALIDATE_ON_MARKET_CLOSE:
            schedule.every().day.at(cache_config.MARKET_CLOSE_TIME).do(
                self._market_close_cleanup
            )
        
        # Prefetch popular stocks if enabled
        if cache_config.ENABLE_PREFETCH:
            schedule.every().day.at("09:00").do(self._prefetch_popular_stocks)
        
        # Cache health check every hour
        schedule.every().hour.do(self._cache_health_check)
        
        # Cleanup old metrics every 6 hours
        schedule.every(6).hours.do(self._cleanup_old_metrics)
    
    def _run_scheduler(self):
        """Main scheduler loop"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
    
    def _market_close_cleanup(self):
        """Clean up cache at market close"""
        try:
            logger.info("Starting market close cache cleanup")
            
            # Invalidate intraday data but keep daily data
            # This would need to be customized based on your data structure
            if hasattr(cache, 'redis_client'):
                # For Redis, you might want to selectively invalidate
                pattern_keys = cache.redis_client.keys("*1m*")  # 1-minute data
                pattern_keys.extend(cache.redis_client.keys("*5m*"))  # 5-minute data
                
                if pattern_keys:
                    cache.redis_client.delete(*pattern_keys)
                    logger.info(f"Invalidated {len(pattern_keys)} intraday cache entries")
            
            logger.info("Market close cache cleanup completed")
            
        except Exception as e:
            logger.error(f"Market close cleanup error: {e}")
    
    def _prefetch_popular_stocks(self):
        """Prefetch data for popular stocks"""
        try:
            logger.info("Starting prefetch for popular stocks")
            
            today = datetime.now().strftime('%Y-%m-%d')
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            for ticker in cache_config.PREFETCH_POPULAR_STOCKS:
                try:
                    # Prefetch 30-day data
                    stock_service.get_stock_data(ticker, thirty_days_ago, today)
                    logger.info(f"Prefetched data for {ticker}")
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    logger.warning(f"Failed to prefetch {ticker}: {e}")
            
            logger.info("Prefetch completed")
            
        except Exception as e:
            logger.error(f"Prefetch error: {e}")
    
    def _cache_health_check(self):
        """Perform cache health check"""
        try:
            stats = cache.get_stats()
            total_requests = stats['hits'] + stats['misses']
            
            logger.info(f"Cache Health Check - Hit Rate: {stats['hit_rate']}%, "
                       f"Total Requests: {total_requests}")
            
            # Alert if hit rate is too low
            if total_requests > 100 and stats['hit_rate'] < 30:
                logger.warning(f"Low cache hit rate detected: {stats['hit_rate']}%")
            
            # Check Redis connection if using Redis
            if cache.cache_type == "redis":
                try:
                    cache.redis_client.ping()
                    logger.info("Redis connection healthy")
                except Exception as e:
                    logger.error(f"Redis connection issue: {e}")
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
    
    def _cleanup_old_metrics(self):
        """Clean up old monitoring metrics"""
        try:
            from src.monitoring.cache_monitor import cache_monitor
            
            # Keep only last 50 metrics
            if len(cache_monitor.metrics_history) > 50:
                cache_monitor.metrics_history = cache_monitor.metrics_history[-50:]
                logger.info("Cleaned up old cache metrics")
                
        except Exception as e:
            logger.error(f"Metrics cleanup error: {e}")

# Global scheduler instance
cache_scheduler = CacheScheduler()

# Function to start scheduler with the application
def start_cache_scheduler():
    """Start the cache scheduler"""
    cache_scheduler.start()

# Function to stop scheduler when application shuts down
def stop_cache_scheduler():
    """Stop the cache scheduler"""
    cache_scheduler.stop()