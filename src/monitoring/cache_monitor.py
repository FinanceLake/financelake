import time
import logging
import threading
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass, asdict
from src.cache.cache_manager import cache
from config.cache_config import cache_config

logger = logging.getLogger(__name__)

@dataclass
class CacheMetrics:
    timestamp: str
    hits: int
    misses: int
    hit_rate: float
    response_time_avg: float
    memory_usage: int = 0
    
class CacheMonitor:
    def __init__(self):
        self.metrics_history: List[CacheMetrics] = []
        self.response_times: List[float] = []
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start cache monitoring in a separate thread"""
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("Cache monitoring started")
    
    def stop_monitoring(self):
        """Stop cache monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("Cache monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                self._collect_metrics()
                time.sleep(cache_config.METRICS_LOG_INTERVAL)
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
    
    def _collect_metrics(self):
        """Collect current cache metrics"""
        stats = cache.get_stats()
        
        # Calculate average response time
        avg_response_time = (
            sum(self.response_times) / len(self.response_times) 
            if self.response_times else 0
        )
        
        # Create metrics snapshot
        metrics = CacheMetrics(
            timestamp=datetime.now().isoformat(),
            hits=stats["hits"],
            misses=stats["misses"],
            hit_rate=stats["hit_rate"],
            response_time_avg=avg_response_time
        )
        
        # Add memory usage for in-memory cache
        if cache.cache_type == "memory":
            metrics.memory_usage = len(cache.memory_cache)
        
        self.metrics_history.append(metrics)
        
        # Keep only last 100 metrics
        if len(self.metrics_history) > 100:
            self.metrics_history = self.metrics_history[-100:]
        
        # Clear response times
        self.response_times.clear()
        
        # Log metrics if enabled
        if cache_config.ENABLE_CACHE_METRICS:
            logger.info(f"Cache Metrics: {asdict(metrics)}")
    
    def record_response_time(self, response_time: float):
        """Record a response time for averaging"""
        self.response_times.append(response_time)
    
    def get_current_metrics(self) -> Dict:
        """Get current cache metrics"""
        if not self.metrics_history:
            return {}
        
        latest = self.metrics_history[-1]
        return asdict(latest)
    
    def get_metrics_history(self, limit: int = 50) -> List[Dict]:
        """Get metrics history"""
        return [asdict(m) for m in self.metrics_history[-limit:]]
    
    def get_performance_summary(self) -> Dict:
        """Get cache performance summary"""
        if not self.metrics_history:
            return {"error": "No metrics available"}
        
        recent_metrics = self.metrics_history[-10:]  # Last 10 readings
        
        avg_hit_rate = sum(m.hit_rate for m in recent_metrics) / len(recent_metrics)
        avg_response_time = sum(m.response_time_avg for m in recent_metrics) / len(recent_metrics)
        total_requests = recent_metrics[-1].hits + recent_metrics[-1].misses
        
        return {
            "average_hit_rate": round(avg_hit_rate, 2),
            "average_response_time": round(avg_response_time, 4),
            "total_requests": total_requests,
            "cache_type": cache.cache_type,
            "monitoring_duration": len(self.metrics_history) * cache_config.METRICS_LOG_INTERVAL
        }

# Global monitor instance
cache_monitor = CacheMonitor()

def timed_cache_operation(func):
    """Decorator to time cache operations"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        response_time = end_time - start_time
        cache_monitor.record_response_time(response_time)
        
        return result
    return wrapper