# ============================================================================
# Data Ingestion Service
# ============================================================================
# Enterprise-grade data ingestion with multiple sources, validation,
# and real-time streaming to Kafka
# ============================================================================

import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

# Simple configuration without shared library
import os
from typing import Dict, Any

class Settings:
    def __init__(self):
        self.service_name = "data-ingestion"
        self.app_version = "1.0.0"
        self.environment = "development"
        self.log_level = "INFO"
        self.log_format = "json"
        self.service_host = "0.0.0.0"
        self.service_port = 8000
        self.cors_origins = ["*"]
        self.redis_url = "redis://redis:6379"
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.postgres_database = os.getenv("POSTGRES_DATABASE", "financelake")
        self.postgres_username = os.getenv("POSTGRES_USERNAME", "financelake")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "financelake_password")
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.tracked_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

# Simple logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_logger(name: str):
    return logging.getLogger(name)

def get_settings():
    return Settings()

# Simple health check
class HealthCheck:
    def __init__(self):
        self.healthy = True

    def mark_healthy(self):
        self.healthy = True

    def mark_unhealthy(self, reason: str = ""):
        self.healthy = False

    def get_status(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if self.healthy else "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "service": settings.service_name,
            "version": settings.app_version
        }

# Simple exception handling
class FinanceLakeException(Exception):
    def __init__(self, message: str, error_code: str = "INTERNAL_ERROR", status_code: int = 500, details: Dict = None):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or {}

def create_error_response(exc: Exception, correlation_id: str = None) -> Dict[str, Any]:
    if isinstance(exc, FinanceLakeException):
        return {
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "details": exc.details
            },
            "correlation_id": correlation_id,
            "timestamp": datetime.now().isoformat()
        }
    else:
        return {
            "error": {
                "code": "INTERNAL_ERROR",
                "message": str(exc)
            },
            "correlation_id": correlation_id,
            "timestamp": datetime.now().isoformat()
        }

# Simple correlation ID handling
_correlation_id = None

def set_correlation_id(correlation_id: str):
    global _correlation_id
    _correlation_id = correlation_id

def get_correlation_id() -> str:
    return _correlation_id

# Configuration
settings: Settings = get_settings()
logger = get_logger(__name__)
health_check = HealthCheck()

# Global variables
kafka_producer: Optional[KafkaProducer] = None
redis_client: Optional[redis.Redis] = None
db_connection: Optional[psycopg2.extensions.connection] = None

# Create FastAPI application
app = FastAPI(
    title="FinanceLake - Data Ingestion Service",
    description="Enterprise-grade data ingestion service for real-time financial data",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# LIFECYCLE MANAGEMENT
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize service on startup."""
    global kafka_producer, redis_client, db_connection

    logger.info("Starting Data Ingestion Service", extra={
        "service": settings.service_name,
        "version": settings.app_version,
        "environment": settings.environment,
    })

    # Initialize Kafka producer with retry
    kafka_producer = None
    max_retries = 10
    for attempt in range(max_retries):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
            logger.info("Kafka producer initialized", extra={
                "bootstrap_servers": settings.kafka_bootstrap_servers,
                "attempt": attempt + 1
            })
            break
        except Exception as e:
            logger.warning(f"Failed to initialize Kafka producer (attempt {attempt + 1}/{max_retries})", extra={"error": str(e)})
            if attempt < max_retries - 1:
                import time
                time.sleep(5)  # Wait 5 seconds before retrying
            else:
                logger.error("Failed to initialize Kafka producer after all retries", extra={"error": str(e)})
                raise

    # Initialize Redis
    try:
        redis_client = redis.Redis.from_url(settings.redis_url)
        redis_client.ping()  # Test connection
        logger.info("Redis client initialized")
    except Exception as e:
        logger.error("Failed to initialize Redis client", extra={"error": str(e)})
        raise

    # Initialize PostgreSQL
    try:
        db_connection = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_database,
            user=settings.postgres_username,
            password=settings.postgres_password,
        )
        db_connection.autocommit = True
        logger.info("PostgreSQL connection initialized")
    except Exception as e:
        logger.error("Failed to initialize PostgreSQL connection", extra={"error": str(e)})
        raise

    # Mark service as healthy
    health_check.mark_healthy()
    logger.info("Data Ingestion Service started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global kafka_producer, redis_client, db_connection

    logger.info("Shutting down Data Ingestion Service")

    # Close Kafka producer
    if kafka_producer:
        kafka_producer.close()
        logger.info("Kafka producer closed")

    # Close database connection
    if db_connection:
        db_connection.close()
        logger.info("Database connection closed")

    # Mark service as unhealthy
    health_check.mark_unhealthy("shutdown")

# ============================================================================
# EXCEPTION HANDLERS
# ============================================================================

@app.exception_handler(FinanceLakeException)
async def finance_lake_exception_handler(request, exc: FinanceLakeException):
    """Handle FinanceLake exceptions."""
    correlation_id = get_correlation_id()
    logger.error(
        "FinanceLake exception occurred",
        extra={
            "correlation_id": correlation_id,
            "error_code": exc.error_code,
            "status_code": exc.status_code,
            "details": exc.details,
        }
    )

    return HTTPException(
        status_code=exc.status_code,
        detail=create_error_response(exc, correlation_id)
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    """Handle general exceptions."""
    correlation_id = get_correlation_id()
    logger.error(
        "Unexpected exception occurred",
        extra={
            "correlation_id": correlation_id,
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
        },
        exc_info=True
    )

    return HTTPException(
        status_code=500,
        detail=create_error_response(exc, correlation_id)
    )

# ============================================================================
# MIDDLEWARE
# ============================================================================

@app.middleware("http")
async def correlation_middleware(request, call_next):
    """Add correlation ID to request context."""
    correlation_id = request.headers.get("X-Correlation-ID")
    if not correlation_id:
        correlation_id = f"{settings.service_name}-{uuid.uuid4().hex[:8]}"

    set_correlation_id(correlation_id)

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

# ============================================================================
# HEALTH CHECK ENDPOINTS
# ============================================================================

@app.get("/health")
async def health():
    """Health check endpoint."""
    return health_check.get_status()

@app.get("/health/detailed")
async def detailed_health():
    """Detailed health check with component status."""
    health_status = health_check.get_status()

    # Check Kafka connectivity
    kafka_healthy = False
    if kafka_producer:
        try:
            # Simple connectivity check
            kafka_producer.metrics()
            kafka_healthy = True
        except Exception:
            kafka_healthy = False

    # Check Redis connectivity
    redis_healthy = False
    if redis_client:
        try:
            redis_client.ping()
            redis_healthy = True
        except Exception:
            redis_healthy = False

    # Check database connectivity
    db_healthy = False
    if db_connection:
        try:
            with db_connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            db_healthy = True
        except Exception:
            db_healthy = False

    health_status["components"] = {
        "kafka": {
            "healthy": kafka_healthy,
            "details": {
                "bootstrap_servers": settings.kafka_bootstrap_servers
            }
        },
        "redis": {
            "healthy": redis_healthy,
            "details": {
                "url": settings.redis_url
            }
        },
        "database": {
            "healthy": db_healthy,
            "details": {
                "host": settings.postgres_host,
                "database": settings.postgres_database
            }
        }
    }

    return health_status

# ============================================================================
# INGESTION ENDPOINTS
# ============================================================================

@app.post("/ingest/market-data")
async def ingest_market_data(
    symbols: List[str] = None,
    sources: List[str] = None,
    frequency: str = "1s",
    duration: int = 60,
    background_tasks: BackgroundTasks = None
):
    """
    Start market data ingestion from various sources.

    Args:
        symbols: List of stock symbols to track
        sources: Data sources (yfinance, alpha_vantage, polygon)
        frequency: Ingestion frequency
        duration: Duration in seconds
    """
    if symbols is None:
        symbols = settings.tracked_symbols

    if sources is None:
        sources = ["yfinance"]

    correlation_id = get_correlation_id()

    logger.info(
        "Starting market data ingestion",
        extra={
            "correlation_id": correlation_id,
            "symbols": symbols,
            "sources": sources,
            "frequency": frequency,
            "duration": duration,
        }
    )

    # Generate task ID
    task_id = f"market-data-{correlation_id}"

    # Start background ingestion task
    background_tasks.add_task(
        run_market_data_ingestion,
        task_id,
        symbols,
        sources,
        frequency,
        duration
    )

    return {
        "task_id": task_id,
        "status": "started",
        "message": "Market data ingestion started successfully"
    }

@app.post("/ingest/simulate")
async def ingest_simulated_data(
    symbols: List[str] = None,
    frequency: str = "1s",
    duration: int = 60,
    background_tasks: BackgroundTasks = None
):
    """
    Start simulated data ingestion for testing.

    Args:
        symbols: List of stock symbols to simulate
        frequency: Ingestion frequency
        duration: Duration in seconds
    """
    if symbols is None:
        symbols = settings.tracked_symbols

    correlation_id = get_correlation_id()

    logger.info(
        "Starting simulated data ingestion",
        extra={
            "correlation_id": correlation_id,
            "symbols": symbols,
            "frequency": frequency,
            "duration": duration,
        }
    )

    # Generate task ID
    task_id = f"simulated-data-{correlation_id}"

    # Start background ingestion task
    background_tasks.add_task(
        run_simulated_data_ingestion,
        task_id,
        symbols,
        frequency,
        duration
    )

    return {
        "task_id": task_id,
        "status": "started",
        "message": "Simulated data ingestion started successfully"
    }

@app.get("/ingest/status/{task_id}")
async def get_ingestion_status(task_id: str):
    """Get the status of an ingestion task."""
    # Check Redis for task status
    status_key = f"ingestion:task:{task_id}"

    try:
        status_data = redis_client.get(status_key)
        if status_data:
            return json.loads(status_data)
        else:
            return {
                "task_id": task_id,
                "status": "not_found",
                "message": "Task not found"
            }
    except Exception as e:
        logger.error("Failed to get task status", extra={
            "task_id": task_id,
            "error": str(e)
        })
        raise HTTPException(status_code=500, detail="Failed to get task status")

@app.get("/ingest/stats")
async def get_ingestion_stats():
    """Get ingestion statistics and metrics."""
    try:
        # Get stats from Redis
        total_ingestions = redis_client.get("ingestion:stats:total") or 0
        active_ingestions = len(redis_client.keys("ingestion:task:*"))
        completed_ingestions = redis_client.get("ingestion:stats:completed") or 0
        failed_ingestions = redis_client.get("ingestion:stats:failed") or 0
        data_volume = redis_client.get("ingestion:stats:volume") or 0

        return {
            "total_ingestions": int(total_ingestions),
            "active_ingestions": active_ingestions,
            "completed_ingestions": int(completed_ingestions),
            "failed_ingestions": int(failed_ingestions),
            "data_volume": int(data_volume),
            "throughput": "N/A",  # Would need more complex calculation
        }
    except Exception as e:
        logger.error("Failed to get ingestion stats", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail="Failed to get ingestion stats")

# ============================================================================
# INGESTION TASK FUNCTIONS
# ============================================================================

async def run_market_data_ingestion(
    task_id: str,
    symbols: List[str],
    sources: List[str],
    frequency: str,
    duration: int
):
    """Run market data ingestion task."""
    try:
        # Update task status
        update_task_status(task_id, "running", {"message": "Starting market data ingestion"})

        # Parse frequency
        if frequency.endswith('s'):
            interval = int(frequency[:-1])
        elif frequency.endswith('m'):
            interval = int(frequency[:-1]) * 60
        else:
            interval = 1

        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration)
        messages_sent = 0

        logger.info("Market data ingestion started", extra={
            "task_id": task_id,
            "symbols": symbols,
            "sources": sources,
            "interval": interval,
            "duration": duration,
        })

        while datetime.now() < end_time:
            for symbol in symbols:
                try:
                    # Generate mock market data (simplified)
                    price_data = generate_market_data(symbol)

                    # Send to Kafka
                    kafka_producer.send(
                        topic="stock-data",
                        key=symbol,
                        value=price_data
                    )

                    # Store in database
                    store_market_data(price_data)

                    messages_sent += 1

                    # Update progress
                    if messages_sent % 10 == 0:
                        update_task_status(task_id, "running", {
                            "message": f"Processed {messages_sent} messages",
                            "progress": min(100, (messages_sent / (len(symbols) * duration / interval)) * 100)
                        })

                except Exception as e:
                    logger.error("Failed to process symbol", extra={
                        "task_id": task_id,
                        "symbol": symbol,
                        "error": str(e)
                    })

            await asyncio.sleep(interval)

        # Mark as completed
        update_task_status(task_id, "completed", {
            "message": f"Successfully processed {messages_sent} messages",
            "messages_sent": messages_sent
        })

        # Update stats
        redis_client.incr("ingestion:stats:total")
        redis_client.incr("ingestion:stats:completed")
        redis_client.incrby("ingestion:stats:volume", messages_sent)

        logger.info("Market data ingestion completed", extra={
            "task_id": task_id,
            "messages_sent": messages_sent
        })

    except Exception as e:
        # Mark as failed
        update_task_status(task_id, "failed", {
            "message": f"Ingestion failed: {str(e)}",
            "error": str(e)
        })

        # Update stats
        redis_client.incr("ingestion:stats:failed")

        logger.error("Market data ingestion failed", extra={
            "task_id": task_id,
            "error": str(e)
        }, exc_info=True)

async def run_simulated_data_ingestion(
    task_id: str,
    symbols: List[str],
    frequency: str,
    duration: int
):
    """Run simulated data ingestion task."""
    try:
        # Update task status
        update_task_status(task_id, "running", {"message": "Starting simulated data ingestion"})

        # Parse frequency
        if frequency.endswith('s'):
            interval = int(frequency[:-1])
        elif frequency.endswith('m'):
            interval = int(frequency[:-1]) * 60
        else:
            interval = 1

        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration)
        messages_sent = 0

        logger.info("Simulated data ingestion started", extra={
            "task_id": task_id,
            "symbols": symbols,
            "interval": interval,
            "duration": duration,
        })

        while datetime.now() < end_time:
            for symbol in symbols:
                try:
                    # Generate simulated market data
                    simulated_data = generate_simulated_data(symbol)

                    # Send to Kafka
                    kafka_producer.send(
                        topic="simulated-stock-data",
                        key=symbol,
                        value=simulated_data
                    )

                    messages_sent += 1

                except Exception as e:
                    logger.error("Failed to generate simulated data", extra={
                        "task_id": task_id,
                        "symbol": symbol,
                        "error": str(e)
                    })

            await asyncio.sleep(interval)

        # Mark as completed
        update_task_status(task_id, "completed", {
            "message": f"Successfully generated {messages_sent} simulated messages",
            "messages_sent": messages_sent
        })

        # Update stats
        redis_client.incr("ingestion:stats:total")
        redis_client.incr("ingestion:stats:completed")
        redis_client.incrby("ingestion:stats:volume", messages_sent)

        logger.info("Simulated data ingestion completed", extra={
            "task_id": task_id,
            "messages_sent": messages_sent
        })

    except Exception as e:
        # Mark as failed
        update_task_status(task_id, "failed", {
            "message": f"Simulated ingestion failed: {str(e)}",
            "error": str(e)
        })

        # Update stats
        redis_client.incr("ingestion:stats:failed")

        logger.error("Simulated data ingestion failed", extra={
            "task_id": task_id,
            "error": str(e)
        }, exc_info=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def update_task_status(task_id: str, status: str, details: Dict[str, Any]):
    """Update task status in Redis."""
    try:
        status_data = {
            "task_id": task_id,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            **details
        }
        redis_client.setex(
            f"ingestion:task:{task_id}",
            3600,  # 1 hour expiry
            json.dumps(status_data)
        )
    except Exception as e:
        logger.error("Failed to update task status", extra={
            "task_id": task_id,
            "error": str(e)
        })

def generate_market_data(symbol: str) -> Dict[str, Any]:
    """Generate mock market data for a symbol."""
    import random
    from datetime import datetime

    # Mock data generation (replace with real API calls)
    base_price = 100 + random.uniform(-50, 50)
    price_change = random.uniform(-2, 2)

    return {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "price": round(base_price + price_change, 2),
        "volume": random.randint(1000, 100000),
        "price_change": round(price_change, 2),
        "rsi": round(random.uniform(20, 80), 2),
        "macd": round(random.uniform(-2, 2), 2),
        "volatility": round(random.uniform(0.5, 5.0), 2),
        "source": "mock_api"
    }

def generate_simulated_data(symbol: str) -> Dict[str, Any]:
    """Generate simulated market data."""
    import random
    from datetime import datetime

    return {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "price": round(100 + random.uniform(-20, 20), 2),
        "volume": random.randint(10000, 50000),
        "price_change": round(random.uniform(-1, 1), 2),
        "rsi": round(random.uniform(30, 70), 2),
        "macd": round(random.uniform(-1, 1), 2),
        "volatility": round(random.uniform(1, 3), 2),
        "source": "simulated"
    }

def store_market_data(data: Dict[str, Any]):
    """Store market data in PostgreSQL."""
    try:
        with db_connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO market_data (
                    symbol, timestamp, price, volume, price_change,
                    rsi, macd, volatility, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING
            """, (
                data["symbol"],
                data["timestamp"],
                data["price"],
                data["volume"],
                data["price_change"],
                data["rsi"],
                data["macd"],
                data["volatility"],
                data["source"]
            ))
    except Exception as e:
        logger.error("Failed to store market data", extra={
            "symbol": data.get("symbol"),
            "error": str(e)
        })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Configure logging
    configure_logging(
        level=settings.log_level,
        format_type=settings.log_format,
        service_name=settings.service_name
    )

    # Start the server
    uvicorn.run(
        "main:app",
        host=settings.service_host,
        port=settings.service_port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
