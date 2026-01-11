# ============================================================================
# API Gateway Service
# ============================================================================
# Unified API gateway providing REST and GraphQL endpoints for all services
# ============================================================================

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import uvicorn
import redis
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
# Note: psycopg2.pool may not be available in some installations
try:
    import psycopg2_pool
    from psycopg2_pool import SimpleConnectionPool
except ImportError:
    psycopg2_pool = None
    SimpleConnectionPool = None
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# Simple configuration without shared library
import os
from typing import Dict, Any

class Settings:
    def __init__(self):
        self.service_name = "api-gateway"
        self.app_version = "1.0.0"
        self.environment = "development"
        self.log_level = "INFO"
        self.log_format = "json"
        self.service_host = "0.0.0.0"
        self.service_port = 8000
        self.cors_origins = ["http://localhost:3000", "http://localhost:8000"]
        self.redis_url = "redis://redis:6379"
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.postgres_database = os.getenv("POSTGRES_DATABASE", "financelake")
        self.postgres_username = os.getenv("POSTGRES_USERNAME", "financelake")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "financelake_password")
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

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
redis_client: Optional[redis.Redis] = None
http_client: Optional[httpx.AsyncClient] = None
db_connection_pool = None  # Will be connection pool if available, otherwise None

# Service URLs
SERVICE_URLS = {
    "data-ingestion": "http://data-ingestion:8000",
    "ml-service": "http://ml-service:8000",
    "stream-processor": "http://stream-processor:8081",
}

# WebSocket connections
active_connections: List[WebSocket] = []

# Create FastAPI application
app = FastAPI(
    title="FinanceLake - API Gateway",
    description="Unified API gateway for all FinanceLake microservices",
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
    global redis_client, http_client

    logger.info("Starting API Gateway Service", extra={
        "service": settings.service_name,
        "version": settings.app_version,
        "environment": settings.environment,
    })

    # Initialize Redis
    try:
        redis_client = redis.Redis.from_url(settings.redis_url)
        redis_client.ping()  # Test connection
        logger.info("Redis client initialized")
    except Exception as e:
        logger.error("Failed to initialize Redis client", extra={"error": str(e)})
        raise

    # Initialize HTTP client
    try:
        http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )
        logger.info("HTTP client initialized")
    except Exception as e:
        logger.error("Failed to initialize HTTP client", extra={"error": str(e)})
        raise

    # Initialize PostgreSQL connection (pool if available, otherwise store params)
    try:
        global db_connection_pool
        if SimpleConnectionPool:
            db_connection_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=settings.postgres_host,
                port=settings.postgres_port,
                database=settings.postgres_database,
                user=settings.postgres_username,
                password=settings.postgres_password,
            )
            logger.info("PostgreSQL connection pool initialized")
        else:
            # Store connection parameters for manual connection management
            db_connection_pool = {
                'host': settings.postgres_host,
                'port': settings.postgres_port,
                'database': settings.postgres_database,
                'user': settings.postgres_username,
                'password': settings.postgres_password,
            }
            logger.info("PostgreSQL connection parameters stored (pool not available)")
    except Exception as e:
        logger.error("Failed to initialize PostgreSQL connection", extra={"error": str(e)})
        raise

    # Mark service as healthy
    health_check.mark_healthy()
    logger.info("API Gateway Service started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global redis_client, http_client

    logger.info("Shutting down API Gateway Service")

    # Close HTTP client
    if http_client:
        await http_client.aclose()
        logger.info("HTTP client closed")

    # Close database connection pool
    if db_connection_pool and hasattr(db_connection_pool, 'closeall'):
        db_connection_pool.closeall()
        logger.info("Database connection pool closed")
    elif db_connection_pool:
        logger.info("Database connection parameters stored (no pool to close)")

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

    error_response = create_error_response(exc, correlation_id)
    return JSONResponse(content=error_response, status_code=exc.status_code)

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

    error_response = create_error_response(exc, correlation_id)
    return JSONResponse(content=error_response, status_code=500)

# ============================================================================
# MIDDLEWARE
# ============================================================================

@app.middleware("http")
async def correlation_middleware(request, call_next):
    """Add correlation ID to request context."""
    correlation_id = request.headers.get("X-Correlation-ID")
    if not correlation_id:
        correlation_id = f"{settings.service_name}-{asyncio.get_event_loop().time()}"

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

@app.get("/health/services")
async def service_health():
    """Check health of all downstream services."""
    health_status = health_check.get_status()

    service_health = {}
    for service_name, service_url in SERVICE_URLS.items():
        try:
            response = await http_client.get(f"{service_url}/health", timeout=5.0)
            service_health[service_name] = {
                "healthy": response.status_code == 200,
                "status_code": response.status_code,
                "url": service_url
            }
        except Exception as e:
            service_health[service_name] = {
                "healthy": False,
                "error": str(e),
                "url": service_url
            }

    health_status["services"] = service_health
    return health_status

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "FinanceLake API Gateway",
        "version": "1.0.0",
        "description": "Unified API gateway for FinanceLake microservices",
        "endpoints": {
            "health": "/health",
            "service_health": "/health/services",
            "data_ingestion": "/api/v1/data",
            "analytics": "/api/v1/analytics",
            "websocket": "/ws/market-data"
        },
        "docs": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "graphql": "/graphql"
        }
    }

# Data Ingestion API
@app.post("/api/v1/data/ingest/market")
async def ingest_market_data(request_data: Dict[str, Any]):
    """Proxy market data ingestion requests to data-ingestion service."""
    correlation_id = get_correlation_id()

    try:
        response = await http_client.post(
            f"{SERVICE_URLS['data-ingestion']}/ingest/market-data",
            json=request_data,
            headers={"X-Correlation-ID": correlation_id}
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise FinanceLakeException(
                message="Data ingestion service error",
                error_code="SERVICE_ERROR",
                status_code=response.status_code,
                details={"service": "data-ingestion", "response": response.text}
            )

    except httpx.RequestError as e:
        raise FinanceLakeException(
            message="Failed to connect to data ingestion service",
            error_code="SERVICE_UNAVAILABLE",
            status_code=503,
            details={"service": "data-ingestion", "error": str(e)}
        )

@app.post("/api/v1/data/ingest/simulate")
async def ingest_simulated_data(request_data: Dict[str, Any]):
    """Proxy simulated data ingestion requests."""
    correlation_id = get_correlation_id()

    try:
        response = await http_client.post(
            f"{SERVICE_URLS['data-ingestion']}/ingest/simulate",
            json=request_data,
            headers={"X-Correlation-ID": correlation_id}
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise FinanceLakeException(
                message="Data ingestion service error",
                error_code="SERVICE_ERROR",
                status_code=response.status_code,
                details={"service": "data-ingestion", "response": response.text}
            )

    except httpx.RequestError as e:
        raise FinanceLakeException(
            message="Failed to connect to data ingestion service",
            error_code="SERVICE_UNAVAILABLE",
            status_code=503,
            details={"service": "data-ingestion", "error": str(e)}
        )

@app.get("/api/v1/data/status/{task_id}")
async def get_ingestion_status(task_id: str, request: Request):
    """Get ingestion task status."""
    logger.info(f"Status request received for task_id: {task_id}", extra={
        "task_id": task_id,
        "user_agent": request.headers.get("user-agent", "unknown"),
        "correlation_id": request.headers.get("x-correlation-id", "unknown")
    })
    try:
        response = await http_client.get(
            f"{SERVICE_URLS['data-ingestion']}/ingest/status/{task_id}",
            timeout=30.0
        )

        if response.status_code == 200:
            result = response.json()
            logger.info(f"Status response for task_id: {task_id}", extra={
                "task_id": task_id,
                "status": result.get("status"),
                "messages_sent": result.get("messages_sent")
            })
            return JSONResponse(content=result)
        else:
            logger.warning(f"Data ingestion service returned {response.status_code} for task_id: {task_id}")
            return JSONResponse(content={"task_id": task_id, "status": "not_found"}, status_code=404)

    except httpx.RequestError as e:
        logger.error(f"Failed to connect to data ingestion service for task_id: {task_id}", extra={
            "task_id": task_id,
            "error": str(e)
        })
        # Try direct fallback to data-ingestion service
        try:
            fallback_response = await http_client.get(
                f"http://data-ingestion:8000/ingest/status/{task_id}",
                timeout=5.0
            )
            if fallback_response.status_code == 200:
                return JSONResponse(content=fallback_response.json())
        except Exception:
            pass
        return JSONResponse(content={"task_id": task_id, "status": "service_unavailable"}, status_code=503)
    except Exception as e:
        logger.error(f"Unexpected error getting task status for task_id: {task_id}", extra={
            "task_id": task_id,
            "error": str(e)
        })
        return JSONResponse(content={"task_id": task_id, "status": "error", "message": str(e)}, status_code=500)

@app.get("/api/v1/data/stats")
async def get_data_stats():
    """Get data ingestion statistics."""
    try:
        response = await http_client.get(
            f"{SERVICE_URLS['data-ingestion']}/ingest/stats"
        )

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": "Failed to get statistics"}

    except httpx.RequestError:
        return {"error": "Service unavailable"}

@app.get("/api/v1/data/stock")
async def get_stock_data(
    symbol: Optional[str] = Query(None, description="Stock symbol to filter by"),
    limit: int = Query(1000, description="Maximum number of records to return", ge=1, le=10000)
):
    """Get stock data from database."""
    try:
        # Handle both connection pool and direct connection
        if hasattr(db_connection_pool, 'getconn'):
            # Connection pool available
            conn = db_connection_pool.getconn()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Build query
                    query = """
                    SELECT
                        symbol,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        timestamp,
                        date,
                        hour,
                        daily_change,
                        price_direction
                    FROM stock_data
                """

                params = []

                # Add symbol filter if provided
                if symbol:
                    query += " WHERE symbol = %s"
                    params.append(symbol)

                # Order by timestamp descending and limit
                query += " ORDER BY timestamp DESC LIMIT %s"
                params.append(limit)

                cursor.execute(query, params)
                results = cursor.fetchall()

                # Convert to list of dicts
                data = [dict(row) for row in results]

                return JSONResponse(content={
                    "data": data,
                    "count": len(data),
                    "symbol": symbol,
                    "limit": limit
                })
            finally:
                db_connection_pool.putconn(conn)
        else:
            # Direct connection using stored parameters
            conn = psycopg2.connect(
                host=db_connection_pool['host'],
                port=db_connection_pool['port'],
                database=db_connection_pool['database'],
                user=db_connection_pool['user'],
                password=db_connection_pool['password']
            )
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Build query
                    query = """
                    SELECT
                        symbol,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        timestamp,
                        date,
                        hour,
                        daily_change,
                        price_direction
                    FROM stock_data
                """

                params = []

                # Add symbol filter if provided
                if symbol:
                    query += " WHERE symbol = %s"
                    params.append(symbol)

                # Order by timestamp descending and limit
                query += " ORDER BY timestamp DESC LIMIT %s"
                params.append(limit)

                cursor.execute(query, params)
                results = cursor.fetchall()

                # Convert to list of dicts
                data = [dict(row) for row in results]

                return JSONResponse(content={
                    "data": data,
                    "count": len(data),
                    "symbol": symbol,
                    "limit": limit
                })
            finally:
                conn.close()

    except Exception as e:
        logger.error("Failed to fetch stock data", extra={"error": str(e)})
        raise FinanceLakeException(
            message="Failed to fetch stock data",
            error_code="DATABASE_ERROR",
            status_code=500,
            details={"error": str(e)}
        )

# Analytics API
@app.get("/api/v1/analytics/portfolio/{portfolio_id}")
async def get_portfolio_analytics(portfolio_id: str):
    """Get portfolio analytics (placeholder for future ML service integration)."""
    return {
        "portfolio_id": portfolio_id,
        "analytics": {
            "total_value": 100000.0,
            "daily_return": 1.25,
            "volatility": 0.15,
            "sharpe_ratio": 1.8,
            "timestamp": datetime.now().isoformat()
        },
        "note": "This is a placeholder. ML service integration coming soon."
    }

@app.get("/api/v1/analytics/market/{symbol}")
async def get_market_analytics(symbol: str):
    """Get market analytics for a symbol."""
    # This would proxy to the ML service when implemented
    return {
        "symbol": symbol,
        "analytics": {
            "current_price": 150.25,
            "prediction": {
                "next_day": 152.10,
                "confidence": 0.78,
                "model": "LSTM"
            },
            "technical_indicators": {
                "rsi": 65.4,
                "macd": 0.85,
                "moving_average_20": 148.50
            },
            "timestamp": datetime.now().isoformat()
        },
        "note": "This is a placeholder. ML service integration coming soon."
    }

# ============================================================================
# WEBSOCKET ENDPOINTS
# ============================================================================

@app.websocket("/ws/market-data")
async def websocket_market_data(websocket: WebSocket):
    """WebSocket endpoint for real-time market data streaming."""
    await websocket.accept()
    active_connections.append(websocket)

    try:
        # Send welcome message
        await websocket.send_json({
            "type": "welcome",
            "message": "Connected to FinanceLake market data stream",
            "timestamp": datetime.now().isoformat()
        })

        # Subscribe to market data (placeholder for now)
        subscription_data = {
            "type": "subscription",
            "symbols": ["AAPL", "GOOGL", "MSFT"],
            "status": "active",
            "timestamp": datetime.now().isoformat()
        }
        await websocket.send_json(subscription_data)

        # Keep connection alive and echo messages
        while True:
            try:
                # Wait for messages from client (optional)
                data = await websocket.receive_text()

                # Echo back (could be subscription changes, etc.)
                await websocket.send_json({
                    "type": "echo",
                    "message": f"Received: {data}",
                    "timestamp": datetime.now().isoformat()
                })

            except Exception as e:
                logger.error("WebSocket error", extra={"error": str(e)})
                break

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error("WebSocket error", extra={"error": str(e)})
    finally:
        if websocket in active_connections:
            active_connections.remove(websocket)

# ============================================================================
# BROADCAST FUNCTIONS (for future real-time data)
# ============================================================================

async def broadcast_market_data(data: Dict[str, Any]):
    """Broadcast market data to all connected WebSocket clients."""
    disconnected_clients = []

    for connection in active_connections:
        try:
            await connection.send_json({
                "type": "market_data",
                "data": data,
                "timestamp": datetime.now().isoformat()
            })
        except Exception:
            disconnected_clients.append(connection)

    # Clean up disconnected clients
    for client in disconnected_clients:
        if client in active_connections:
            active_connections.remove(client)

# ============================================================================
# FRONTEND REDIRECT (for development)
# ============================================================================

@app.get("/app")
async def app_redirect():
    """Redirect to frontend application."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>FinanceLake</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                margin: 0;
                padding: 0;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                color: white;
            }
            .container {
                text-align: center;
                background: rgba(255, 255, 255, 0.1);
                padding: 2rem;
                border-radius: 10px;
                backdrop-filter: blur(10px);
            }
            h1 {
                margin-bottom: 1rem;
                font-size: 3rem;
            }
            p {
                margin-bottom: 2rem;
                font-size: 1.2rem;
            }
            .buttons {
                display: flex;
                gap: 1rem;
                justify-content: center;
            }
            .btn {
                padding: 0.75rem 1.5rem;
                background: white;
                color: #667eea;
                text-decoration: none;
                border-radius: 5px;
                font-weight: bold;
                transition: transform 0.2s;
            }
            .btn:hover {
                transform: translateY(-2px);
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 FinanceLake</h1>
            <p>Enterprise-Grade Real-Time Financial Analytics Platform</p>
            <div class="buttons">
                <a href="/docs" class="btn">📖 API Documentation</a>
                <a href="http://localhost:3000" class="btn" target="_blank">🎨 Frontend Dashboard</a>
                <a href="http://localhost:9090" class="btn" target="_blank">📊 Prometheus Metrics</a>
                <a href="http://localhost:3000" class="btn" target="_blank">📈 Grafana Dashboards</a>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

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
