# Redis Cache Module for FinanceLake

This module provides a Redis-backed caching layer to optimize frequent stock data queries by caching results based on stock ticker and date range.

## Features

- Cache stock data for configurable TTL (default: 24 hours)
- Cache invalidation support for data refresh
- Metrics collection with Prometheus (cache hits, misses, lookup duration)
- Graceful fallback if Redis is unavailable

## Requirements

- Python 3.7+
- Redis server running (local or remote)
- Prometheus client library (`prometheus_client`)
- Docker and Docker Compose *(optional, for running Redis and Prometheus during tests)*


## Installation

```bash
pip install -r requirements.txt
```

Make sure Redis is running and accessible at the host and port specified in environment variables or defaults (localhost:6379).

## Usage

Import get_stock_data and invalidate_cache from redis_cache.py and pass your stock data fetching function as a callback.

 ## Quick Example

```python
from redis_cache import get_stock_data, invalidate_cache

def fetch_stock_from_api(ticker, start, end):
    # Your code to fetch data from API
    return {...}

data = get_stock_data("AAPL", "2024-01-01", "2024-01-31", fetch_stock_from_api)
print(data)
```

### redis-cache.py

The `redis_cache.py` file contains the core logic of the module:

- Initialization of the Redis cache
- Data retrieval functions (get_stock_data, etc.)
- Incrementation of Prometheus metrics

> **You don't have to run `redis_cache.py` directly.**
> It is automatically imported in files `test-cache.py` and `stock-producer.py`.



## To invalidate cache manually:

```python
invalidate_cache("AAPL", "2024-01-01", "2024-01-31")
```
## Metrics

Prometheus metrics are exposed at [http://localhost:8000/metrics](http://localhost:8000/metrics) by default.

## Environment Variables

- `REDIS_HOST` (default: localhost)
- `REDIS_PORT` (default: 6379)
- `REDIS_DB` (default: 0)
- `CACHE_TTL_SECONDS` (default: 86400)

## Testing

A sample test script `test_cache.py` is included to verify caching behavior.

### Run Tests

### Using Docker

If you want a quick and isolated setup, run Redis and Prometheus using Docker Compose:

Start Redis and Prometheus services using Docker Compose:

```bash
docker-compose up -d
```

Then run cache test logic (which simulates API fetches and exercises the cache):

```bash
python3 test_cache.py
```


### Testing stock-producer.py file 
run 
```bash
python3 stock-producer.py
```

### Viewing Metrics in Prometheus
You can view metrics at : [http://localhost:8000/metrics] 
or
Open Prometheus UI: [http://localhost:9090](http://localhost:9090)

#### 1. When running `test_cache.py`

To verify that cache metrics are collected correctly from the test module:

- Check Prometheus Target:
  - Go to **Status → Targets**
  - You should see `localhost:8000/metrics` marked as **UP**

- Explore the following metrics:
  - `cache_hits_total`
  - `cache_misses_total`
  - `cache_errors_total`
  - `cache_hits_created`
  - `cache_misses_created`

#### 2. When running `stock-producer.py`

To observe real stock request metrics:

- Again, ensure that `localhost:8000/metrics` is **UP** in the targets.

- Explore these additional metrics:
  - `stock_requests_total`
  - `stock_requests_success_total`
  - `stock_requests_failure_total`
  - `stock_requests_created`
  - `stock_requests_failure_created`

After you're done, you can stop the services:

```bash
docker-compose down
```


