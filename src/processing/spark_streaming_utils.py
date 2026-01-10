import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/spark_streaming.log', mode='w'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_sql_queries(view_name="stock_analytics"):
    return {
        "stats": f"""
        SELECT symbol,
               AVG(avg_price) as overall_avg_price,
               AVG(volatility) as avg_volatility,
               SUM(total_volume) as cumulative_volume,
               COUNT(*) as window_count
        FROM {view_name}
        GROUP BY symbol
        ORDER BY avg_volatility DESC
        """,
        "movers": f"""
        SELECT symbol, window_start, window_end, price_change_pct, avg_price
        FROM {view_name}
        WHERE ABS(price_change_pct) > 1.0
        ORDER BY ABS(price_change_pct) DESC
        LIMIT 10
        """,
        "volatility": f"""
        SELECT symbol, window_start, volatility, avg_price
        FROM {view_name}
        WHERE volatility > 2.0
        ORDER BY volatility DESC
        """
    }