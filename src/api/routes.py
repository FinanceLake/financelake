from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import logging
from src.services.data_service import stock_service  # Fixed import path

app = Flask(__name__)
logger = logging.getLogger(__name__)

@app.route('/')
def home():
    """API home endpoint"""
    return jsonify({
        "message": "FinanceLake Stock Data API",
        "version": "1.0.0",
        "endpoints": {
            "single_stock": "/api/stock/<ticker>",
            "batch_stocks": "/api/stocks/batch",
            "cache_stats": "/api/cache/stats",
            "invalidate_cache": "/api/cache/invalidate/<ticker>"
        },
        "status": "running"
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api/stock/<ticker>', methods=['GET'])
def get_stock_data(ticker):
    """Get stock data endpoint"""
    try:
        # Get query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        interval = request.args.get('interval', '1d')
        
        # Default to last 30 days if no dates provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Validate ticker
        ticker = ticker.upper()
        
        # Get data
        data = stock_service.get_stock_data(ticker, start_date, end_date, interval)
        
        if not data:
            return jsonify({"error": f"No data found for {ticker}"}), 404
        
        return jsonify({
            "success": True,
            "data": data,
            "cache_stats": stock_service.get_cache_stats()
        })
        
    except Exception as e:
        logger.error(f"API error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/stocks/batch', methods=['POST'])
def get_multiple_stocks():
    """Get multiple stocks data endpoint"""
    try:
        request_data = request.get_json()
        tickers = request_data.get('tickers', [])
        start_date = request_data.get('start_date')
        end_date = request_data.get('end_date')
        
        if not tickers:
            return jsonify({"error": "No tickers provided"}), 400
        
        # Default dates
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Get data for all tickers
        results = stock_service.get_multiple_stocks(tickers, start_date, end_date)
        
        return jsonify({
            "success": True,
            "data": results,
            "cache_stats": stock_service.get_cache_stats()
        })
        
    except Exception as e:
        logger.error(f"Batch API error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/cache/stats', methods=['GET'])
def get_cache_stats():
    """Get cache statistics"""
    return jsonify(stock_service.get_cache_stats())

@app.route('/api/cache/invalidate/<ticker>', methods=['DELETE'])
def invalidate_cache(ticker):
    """Invalidate cache for a specific ticker"""
    try:
        stock_service.invalidate_stock_cache(ticker.upper())
        return jsonify({"success": True, "message": f"Cache invalidated for {ticker}"})
    except Exception as e:
        logger.error(f"Cache invalidation error: {e}")
        return jsonify({"error": "Failed to invalidate cache"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)