from flask import Flask, render_template, request, jsonify
import json
from collections import deque, defaultdict
from datetime import datetime
import threading
import time
import os
import glob

app = Flask(__name__)

# Stockage en mémoire des données
stock_data = {}           # Données agrégées de Spark
raw_stock_data = deque(maxlen=100)  # Données brutes de Kafka Producer
price_history = {}
MAX_HISTORY = 50

# Data output directory (must match Spark consumer settings)
DATA_OUTPUT_DIR = os.getenv('DATA_OUTPUT_DIR', './data_output')

# Cache for bronze/silver/gold files with timestamps
bronze_cache = {}  # {symbol: {data, timestamp}}
silver_cache = defaultdict(list)  # {symbol: [{data}, ...]}
gold_cache = defaultdict(list)  # {symbol: [{data}, ...]}
last_scan_time = {'bronze': 0, 'silver': 0, 'gold': 0}

@app.route('/')
def index():
    return render_template('enhanced.html')

@app.route('/api/stock-stats', methods=['POST'])
def receive_stock_stats():
    """Endpoint pour recevoir les statistiques agrégées de Spark"""
    try:
        data = request.get_json()
        
        symbol = data.get('symbol')
        if not symbol:
            return jsonify({'error': 'Symbol manquant'}), 400
        
        print(f"📊 Données agrégées reçues: {symbol} - Prix: ${data.get('avg_price', 'N/A')}")
        
        # Initialisation des structures de données pour le symbole
        if symbol not in stock_data:
            stock_data[symbol] = {}
            price_history[symbol] = deque(maxlen=MAX_HISTORY)
        
        # Mise à jour des données agrégées
        stock_data[symbol] = {
            'symbol': symbol,
            'avg_price': round(data.get('avg_price', 0), 2),
            'volatility': round(data.get('volatility', 0), 2),
            'min_price': round(data.get('min_price', 0), 2),
            'max_price': round(data.get('max_price', 0), 2),
            'avg_volume': int(data.get('total_volume', 0) / max(1, data.get('data_points', 1))),  # Calculated from total
            'total_volume': int(data.get('total_volume', 0)),
            'data_points': data.get('data_points', 0),
            'avg_high': round(data.get('max_price', 0), 2),  # Use max_price as proxy
            'avg_low': round(data.get('min_price', 0), 2),   # Use min_price as proxy
            'window_start': data.get('start'),
            'window_end': data.get('end'),
            'last_update': datetime.now().isoformat(),
            'data_type': 'aggregated'
        }
        
        # Ajout à l'historique des prix
        price_history[symbol].append({
            'timestamp': data.get('start'),
            'price': round(data.get('avg_price', 0), 2),
            'volatility': round(data.get('volatility', 0), 2)
        })
        
        return jsonify({'status': 'success'}), 200
        
    except Exception as e:
        print(f"Erreur lors de la réception des données agrégées: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/raw-stock-data', methods=['POST'])
def receive_raw_stock_data():
    """Endpoint pour recevoir les données brutes du Kafka Producer"""
    try:
        data = request.get_json()
        
        # Ajout du timestamp de réception
        data['web_received_at'] = datetime.now().isoformat()
        data['data_type'] = 'raw'
        
        # Ajout à la file des données brutes
        raw_stock_data.append(data)
        
        print(f"📨 Donnée brute reçue: {data['symbol']} - ${data['price']}")
        
        return jsonify({'status': 'success'}), 200
        
    except Exception as e:
        print(f"Erreur lors de la réception des données brutes: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stocks')
def get_stocks():
    """Endpoint pour récupérer toutes les données boursières"""
    return jsonify({
        'current_data': stock_data,
        'raw_data': list(raw_stock_data),
        'price_history': {symbol: list(history) for symbol, history in price_history.items()}
    })

@app.route('/api/raw-data')
def get_raw_data():
    """Endpoint pour récupérer uniquement les données brutes"""
    return jsonify({
        'raw_data': list(raw_stock_data),
        'total_count': len(raw_stock_data)
    })

@app.route('/api/aggregated-data')
def get_aggregated_data():
    """Endpoint pour récupérer uniquement les données agrégées"""
    return jsonify({
        'aggregated_data': stock_data
    })


@app.route('/api/stock-series', methods=['GET'])
def get_stock_series():
    """Retourne deux séries: price_series et volatility_series pour un symbole donné.

    Query params:
      - symbol: symbole boursier (par défaut 'GOOGL')
      - limit: nombre maximum de points à retourner (optionnel)
    """
    try:
        symbol = request.args.get('symbol', 'GOOGL')
        limit = request.args.get('limit')

        if symbol not in price_history:
            return jsonify({'error': f"Symbol '{symbol}' not found"}), 404

        series = list(price_history[symbol])
        if limit:
            try:
                lim = int(limit)
                if lim > 0:
                    series = series[-lim:]
            except Exception:
                pass

        # Transformer en deux séries séparées avec {timestamp, value}
        price_series = [{'timestamp': p.get('timestamp'), 'value': p.get('price')} for p in series]
        volatility_series = [{'timestamp': p.get('timestamp'), 'value': p.get('volatility')} for p in series]

        return jsonify({
            'symbol': symbol,
            'price_series': price_series,
            'volatility_series': volatility_series,
            'points_returned': len(series)
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def load_json_files_from_directory(directory_path):
    """Load all JSON files from a directory and return combined data."""
    data = []
    if not os.path.exists(directory_path):
        return data
    
    try:
        json_files = sorted(glob.glob(os.path.join(directory_path, '*.json')), 
                          key=os.path.getctime, reverse=True)[:20]  # Load last 20 files
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    file_data = json.load(f)
                    if isinstance(file_data, list):
                        data.extend(file_data)
                    else:
                        data.append(file_data)
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {e}")
    except Exception as e:
        print(f"Error scanning directory {directory_path}: {e}")
    
    return data


def get_latest_gold_kpis():
    """Load latest gold KPI files and organize by symbol."""
    kpi_data = defaultdict(list)
    gold_dir = os.path.join(DATA_OUTPUT_DIR, 'gold')
    
    if not os.path.exists(gold_dir):
        return kpi_data
    
    try:
        json_files = sorted(glob.glob(os.path.join(gold_dir, '*.json')), 
                          key=os.path.getctime, reverse=True)[:50]  # Last 50 files
        
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    records = json.load(f)
                    if isinstance(records, list):
                        for record in records:
                            if 'symbol' in record:
                                kpi_data[record['symbol']].append(record)
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {e}")
    except Exception as e:
        print(f"Error scanning gold directory: {e}")
    
    return kpi_data


@app.route('/api/bronze-data')
def get_bronze_data():
    """Get latest bronze (raw ingested) data grouped by symbol."""
    try:
        bronze_dir = os.path.join(DATA_OUTPUT_DIR, 'bronze')
        data = load_json_files_from_directory(bronze_dir)
        
        # Group by symbol
        by_symbol = defaultdict(list)
        for record in data:
            if 'symbol' in record:
                by_symbol[record['symbol']].append(record)
        
        return jsonify({
            'bronze_data': dict(by_symbol),
            'total_records': len(data),
            'symbols': list(by_symbol.keys())
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/silver-data')
def get_silver_data():
    """Get latest silver (cleaned) data."""
    try:
        silver_dir = os.path.join(DATA_OUTPUT_DIR, 'silver')
        data = load_json_files_from_directory(silver_dir)
        
        # Group by symbol
        by_symbol = defaultdict(list)
        for record in data:
            if 'symbol' in record:
                by_symbol[record['symbol']].append(record)
        
        return jsonify({
            'silver_data': dict(by_symbol),
            'total_records': len(data),
            'symbols': list(by_symbol.keys())
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/gold-data')
def get_gold_data():
    """Get latest gold (KPI) data."""
    try:
        kpi_data = get_latest_gold_kpis()
        
        # Convert defaultdict to regular dict for JSON serialization
        gold_data = {symbol: records for symbol, records in kpi_data.items()}
        
        return jsonify({
            'gold_data': gold_data,
            'symbols': list(gold_data.keys()),
            'total_records': sum(len(records) for records in gold_data.values())
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/debug-html')
def debug_html():
        """Simple debug page to verify the browser renders HTML/CSS correctly."""
        html = """
        <!doctype html>
        <html>
            <head>
                <meta charset='utf-8'/>
                <title>Debug Render</title>
                <style>body{font-family:Arial,sans-serif;padding:24px;background:#fff;color:#111} .ok{color:green;font-weight:700}</style>
            </head>
            <body>
                <h1>Debug Render</h1>
                <p class='ok'>If you see styled HTML, the server and browser render HTML correctly.</p>
                <p>Visit <a href='/'>/</a> to load the main dashboard.</p>
            </body>
        </html>
        """
        return html

@app.route('/api/clear-raw-data', methods=['POST'])
def clear_raw_data():
    """Endpoint pour vider les données brutes"""
    try:
        raw_stock_data.clear()
        return jsonify({'status': 'success', 'message': 'Données brutes vidées'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def cleanup_old_data():
    """Nettoyage périodique des anciennes données"""
    while True:
        time.sleep(60)  # Toutes les minutes
        current_time = datetime.now()
        
        # Nettoyage des données agrégées
        for symbol in list(stock_data.keys()):
            if symbol in stock_data and 'last_update' in stock_data[symbol]:
                last_update = datetime.fromisoformat(stock_data[symbol]['last_update'])
                if (current_time - last_update).total_seconds() > 300:  # 5 minutes
                    del stock_data[symbol]
                    if symbol in price_history:
                        del price_history[symbol]

if __name__ == '__main__':
    # Démarrage du thread de nettoyage
    cleanup_thread = threading.Thread(target=cleanup_old_data, daemon=True)
    cleanup_thread.start()
    
    # Run the app without the reloader in this dev environment to avoid
    # the 'Bad file descriptor' error that can happen with the reloader
    # and the way the process is launched by the orchestration scripts.
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)