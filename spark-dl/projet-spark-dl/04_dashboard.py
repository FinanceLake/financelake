# 04_dashboard.py
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import threading
import time
import json
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Données simulées pour le dashboard
dashboard_data = {
    'predictions': [],
    'alerts': [],
    'stats': {},
    'last_update': datetime.now().isoformat()
}

print("🌐 Dashboard démarré: http://localhost:5000")

@app.route('/')
def index():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>📈 Dashboard Trading GRU</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            }
            h1 {
                color: #333;
                text-align: center;
                margin-bottom: 30px;
            }
            .grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            .card {
                background: #f8f9fa;
                padding: 20px;
                border-radius: 10px;
                border-left: 5px solid #667eea;
            }
            .card h3 {
                color: #333;
                margin-top: 0;
            }
            .prediction {
                display: flex;
                justify-content: space-between;
                padding: 10px;
                margin: 5px 0;
                background: white;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            .trend-up { color: #28a745; }
            .trend-down { color: #dc3545; }
            .alert {
                background: #fff3cd;
                border-left: 4px solid #ffc107;
                padding: 15px;
                margin: 10px 0;
                border-radius: 5px;
            }
            .alert-buy {
                background: #d4edda;
                border-left-color: #28a745;
            }
            .alert-sell {
                background: #f8d7da;
                border-left-color: #dc3545;
            }
            .stats-box {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
                margin-top: 20px;
            }
            .stat {
                background: #667eea;
                color: white;
                padding: 15px;
                text-align: center;
                border-radius: 5px;
            }
            .stat-value {
                font-size: 24px;
                font-weight: bold;
            }
            .refresh {
                text-align: center;
                color: #666;
                margin-top: 20px;
            }
        </style>
        <script src="https://cdn.socket.io/4.5.0/socket.io.min.js"></script>
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                const socket = io();
                
                socket.on('data_update', function(data) {
                    updateDashboard(data);
                });
                
                function updateDashboard(data) {
                    // Mettre à jour les prédictions
                    const predictionsDiv = document.getElementById('predictions');
                    predictionsDiv.innerHTML = '';
                    
                    data.predictions.slice(-5).forEach(pred => {
                        const div = document.createElement('div');
                        div.className = 'prediction';
                        div.innerHTML = `
                            <div>
                                <strong>${pred.symbol}</strong><br>
                                <span class="${pred.trend === '📈 HAUSSE' ? 'trend-up' : 'trend-down'}">
                                    ${pred.trend}
                                </span>
                            </div>
                            <div style="text-align: right;">
                                $${pred.price.toFixed(2)}<br>
                                <small>${(pred.confidence * 100).toFixed(0)}%</small>
                            </div>
                        `;
                        predictionsDiv.appendChild(div);
                    });
                    
                    // Mettre à jour les alertes
                    const alertsDiv = document.getElementById('alerts');
                    alertsDiv.innerHTML = '';
                    
                    data.alerts.slice(-3).forEach(alert => {
                        const div = document.createElement('div');
                        div.className = 'alert ' + (alert.action === 'ACHAT' ? 'alert-buy' : 'alert-sell');
                        div.innerHTML = `
                            <strong>${alert.symbol} - ${alert.action}</strong><br>
                            Prix: $${alert.price.toFixed(2)}<br>
                            Confiance: ${(alert.confidence * 100).toFixed(0)}%<br>
                            <small>${alert.time}</small>
                        `;
                        alertsDiv.appendChild(div);
                    });
                    
                    // Mettre à jour les stats
                    document.getElementById('totalPred').textContent = data.stats.total_predictions || 0;
                    document.getElementById('avgConf').textContent = 
                        data.stats.avg_confidence ? (data.stats.avg_confidence * 100).toFixed(0) + '%' : 'N/A';
                    document.getElementById('alertsCount').textContent = data.alerts.length;
                    
                    // Dernière mise à jour
                    document.getElementById('lastUpdate').textContent = 
                        new Date().toLocaleTimeString();
                }
                
                // Rafraîchir toutes les 5 secondes
                setInterval(() => {
                    socket.emit('get_data');
                }, 5000);
            });
        </script>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Dashboard Trading - Prédictions GRU en Temps Réel</h1>
            
            <div class="stats-box">
                <div class="stat">
                    <div class="stat-value" id="totalPred">0</div>
                    <div>Prédictions</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="avgConf">0%</div>
                    <div>Confiance Moyenne</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="alertsCount">0</div>
                    <div>Alertes Actives</div>
                </div>
            </div>
            
            <div class="grid">
                <div class="card">
                    <h3>📊 Dernières Prédictions</h3>
                    <div id="predictions">
                        <div class="prediction">
                            <div>Chargement...</div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>🚨 Alertes de Trading</h3>
                    <div id="alerts">
                        <div class="alert">
                            En attente de données...
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="refresh">
                Dernière mise à jour: <span id="lastUpdate">--:--:--</span>
                <br>
                <small>Rafraîchissement automatique toutes les 5 secondes</small>
            </div>
        </div>
    </body>
    </html>
    """

@socketio.on('connect')
def handle_connect():
    print(f"🔗 Client connecté")
    socketio.emit('data_update', dashboard_data)

@socketio.on('get_data')
def handle_get_data():
    # Simuler des mises à jour
    import random
    
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
    
    # Ajouter une prédiction simulée
    symbol = random.choice(symbols)
    prediction = {
        'symbol': symbol,
        'price': random.uniform(100, 4000),
        'trend': random.choice(['📈 HAUSSE', '📉 BAISSE']),
        'confidence': random.uniform(0.5, 0.95),
        'timestamp': datetime.now().isoformat()
    }
    
    dashboard_data['predictions'].append(prediction)
    if len(dashboard_data['predictions']) > 10:
        dashboard_data['predictions'] = dashboard_data['predictions'][-10:]
    
    # Ajouter une alerte si confiance > 0.8
    if prediction['confidence'] > 0.8:
        alert = {
            'symbol': symbol,
            'action': 'ACHAT' if 'HAUSSE' in prediction['trend'] else 'VENTE',
            'price': prediction['price'],
            'confidence': prediction['confidence'],
            'time': datetime.now().strftime('%H:%M:%S')
        }
        dashboard_data['alerts'].append(alert)
        if len(dashboard_data['alerts']) > 5:
            dashboard_data['alerts'] = dashboard_data['alerts'][-5:]
    
    # Mettre à jour les stats
    dashboard_data['stats'] = {
        'total_predictions': len(dashboard_data['predictions']),
        'avg_confidence': np.mean([p['confidence'] for p in dashboard_data['predictions']]) 
                          if dashboard_data['predictions'] else 0,
        'active_alerts': len([a for a in dashboard_data['alerts'] if a['confidence'] > 0.7])
    }
    
    dashboard_data['last_update'] = datetime.now().isoformat()
    
    socketio.emit('data_update', dashboard_data)

if __name__ == '__main__':
    print("\n✅ Dashboard prêt!")
    print("📊 Accédez à: http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
