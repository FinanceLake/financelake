import os
from dotenv import load_dotenv
from flask import Flask, request, jsonify, abort, make_response
import yfinance as yf
from flask_jwt_extended import (
    JWTManager, create_access_token,
    jwt_required, get_jwt_identity
)
from middleware import token_required  # importe ton middleware

# Charge les variables d'environnement depuis le fichier .env
load_dotenv()

app = Flask(__name__)

# Configuration de la clé secrète JWT depuis .env (ou valeur par défaut)
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY", "cle_jwt_par_defaut")

# Initialisation de JWT avec Flask
jwt = JWTManager(app)

# Gestionnaire d'erreur 401 (Unauthorized) pour renvoyer un JSON clair
@app.errorhandler(401)
def unauthorized(error):
    return make_response(jsonify({"error": str(error)}), 401)

# Fonction pour récupérer les données boursières via yfinance
def fetch_stock_data(symbol="Gold"):
    ticker = yf.Ticker(symbol)
    todays_data = ticker.history(period='1d')
    if todays_data.empty:
        return None
    latest = todays_data.iloc[-1]
    data = {
        "symbol": symbol,
        "date": latest.name.strftime("%Y-%m-%d"),
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
        "volume": int(latest["Volume"])
    }
    return data

# Route POST /login pour obtenir un token JWT (login simplifié)
@app.route('/login', methods=['POST'])
def login():
    username = request.json.get("username", None)
    password = request.json.get("password", None)

    if username != "admin" or password != "password123":
        return jsonify({"msg": "Nom d'utilisateur ou mot de passe incorrect"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)

# Route GET /data protégée par JWT : accès autorisé uniquement avec token valide
@app.route('/data', methods=['GET'])
@jwt_required()
def get_data():
    current_user = get_jwt_identity()
    symbol = request.args.get('symbol', 'Gold')
    data = fetch_stock_data(symbol)
    if not data:
        return jsonify({"error": "No data found"}), 404
    return jsonify(data)

# Nouvelle route protégée avec ton middleware personnalisé
@app.route('/protected')
@token_required
def protected_route(current_user):
    return jsonify({'message': f'Bienvenue {current_user}, tu es authentifié !'})

if __name__ == '__main__':
    app.run(debug=True)
