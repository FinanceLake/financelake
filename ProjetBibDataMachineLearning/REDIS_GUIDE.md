# Guide d'utilisation - Système de Prédiction AMZN en Temps Réel avec Redis

## 📋 Architecture du Système

```
Redis Server
    ↓
    ├─→ Producer (redis_producer.py)
    │   └─ Génère les prix AMZN en temps réel
    │
    └─→ Consumer (redis_consumer.py)
        └─ Reçoit les prix et prédit avec LSTM
```

## 🚀 Démarrage Rapide

### Étape 1 : Vérifier que Redis est en cours d'exécution

Redis doit être démarré avant tout. Vous verrez quelque chose comme :
```
                _._
           _.-``__ ''-._
      _.-``    `.  `_.  ''-._           Redis 7.0.x
  .-`` .-```.  ```\/    _.,_ ''-._
 (    '      ,       .-`  | `,    )
 |`-._`-...-` __...-.``-._|'` _.-'|
 |    `-._   `._    /     _.-'    |
  `-._    `-._  `-./  _.-'    _.-'
```

### Étape 2 : Vérifier que le modèle LSTM existe

Assurez-vous que vous avez entraîné le modèle :
```powershell
python main.py
```

Cela crée le fichier: `data/gold/lstm_amzn_model.h5`

### Étape 3 : Lancer le système

Ouvrez **deux terminaux PowerShell** différents:

**Terminal 1 - Producteur (génère les prix):**
```powershell
cd C:\Users\hp\Desktop\projet\ProjetBibData
python -m src.redis_producer
```

**Terminal 2 - Consommateur (prédit les prix):**
```powershell
cd C:\Users\hp\Desktop\projet\ProjetBibData
python -m src.redis_consumer
```

---

## 📦 Fichiers créés

### 1. `src/redis_producer.py`
Classe `RedisProducer`:
- Génère des prix AMZN réalistes
- Publie sur le channel Redis: `amzn_prices`
- Utilise un mouvement brownien géométrique pour la réalisme

**Utilisation:**
```python
from src.redis_producer import RedisProducer

producer = RedisProducer()
producer.stream_batch(num_messages=10, interval=2)
```

### 2. `src/redis_consumer.py`
Classe `RedisConsumer`:
- Consomme les prix depuis Redis
- Prédit le prix suivant avec LSTM
- Sauvegarde les prédictions en JSON

**Utilisation:**
```python
from src.redis_consumer import RedisConsumer

consumer = RedisConsumer()
consumer.consume_messages(max_messages=10)
```

### 3. `run_realtime.py`
Script d'orchestration qui:
- Lance producteur et consommateur
- Gère les threads
- Vérifie que tout fonctionne

**Utilisation:**
```powershell
python run_realtime.py
```

### 4. `start_redis.py`
Script helper pour:
- Vérifier si Redis est en cours d'exécution
- Démarrer Redis automatiquement si possible

**Utilisation:**
```powershell
python start_redis.py
```

---

## 🔧 Configuration

Les paramètres se trouvent dans `config.py`:

```python
LOOKBACK = 60          # Nombre de jours pour les features
BATCH_SIZE = 32        # Taille des batches
EPOCHS = 50            # Nombre d'entraînements
```

---

## 📊 Résultats

Les prédictions sont sauvegardées dans:
```
data/gold/redis_predictions.json
```

Format:
```json
[
  {
    "timestamp": "2026-01-08T10:30:45.123456",
    "current_price": 150.32,
    "predicted_price": 150.85,
    "change": 0.53,
    "percent_change": 0.35
  }
]
```

---

## ⚠️ Troubleshooting

### Redis n'est pas disponible
```powershell
"C:\Program Files\Redis\redis-server.exe"
```

### Le modèle LSTM n'existe pas
```powershell
python main.py
```

### Erreur de connexion Redis
Vérifiez que Redis est démarré:
```powershell
& "C:\Program Files\Redis\redis-cli.exe" PING
# Doit retourner: PONG
```

---

## 💡 Cas d'utilisation

1. **Streaming en temps réel**: Recevoir des prix en continu
2. **Prédictions instantanées**: Générer des prédictions dès réception du prix
3. **Monitoring**: Surveiller les changements de prix
4. **Intégration**: Connecter à d'autres systèmes via Redis

---

## 🔄 Workflow complet

1. Entraîner le modèle: `python main.py`
2. Démarrer Redis: `redis-server.exe` (si pas déjà lancé)
3. Lancer le producteur: `python -m src.redis_producer`
4. Lancer le consommateur: `python -m src.redis_consumer`
5. Consulter les résultats: `data/gold/redis_predictions.json`

---

## 📈 Prochaines étapes

- Ajouter une base de données (PostgreSQL) pour historique
- Créer un dashboard en temps réel (Dash/Streamlit)
- Intégrer des données réelles (Yahoo Finance en streaming)
- Ajouter de multiples symboles (MSFT, GOOGL, etc.)
