# Kafka Ingestion Demo – FinanceLake

Ce dossier contient une démonstration fonctionnelle d'un pipeline d'ingestion temps réel avec **Kafka**, dans le cadre du projet FinanceLake.

## ⚙️ Architecture

- Kafka & Zookeeper tournent en local via Docker Compose  
- Producteur Python envoie un message dans un topic Kafka  
- Consommateur Python lit les messages du topic  

## 🧱 Prérequis

- Docker installé  
- Python 3 installé (`pip install kafka-python`)  
- Port `9092` disponible pour Kafka  

## 🚀 Lancer le projet

### 1. Démarrer les services Kafka & Zookeeper

```bash
docker-compose up -d
```

### 2. Vérifier que les conteneurs tournent

```bash
docker ps
```
Tu dois voir `kafka-test-kafka-1` et `kafka-test-zookeeper-1`.

### 3. Exécuter le producteur Kafka

```bash
python kafka_producer.py
```
Ce script envoie un message dans le topic Kafka (ex: `test-topic`).

### 4. Exécuter le consommateur Kafka

```bash
python kafka_consumer.py
```
Ce script lit les messages du topic Kafka.

### 5. Arrêter les services

```bash
docker-compose down
```

## 📁 Structure du projet

```
kafka-test/
├── assets/                # Captures d’écran et vidéos de démonstration
│   ├── kafka_producer.png
│   └── kafka_consumer.png
├── docker-compose.yml     # Configuration Docker Compose (Kafka + Zookeeper)
├── kafka_producer.py      # Script producteur Kafka en Python
├── kafka_consumer.py      # Script consommateur Kafka en Python
└── README.md              # Documentation du projet
```

## ✅ Étapes Git à suivre pour soumettre

```bash
git add kafka-test/assets/
git add kafka-test/README.md
git commit -m "Ajout du README et des captures de démonstration Kafka"
git push origin feature/abdourahim_ali
```

---

### 👍 Ce que tu dois faire maintenant :

1. Copier-coller tout ce contenu dans le fichier `README.md` dans le dossier `kafka-test/`.  
2. Mettre tes captures d’écran dans `kafka-test/assets/` (ex: `kafka_producer.png` et `kafka_consumer.png`).  
3. Remplacer les liens ou ajouter une vidéo si besoin.  
4. Faire ton commit & push.

---

🎉 Félicitations, tu as terminé la démonstration d’ingestion Kafka dans FinanceLake !
