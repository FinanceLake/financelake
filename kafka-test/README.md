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

### 1. Lancer Kafka

```bash
docker-compose up -d
