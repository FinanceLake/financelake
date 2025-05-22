#  FinanceLake - Ingestion de données boursières avec yfinance


---

##  Fonctionnalité principale

-  Récupération des données de marché pour un symbole donné (`AAPL`, `MSFT`, etc.)
-  Vérification que les champs clés sont présents (`Open`, `Close`, `Volume`, etc.)
-  Tests unitaires intégrés

---

##  Installation

### 1. Cloner le dépôt

git clone https://github.com/<ton-utilisateur>/financelake.git
cd financelake


 ### 2. Créer un environnement virtuel (recommandé)

python -m venv venv

### 3. Activer l’environnement

### Sous Windows :

venv\Scripts\activate


### Sous macOS/Linux :


source venv/bin/activate

### 4. Installer les dépendances
# Crée d’abord un fichier requirements.txt si ce n’est pas déjà fait :

yfinance
pytest
pytest-mock


pip install -r requirements.txt


#Structure du projet

financelake/
│
├── stock_ingestion.py               # Fonction fetch_stock_data()
├── tests/
│   └── test_stock_ingestion.py      # Test unitaire de l’ingestion
├── README.md                        # Ce fichier
├── requirements.txt                 # Dépendances du projet
└── .venv/                           # Environnement virtuel (à exclure du dépôt)


#Lancer les tests

#Dans la racine du projet :

pytest


#Tu devrais voir un message comme :

1 passed in 0.45s

