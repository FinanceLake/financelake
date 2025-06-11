import os
import pandas as pd

def load_stock_data(symbol, start_date, end_date):
    # Chemin absolu vers le dossier des fichiers
    base_path = os.path.join(os.path.dirname(__file__), "data", "stock")

    # Liste les fichiers CSV qui commencent par le symbole donné
    matching_files = [
        f for f in os.listdir(base_path)
        if f.startswith(symbol) and f.endswith(".csv")
    ]

    if not matching_files:
        raise Exception(f"404: Aucune donnée trouvée pour le symbole '{symbol}'")

    file_path = os.path.join(base_path, matching_files[0])

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise Exception(f"Erreur de lecture du fichier : {e}")

    # Normalise les noms de colonnes
    df.columns = [col.strip().lower() for col in df.columns]

    # Vérifie que la colonne 'date' existe
    if 'date' not in df.columns:
        raise Exception("Colonne 'date' manquante dans le fichier CSV.")

    # Convertit la colonne 'date' en datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Filtre les données par date
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    df_filtered = df.loc[mask]

    if df_filtered.empty:
        raise Exception("Aucune donnée disponible dans la plage de dates spécifiée.")

    # Retourne les données filtrées au format dictionnaire
    return df_filtered.to_dict(orient='records')
