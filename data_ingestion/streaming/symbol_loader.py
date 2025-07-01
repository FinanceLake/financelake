import pandas as pd
import logging

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def get_sp500_symbols():
    try:
        table = pd.read_html(WIKI_URL)
        df = table[0]
        symbols = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        logging.info(f"Chargé {len(symbols)} symboles depuis Wikipedia.")
        return symbols
    except Exception as e:
        logging.error(f"Erreur lors du chargement des symboles S&P500 : {e}")
        return []