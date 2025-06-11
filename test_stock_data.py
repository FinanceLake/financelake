from datetime import datetime
from stock_data import load_stock_data

def test_load_stock_data():
    symbol = "AABA"  # adapte selon ton fichier (ex: AABA_2006-01-01_to_2018-01-01.xls)
    start_date = datetime(2007, 1, 1)
    end_date = datetime(2007, 1, 10)

    try:
        data = load_stock_data(symbol, start_date, end_date)
        print(f"Données chargées pour {symbol} du {start_date.date()} au {end_date.date()}:")
        for row in data:
            print(row)
    except Exception as e:
        print(f"Erreur: {e}")

if __name__ == "__main__":
    test_load_stock_data()
