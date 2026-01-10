import pandas as pd
from dashboard.database import get_db_connection

def get_available_symbols():
    con = get_db_connection()
    try:
        df = con.execute("SELECT DISTINCT symbol FROM gold ORDER BY symbol").df()
        return df['symbol'].tolist()
    except:
        return []

def get_market_data(symbol, limit=100):
    con = get_db_connection()
    try:
        query = f"""
            SELECT * FROM gold 
            WHERE symbol = '{symbol}' 
            ORDER BY window_end DESC 
            LIMIT {limit}
        """
        df = con.execute(query).df()
        if not df.empty:
            df['window_end'] = pd.to_datetime(df['window_end'])
            return df
        return None
    except:
        return None

def get_latest_prediction(symbol):
    con = get_db_connection()
    try:
        query = f"""
            SELECT signal, probability, window_end 
            FROM predictions 
            WHERE symbol = '{symbol}' 
            ORDER BY window_end DESC 
            LIMIT 1
        """
        df = con.execute(query).df()
        return df.iloc[0] if not df.empty else None
    except:
        return None