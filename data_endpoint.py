from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
from stock_data import load_stock_data

app = FastAPI()

@app.get("/data/stock/{symbol}")
def get_stock_data(
    symbol: str,
    from_date: str = Query(..., alias="from"),
    to_date: str = Query(..., alias="to")
):
    try:
        start_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Format de date invalide. Utilisez AAAA-MM-JJ.")

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="La date de début ne peut pas être postérieure à la date de fin.")

    data = load_stock_data(symbol.upper(), start_date, end_date)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Données introuvables pour le symbole {symbol.upper()}")

    return {
        "symbol": symbol.upper(),
        "data": data
    }
