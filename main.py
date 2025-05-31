from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os
import pandas as pd
import requests

from utils import calculate_ema, min_max_normalize, remove_outliers

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_KEY_YAHOO")

# External data source
EXTERNAL_DATA_URL = "https://dummyjson.com/products"

app = FastAPI()

# --- AUTHENTICATION DEPENDENCY ---
def verify_api_key(request: Request):
    client_key = request.headers.get("x-api-key")
    if client_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

# --- SECURE ENDPOINT ---
@app.get("/transformed-data")
def transformed_data(request: Request = Depends(verify_api_key)):
    try:
        # Example: get dummy data from external API
        res = requests.get(EXTERNAL_DATA_URL)
        res.raise_for_status()
        data = res.json()
        prices = [item["price"] for item in data.get("products", [])]

        if len(prices) < 3:
            raise HTTPException(status_code=400, detail="Not enough data points.")

        # Apply transformation
        price_series = pd.Series(prices)
        return {
            "original": prices,
            "ema": calculate_ema(price_series, span=3).tolist(),
            "normalized": min_max_normalize(prices),
            "filtered": remove_outliers(prices)
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
