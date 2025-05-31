import requests
import json
import time
import random

URL = "http://localhost:8090/contentListener"  #   ListenHTTP port

stocks = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA']

while True:
    data = {
        "symbol": random.choice(stocks),
        "price": round(random.uniform(100, 1500), 2),
        "volume": random.randint(1000, 1000000)
    }

    try:
        response = requests.post(URL, json=data)
        print(f"Sent: {data} | Status: {response.status_code}")
    except Exception as e:
        print(f"Error sending data: {e}")

    time.sleep(2)  # Wait 2 seconds before sending next
