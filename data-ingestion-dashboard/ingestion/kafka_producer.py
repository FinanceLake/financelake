from kafka import KafkaProducer
import json, yfinance as yf, uuid, time, itertools
import logging

logging.basicConfig(
    filename='producer_error.log',  
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s:%(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN"]

data = {}
for ticker in TICKERS:
    df = yf.Ticker(ticker).history(period="90d", interval="1d")
    data[ticker] = df.reset_index()

for i in itertools.cycle(range(90)):
    for ticker in TICKERS:
        try:
            row = data[ticker].iloc[i].to_dict()
            row["ticker"] = ticker
            row["transaction_id"] = str(uuid.uuid4())
            row["event_time"] = int(row["Date"].timestamp() * 1000)  
            del row["Date"]
            
            producer.send("finance", row)
            print("Envoyé:", row)
        
        except Exception as e:
            error_msg = f"Error sending data for ticker {ticker} at index {i}: {e}"
            print(error_msg)
            logging.error(error_msg)
    time.sleep(1)
