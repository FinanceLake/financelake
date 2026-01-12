import socket
import json
import random
import time
from datetime import datetime

# --- Configuration ---
HOST = 'localhost'  # Run on your local machine
PORT = 9999         # The "doorway" number Spark will connect to
SYMBOLS = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA']

# --- Helper Function ---
def generate_stock_data():
    """Generates a random stock tick as a dictionary."""
    symbol = random.choice(SYMBOLS)
    price = round(random.uniform(100, 500), 2)
    volume = random.randint(100, 10000)
    timestamp = datetime.now().isoformat()
    
    return {
        'symbol': symbol,
        'price': price,
        'volume': volume,
        'timestamp': timestamp
    }

# --- Main Server Logic ---
print("Starting producer server...")
print(f"Waiting for a client to connect on port {PORT}...")

# Create the socket server
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))  # "Open the phone line" at this address
    s.listen()            # "Wait for a call"
    
    # "Pick up the call" when a client (Spark) connects
    conn, addr = s.accept() 
    
    with conn:
        print(f"Client connected: {addr}")
        print("Starting to send data...")
        
        try:
            while True:
                # 1. Generate new stock data
                stock_data = generate_stock_data()
                
                # 2. Convert to a JSON string
                # We add a newline (\n) because Spark uses it to 
                # separate one message from the next.
                data_string = json.dumps(stock_data) + '\n'
                
                # 3. Send the data (must be encoded as bytes)
                conn.sendall(data_string.encode('utf-8'))
                
                print(f"Sent: {stock_data}")
                
                # 4. Wait a moment to simulate a real-world flow
                time.sleep(random.uniform(0.5, 2.0))
                
        except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected. Stopping producer.")
        except KeyboardInterrupt:
            print("Stopping producer.")
        finally:
            print("Server shutting down.")