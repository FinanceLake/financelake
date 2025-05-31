import threading
import time
import random
from datetime import datetime

class IngestionSimulator:
    def __init__(self):
        self.status = "Paused"
        self.last_updated = None
        self.errors = []
        self._running = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            self.status = "Running"
            self._running = True
        threading.Thread(target=self._run, daemon=True).start()

    def pause(self):
        with self._lock:
            self.status = "Paused"
            self._running = False

    def _run(self):
        while True:
            with self._lock:
                if not self._running:
                    time.sleep(1)
                    continue

                # Simulate update
                self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Simulate errors randomly
                if random.random() < 0.1:  # 10% chance of error
                    error_msg = f"[{self.last_updated}] API timeout or Kafka error."
                    self.errors.append(error_msg)
                    self.status = "Error"
                else:
                    self.status = "Running"

            time.sleep(2)  # Simulate an ingestion cycle every 2s

    def get_status(self):
        with self._lock:
            return {
                "status": self.status,
                "last_updated": self.last_updated or "Never",
                "errors": self.errors[-5:]  # Last 5 errors
            }

# Global singleton
simulator = IngestionSimulator()
