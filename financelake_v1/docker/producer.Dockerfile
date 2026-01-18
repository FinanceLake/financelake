FROM python:3.10-slim

# Set the working directory to /app
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir yfinance kafka-python pandas

# DO NOT copy the file directly to /app. 
# We keep the structure consistent with our volumes.
COPY apps/ /app/apps/

# Set the Python Path so it can find everything
ENV PYTHONPATH=/app

# Run the script from its actual location
CMD ["python", "apps/producer.py"]