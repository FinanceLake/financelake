# Use official Python slim image for a smaller footprint
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./

# Expose ports for dashboard and backend
EXPOSE 8050 8000

# Command will be specified in docker-compose.yml
CMD ["python3", "stock_dashboard.py"]