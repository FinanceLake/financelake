FROM python:3.10-slim

# Install Java and 'procps' (provides the 'ps' command Spark needs)
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    procps \
    && apt-get clean

WORKDIR /app

# Install dependencies
# ADDED: 'aiofiles' is required for FastAPI FileResponse
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    fastapi \
    uvicorn \
    pandas \
    aiofiles

# Copy your apps folder
COPY apps/ /app/apps/

# This helps Python find the files without an __init__.py
ENV PYTHONPATH="${PYTHONPATH}:/app/apps"

EXPOSE 8000

# Use uvicorn to run the app
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/app/apps"]