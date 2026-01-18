FROM apache/spark:3.5.0
USER root

RUN pip install --no-cache-dir \
    delta-spark==3.1.0 --no-deps \
    pandas \
    numpy

WORKDIR /app

# Copy the entire folder so 'apps' exists as a package
COPY apps/ /app/apps/

# Crucial: Tell Python to look in /app to find the 'apps' folder
ENV PYTHONPATH="${PYTHONPATH}:/app"
