##!/bin/bash

# Initialize Superset database
superset db upgrade

# Create admin user
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@financelake.com \
    --password admin

# Initialize Superset
superset init

# Load examples (optional)
# superset load_examples

echo "Superset initialization completed!"