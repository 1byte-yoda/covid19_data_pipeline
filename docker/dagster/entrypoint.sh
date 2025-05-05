#!/bin/bash

echo "Creating Deltalake Tables"
python /create_deltalake.py

cd /dags

echo "Starting Dagster Webserver"
exec dagster dev -h 0.0.0.0
