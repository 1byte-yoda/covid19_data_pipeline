#!/bin/bash
set -e

echo "Creating Deltalake Tables"
python /create_deltalake.py

mc alias set minio_server http://"$MINIO_ENDPOINT_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc cp /data/country_mapping.csv minio_server/${DESTINATION__FILESYSTEM__BUCKET_URL#s3://}/covid19/static/country_mapping.csv --insecure

cd /dags

echo "Starting Dagster Webserver"
exec dagster dev -h 0.0.0.0
