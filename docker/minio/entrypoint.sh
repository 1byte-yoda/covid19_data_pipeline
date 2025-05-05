#!/bin/sh
set -e

minio server --console-address ":9001" /data &
MINIO_PID=$!

echo "Waiting for MinIO to start..."
until mc alias set local http://$MINIO_ENDPOINT_URL "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null
do
  sleep 1
done

if mc admin accesskey list local | grep "$MINIO_ACCESS_KEY"; then
  echo "Access key '$MINIO_ACCESS_KEY' already exists. Skipping creation."
else
  echo "Creating access key '$MINIO_ACCESS_KEY'..."
  mc admin accesskey create local --access-key "$MINIO_ACCESS_KEY" --secret-key "$MINIO_SECRET_KEY"
  echo "Access key '$MINIO_ACCESS_KEY' created successfully."
fi

wait $MINIO_PID
