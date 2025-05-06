#!/bin/bash
set -e

SRC_ALIAS="aws-s3"
DST_ALIAS="localminio"
SRC_BUCKET="covid19datahub-dataset"
DST_BUCKET=${DESTINATION__FILESYSTEM__BUCKET_URL#s3://}
SCHEMA="covid19"

mc alias set $DST_ALIAS http://"$MINIO_ENDPOINT_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc alias set $SRC_ALIAS https://s3-ap-southeast-1.amazonaws.com "" ""

datasets=("covid19datahub" "github_csse_daily")

echo "Downloading ${datasets[@]}"

MAX_RETRIES=5
RETRY_DELAY=5

for prefix in "${datasets[@]}"; do
  (
    attempt=1
    until mc mirror "$SRC_ALIAS/$SRC_BUCKET/$SCHEMA/$prefix/" "$DST_ALIAS/$DST_BUCKET/$SCHEMA/$prefix/" --overwrite --insecure; do
      if (( attempt >= MAX_RETRIES )); then
        echo "Failed to mirror $prefix after $MAX_RETRIES attempts."
        exit 1
      fi
      echo "Retrying $prefix in $RETRY_DELAY seconds (Attempt: $((++attempt))/$MAX_RETRIES)..."
      sleep $RETRY_DELAY
    done
  ) &

  # Limit concurrency to 2 jobs
  [[ $(jobs -r -p | wc -l) -ge 2 ]] && wait -n
done

wait
echo "Copy S3 -> Local MinIO Complete."

echo "Synthetic BackFill Starting..."
dagster job launch -j synthetic_backfill_job

echo "BackFill Started..."
sleep 140

echo "BackFill Complete..."