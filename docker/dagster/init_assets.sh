#!/bin/bash
set -e

SRC_ALIAS="aws-s3"
DST_ALIAS="localminio"
SRC_BUCKET="covid19datahub-dataset"
DST_BUCKET=${DESTINATION__FILESYSTEM__BUCKET_URL#s3://}
SCHEMA="covid19"

mc alias set $DST_ALIAS http://"$MINIO_ENDPOINT_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc alias set $SRC_ALIAS https://s3-ap-southeast-1.amazonaws.com "" ""

datasets=("covid19datahub" "github_csse_daily" "static")

echo "Downloading ${datasets[@]}"

for prefix in ${datasets[@]}; do
  echo "$SRC_ALIAS/$SRC_BUCKET/$SCHEMA/$prefix/ to $DST_ALIAS/$DST_BUCKET/$SCHEMA/$prefix/"
  mc mirror "$SRC_ALIAS/$SRC_BUCKET/$SCHEMA/$prefix/" "$DST_ALIAS/$DST_BUCKET/$SCHEMA/$prefix/" --overwrite --insecure &

  [[ $(jobs -r -p | wc -l) -ge 2 ]] && wait -n
done

# Fix any broken downloads from above
mc mirror "$SRC_ALIAS/$SRC_BUCKET/$SCHEMA/" "$DST_ALIAS/$DST_BUCKET/$SCHEMA/" --overwrite --insecure

wait
echo "Copy S3 -> Local MinIO Complete."

echo "Synthetic BackFill Starting..."
dagster job launch -j synthetic_backfill_job

echo "BackFill Started..."
sleep 140

echo "BackFill Complete..."