#!/usr/bin/env bash
# Инициализация MinIO: создание бакетов lakehouse / warehouse.
set -euo pipefail

: "${MINIO_ROOT_USER:=minioadmin}"
: "${MINIO_ROOT_PASSWORD:=minioadmin123}"
: "${LAKEHOUSE_BUCKET:=lakehouse}"
: "${WAREHOUSE_BUCKET:=warehouse}"
ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"

echo "Waiting for MinIO at $ENDPOINT ..."
for i in $(seq 1 30); do
  if mc alias set minio "$ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

mc mb -p "minio/$LAKEHOUSE_BUCKET" || true
mc mb -p "minio/$WAREHOUSE_BUCKET" || true
mc anonymous set public "minio/$WAREHOUSE_BUCKET" || true

echo "Buckets ready:"
mc ls minio/
