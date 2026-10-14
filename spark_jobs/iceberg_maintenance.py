"""Iceberg maintenance: expire snapshots / rewrite data files.

Запуск:
  spark-submit --master spark://spark-master:7077 spark_jobs/iceberg_maintenance.py --table rest.silver.weather_daily
  spark-submit ... --table rest.silver.exchange_rates --expire-days 7

Операции:
- EXPIRE SNAPSHOTS старше N дней (освобождает S3, но убивает time travel глубже N).
- REWRITE DATA FILES (binpack) для мелких файлов из стриминга/MERGE.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret = os.getenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    )
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = os.getenv("ICEBERG_S3_WAREHOUSE", "s3://warehouse/wh/")
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    return (
        SparkSession.builder.appName("iceberg-maintenance")
        .master(master)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.catalog.rest.warehouse", warehouse)
        .config(
            "spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.rest.s3.endpoint", endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", key)
        .config("spark.sql.catalog.rest.s3.secret-access-key", secret)
        .getOrCreate()
    )


def main(table: str, expire_days: int = 7, rewrite: bool = True) -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[maint] table={table} expire_days={expire_days} rewrite={rewrite}")
    # История снапшотов до чистки
    try:
        spark.sql(f"SELECT * FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 20").show(20, truncate=False)
    except Exception as exc:
        print(f"[maint] snapshots read failed: {exc}")
    if expire_days > 0:
        # Удаляем старые снапшоты, оставляя минимум 5 последних
        # (time travel глубже expire_days станет недоступен — это и демонстрируем).
        try:
            spark.sql(
                f"CALL rest.system.expire_snapshots(table => '{table}', "
                f"retain_last => 5)"
            )
            print("[maint] expire_snapshots done (retain_last=5)")
        except Exception as exc:
            print(f"[maint] expire failed: {exc}")
    if rewrite:
        try:
            spark.sql(
                f"CALL rest.system.rewrite_data_files(table => '{table}', "
                f"strategy => 'binpack', sort_order => 'zorder(day)')"
            )
            print("[maint] rewrite_data_files done")
        except Exception as exc:
            print(f"[maint] rewrite failed (non-critical): {exc}")
    try:
        spark.sql(f"SELECT * FROM {table}.files LIMIT 20").show(20, truncate=False)
    except Exception as exc:
        print(f"[maint] files read failed: {exc}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--expire-days", type=int, default=7)
    ap.add_argument("--no-rewrite", action="store_true")
    a = ap.parse_args()
    main(a.table, a.expire_days, rewrite=not a.no_rewrite)
