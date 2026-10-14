#!/usr/bin/env python3
"""Создание namespaces и пустых Iceberg-таблиц через Spark SQL.

Используется в `make init` для идемпотентной инициализации каталога
до первого запуска DAG. Работает внутри сети docker (iceberg-rest:8181,
minio:9000) при запуске через spark-submit или plain python с pyspark.

Пример:
    docker compose exec spark-master spark-submit /home/iceberg/spark_jobs/../scripts/register_iceberg_tables.py
    python scripts/register_iceberg_tables.py  # локально при проброшенных портах
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))

from bronze_to_silver import ensure_tables, get_spark  # noqa: E402


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    for tbl in ("rest.silver.weather_daily", "rest.silver.exchange_rates"):
        try:
            spark.sql(f"SELECT count(*) FROM {tbl}").show()
            print(f"[init] OK {tbl}")
        except Exception as exc:
            print(f"[init] {tbl}: {exc}")
    # gold-схемы создаются Trino/dbt, но создадим заранее чтобы Trino их видел
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.staging")
    spark.stop()


if __name__ == "__main__":
    main()
