"""Bronze -> Silver: чтение сырого JSON из MinIO, нормализация, запись в Iceberg.

Таблицы (REST-каталог `rest`):
- rest.silver.weather_daily  (partitioned by day)
- rest.silver.exchange_rates (partitioned by day)

Запуск:
  spark-submit --master spark://spark-master:7077 spark_jobs/bronze_to_silver.py --date 2024-01-01
  или без --date — обработает все dt=* в bronze.

Логика:
- bronze/weather/dt=*/...json: либо Open-Meteo формат (hourly arrays),
  либо mock формат (hourly list of dicts) -> приводим к daily-агрегату.
- bronze/rates/dt=*/...json: курсы -> плоская таблица (currency, rate).
- Дедупликация, cast типов, обработка null.
- Iceberg: CREATE NAMESPACE IF NOT EXISTS + CREATE TABLE IF NOT EXISTS
  с PARTITIONED BY (day), затем MERGE INTO (идемпотентность).
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


def get_spark(app: str = "bronze-to-silver") -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret = os.getenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
    )
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = os.getenv("ICEBERG_S3_WAREHOUSE", "s3://warehouse/wh/")
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    return (
        SparkSession.builder.appName(app)
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
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )


BRONZE = "s3a://lakehouse/bronze"


def _bronze_paths(source: str, date: str | None) -> str:
    if date:
        return f"{BRONZE}/{source}/dt={date}/*.json"
    return f"{BRONZE}/{source}/dt=*/*.json"


def build_weather_daily(bronze_df: DataFrame) -> DataFrame:
    """Нормализует разные форматы bronze weather к daily-строкам.

    Ожидаемые колонки bronze: data (struct/json-string), ingested_at, source, ds.
    """
    df = bronze_df.withColumn("ds", F.coalesce(F.col("ds"), F.lit("1970-01-01")))
    # mock-формат: data.hourly = array<struct<hour,temp_c,humidity,pressure>>
    # open-meteo формат: data.hourly = struct<time:array, temperature_2m:array, ...>
    # Упрощённо: считаем из json-строки data, если struct — explode.
    # Для надёжности работаем через to_json + python-friendly выражения:
    cols = df.columns
    # Извлекаем ds как day
    out = df.select(
        F.col("ds").cast(StringType()).alias("day"),
        F.col("source").cast(StringType()).alias("source"),
        F.col("ingested_at").cast(StringType()).alias("ingested_at"),
        F.to_json(F.col("data")).alias("data_json"),
    )
    # Парсим mock-агрегаты через get_json_object
    # mock: {"city":..,"hourly":[{"temp_c":..,"humidity":..}, ...]}
    # Считаем средние на уровне json — проще развернуть через from_json с гибкой схемой:
    # Fallback: если распарсить не удалось — ставим null и фильтруем позже.
    out = (
        out.withColumn(
            "city",
            F.coalesce(
                F.get_json_object("data_json", "$.city"),
                F.get_json_object("data_json", "$.city"),
                F.lit("Moscow"),
            ),
        )
        .withColumn("avg_temp", F.get_json_object("data_json", "$.avg_temp").cast(DoubleType()))
        .withColumn("raw_hourly", F.get_json_object("data_json", "$.hourly"))
    )
    # Если avg_temp нет (сырые почасовые) — аппроксимируем: кол-во часов как прокси,
    # реальные средние посчитаем ниже через explode mock-массива.
    # Пробуем распарсить mock hourly array:
    hourly_schema = "array<struct<hour:int,temp_c:double,humidity:int,pressure:double>>"
    out = out.withColumn(
        "hourly_parsed", F.from_json(F.col("raw_hourly"), hourly_schema)
    )
    exploded = out.withColumn("h", F.explode_outer("hourly_parsed")).select(
        "day", "city", "source", "ingested_at",
        F.col("h.temp_c").alias("temp_c"),
        F.col("h.humidity").alias("humidity"),
        F.col("h.pressure").alias("pressure"),
    )
    daily_from_mock = (
        exploded.groupBy("day", "city")
        .agg(
            F.avg("temp_c").alias("avg_temp_c"),
            F.max("temp_c").alias("max_temp_c"),
            F.min("temp_c").alias("min_temp_c"),
            F.avg("humidity").alias("avg_humidity"),
            F.count("*").alias("n_obs"),
        )
        .withColumn("source", F.lit("weather"))
    )
    # Open-Meteo нативный формат: data.hourly.temperature_2m = array<double>
    # Достаём через json:
    native = out.select(
        "day",
        F.get_json_object("data_json", "$.hourly.temperature_2m").alias("t2m_json"),
        F.get_json_object("data_json", "$.hourly.relative_humidity_2m").alias("rh_json"),
    ).withColumn("t2m", F.from_json("t2m_json", "array<double>")).withColumn(
        "rh", F.from_json("rh_json", "array<int>")
    ).select(
        "day",
        F.expr("aggregate(t2m, 0D, (acc, x) -> acc + x) / size(t2m)").alias("avg_temp_c"),
        F.array_max("t2m").alias("max_temp_c"),
        F.array_min("t2m").alias("min_temp_c"),
        F.expr("aggregate(cast(rh as array<double>), 0D, (acc, x) -> acc + x) / size(rh)").alias("avg_humidity"),
        F.size("t2m").alias("n_obs"),
    ).withColumn("city", F.lit("Moscow")).withColumn("source", F.lit("weather"))
    # Объединяем: приоритет mock-агрегату, fallback native
    daily = daily_from_mock.unionByName(native, allowMissingColumns=True)
    daily = (
        daily.filter(F.col("day").isNotNull() & F.col("avg_temp_c").isNotNull())
        .dropDuplicates(["day", "city"])
        .withColumn("day", F.to_date("day"))
        .withColumn("avg_temp_c", F.col("avg_temp_c").cast(DoubleType()))
        .withColumn("max_temp_c", F.col("max_temp_c").cast(DoubleType()))
        .withColumn("min_temp_c", F.col("min_temp_c").cast(DoubleType()))
        .withColumn("avg_humidity", F.col("avg_humidity").cast(DoubleType()))
        .withColumn("n_obs", F.col("n_obs").cast(IntegerType()))
    )
    return daily.select(
        "day", "city", "avg_temp_c", "max_temp_c", "min_temp_c", "avg_humidity", "n_obs"
    )


def build_rates(bronze_df: DataFrame) -> DataFrame:
    """Нормализует bronze rates к (day, base, currency, rate)."""
    df = bronze_df.select(
        F.coalesce(F.col("ds"), F.lit("1970-01-01")).cast(StringType()).alias("day_str"),
        F.to_json(F.col("data")).alias("data_json"),
    )
    # mock: {"base":"USD","rates":{"EUR":..,"RUB":..}}
    # er-api: {"base_code":"USD","rates":{"EUR":..}}
    base = F.coalesce(
        F.get_json_object("data_json", "$.base"),
        F.get_json_object("data_json", "$.base_code"),
        F.lit("USD"),
    )
    rates_json = F.coalesce(
        F.get_json_object("data_json", "$.rates"),
        F.lit("{}"),
    )
    # Известные валюты — явно, чтобы схема была стабильной
    currencies = ["EUR", "RUB", "CNY", "GBP", "JPY", "KZT", "BYN"]
    cols = [F.get_json_object(rates_json, f"$.{c}").cast(DoubleType()).alias(c) for c in currencies]
    wide = df.select(F.to_date("day_str").alias("day"), base.alias("base"), *cols)
    long = wide.select(
        "day",
        "base",
        F.explode(
            F.array(*[F.struct(F.lit(c).alias("currency"), F.col(c).alias("rate")) for c in currencies])
        ).alias("kv"),
    ).select("day", "base", F.col("kv.currency").alias("currency"), F.col("kv.rate").alias("rate"))
    return long.filter(F.col("rate").isNotNull()).dropDuplicates(["day", "base", "currency"])


def ensure_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS rest.silver.weather_daily (
          day DATE, city STRING,
          avg_temp_c DOUBLE, max_temp_c DOUBLE, min_temp_c DOUBLE,
          avg_humidity DOUBLE, n_obs INT
        ) USING iceberg PARTITIONED BY (days(day))
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS rest.silver.exchange_rates (
          day DATE, base STRING, currency STRING, rate DOUBLE
        ) USING iceberg PARTITIONED BY (days(day))
        """
    )


def merge_weather(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("w_new")
    spark.sql(
        """
        MERGE INTO rest.silver.weather_daily t
        USING w_new s ON t.day = s.day AND t.city = s.city
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_rates(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("r_new")
    spark.sql(
        """
        MERGE INTO rest.silver.exchange_rates t
        USING r_new s ON t.day = s.day AND t.base = s.base AND t.currency = s.currency
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main(date: str | None = None) -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    # --- weather ---
    try:
        w_bronze = spark.read.option("multiLine", True).json(_bronze_paths("weather", date))
        # + local csv bronze (local_weather) подмешиваем как дополнительный источник
        if w_bronze.head(1):
            w_daily = build_weather_daily(w_bronze)
            print(f"[silver] weather rows: {w_daily.count()}")
            if w_daily.head(1):
                merge_weather(spark, w_daily)
                print("[silver] weather_daily MERGEd")
    except Exception as exc:
        print(f"[silver] weather failed: {exc}")

    # --- rates ---
    try:
        r_bronze = spark.read.option("multiLine", True).json(_bronze_paths("rates", date))
        if r_bronze.head(1):
            r_long = build_rates(r_bronze)
            print(f"[silver] rates rows: {r_long.count()}")
            if r_long.head(1):
                merge_rates(spark, r_long)
                print("[silver] exchange_rates MERGEd")
    except Exception as exc:
        print(f"[silver] rates failed: {exc}")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Bronze -> Silver (Iceberg)")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD, иначе все dt=*")
    args = ap.parse_args()
    main(args.date)
