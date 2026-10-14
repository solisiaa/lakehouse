"""DAG bronze-ингестии: REST API + локальные CSV -> MinIO (s3://lakehouse/bronze).

Слои medallion:
- bronze: сырые данные «как есть» + метаданные ingestion (ingested_at, source).

Источники:
1. Open-Meteo (погода, без API-ключа): https://api.open-meteo.com/v1/forecast
2. Exchange rates (https://open.er-api.com/v6/latest/USD, без ключа)
3. Локальные CSV из /opt/airflow/data/raw/

MOCK_MODE=true (по умолчанию из .env) — генерирует детерминированные
синтетические данные, проект полностью работает офлайн.
При MOCK_MODE=false — tries real API, при ошибке fallback к mock.

Расписание: ежедневно @daily. Ручной запуск тоже поддерживается.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import requests
from airflow.decorators import dag, task

# ---------------------------------------------------------------------------
# Конфиг через env (см. .env.example)
# ---------------------------------------------------------------------------
S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_REGION: str = os.getenv("S3_REGION", "us-east-1")
AWS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
AWS_SECRET: str = os.getenv(
    "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
)
BUCKET: str = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
MOCK_MODE: bool = os.getenv("MOCK_MODE", "true").lower() in ("1", "true", "yes")
LAT: float = float(os.getenv("OPEN_METEO_LAT", "55.75"))
LON: float = float(os.getenv("OPEN_METEO_LON", "37.61"))
EXCHANGE_URL: str = os.getenv(
    "EXCHANGE_API_URL", "https://open.er-api.com/v6/latest/USD"
)

RAW_DIR = Path("/opt/airflow/data/raw")


def _s3_client():  # type: ignore[no-untyped-def]
    """S3-клиент, совместимый с MinIO (path-style)."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=S3_REGION,
    )


def _put_json(key: str, payload: dict[str, Any]) -> str:
    s3 = _s3_client()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    return f"s3://{BUCKET}/{key}"


def _mock_weather(ds: str) -> dict[str, Any]:
    rng = random.Random(hash(ds) % (2**32))
    hours = []
    for h in range(24):
        hours.append(
            {
                "hour": h,
                "temp_c": round(rng.uniform(-8, 5), 1),
                "humidity": rng.randint(60, 95),
                "pressure": round(rng.uniform(735, 760), 1),
            }
        )
    return {
        "data": {
            "city": "Moscow",
            "lat": LAT,
            "lon": LON,
            "date": ds,
            "hourly": hours,
        },
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "source": "open-meteo-mock",
        "ds": ds,
    }


def _mock_rates(ds: str) -> dict[str, Any]:
    rng = random.Random((hash(ds + "rates")) % (2**32))
    return {
        "data": {
            "base": "USD",
            "date": ds,
            "rates": {
                "EUR": round(rng.uniform(0.88, 0.95), 4),
                "RUB": round(rng.uniform(85, 95), 2),
                "CNY": round(rng.uniform(7.0, 7.4), 2),
                "GBP": round(rng.uniform(0.78, 0.82), 4),
            },
        },
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "source": "exchange-mock",
        "ds": ds,
    }


def _fetch_weather(ds: str) -> dict[str, Any]:
    """Тянет погоду из Open-Meteo, при MOCK или ошибке — mock."""
    if MOCK_MODE:
        return _mock_weather(ds)
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={LAT}&longitude={LON}"
            "&hourly=temperature_2m,relative_humidity_2m,pressure_msl"
            f"&start_date={ds}&end_date={ds}&timezone=Europe%2FMoscow"
        )
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        raw = r.json()
        return {
            "data": raw,
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "source": "open-meteo",
            "ds": ds,
        }
    except Exception as exc:  # fallback офлайн
        print(f"[ingest] weather API failed ({exc}), using mock")
        payload = _mock_weather(ds)
        payload["source"] = "open-meteo-mock-fallback"
        return payload


def _fetch_rates(ds: str) -> dict[str, Any]:
    if MOCK_MODE:
        return _mock_rates(ds)
    try:
        r = requests.get(EXCHANGE_URL, timeout=20)
        r.raise_for_status()
        raw = r.json()
        return {
            "data": raw,
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "source": "exchange-api",
            "ds": ds,
        }
    except Exception as exc:
        print(f"[ingest] rates API failed ({exc}), using mock")
        payload = _mock_rates(ds)
        payload["source"] = "exchange-mock-fallback"
        return payload


def _read_local_csvs(ds: str) -> list[dict[str, Any]]:
    """Читает все CSV из data/raw, возвращает список payload'ов."""
    out: list[dict[str, Any]] = []
    if not RAW_DIR.exists():
        print(f"[ingest] {RAW_DIR} not found, skip local csv")
        return out
    for csv_path in sorted(RAW_DIR.glob("*.csv")):
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        out.append(
            {
                "data": {"file": csv_path.name, "rows": rows},
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "source": f"local-csv:{csv_path.name}",
                "ds": ds,
            }
        )
    return out


@dag(
    dag_id="ingest_bronze",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lakehouse", "bronze", "ingestion"],
    doc_md=__doc__,
)
def ingest_bronze():  # type: ignore[no-untyped-def]
    @task
    def ingest_weather(ds: str | None = None) -> str:
        ds = ds or datetime.utcnow().strftime("%Y-%m-%d")
        payload = _fetch_weather(ds)
        key = f"bronze/weather/dt={ds}/weather_{ds}.json"
        uri = _put_json(key, payload)
        print(f"[bronze] weather -> {uri}")
        return uri

    @task
    def ingest_rates(ds: str | None = None) -> str:
        ds = ds or datetime.utcnow().strftime("%Y-%m-%d")
        payload = _fetch_rates(ds)
        key = f"bronze/rates/dt={ds}/rates_{ds}.json"
        uri = _put_json(key, payload)
        print(f"[bronze] rates -> {uri}")
        return uri

    @task
    def ingest_local_csv(ds: str | None = None) -> list[str]:
        ds = ds or datetime.utcnow().strftime("%Y-%m-%d")
        payloads = _read_local_csvs(ds)
        uris: list[str] = []
        for p in payloads:
            fname = p["data"]["file"].replace(".csv", "")
            key = f"bronze/local_{fname}/dt={ds}/{fname}_{ds}.json"
            uris.append(_put_json(key, p))
        print(f"[bronze] local csv -> {uris}")
        return uris

    @task
    def log_summary(weather_uri: str, rates_uri: str, csv_uris: list[str]) -> dict[str, Any]:
        summary = {
            "weather": weather_uri,
            "rates": rates_uri,
            "local_csv": csv_uris,
            "checked_at": datetime.utcnow().isoformat() + "Z",
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    w = ingest_weather()
    r = ingest_rates()
    c = ingest_local_csv()
    log_summary(w, r, c)


ingest_bronze()
