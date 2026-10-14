"""Тесты bronze-логики: mock-генераторы, fallback API, локальные CSV.

Импортируют dags/ingest_bronze.py через стабы из conftest (без Airflow/MinIO).
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def load_ingest_module():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location(
        "ingest_bronze", REPO / "dags" / "ingest_bronze.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ingest_bronze"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ingest():  # type: ignore[no-untyped-def]
    return load_ingest_module()


def test_mock_weather_structure_and_determinism(ingest):  # type: ignore[no-untyped-def]
    p1 = ingest._mock_weather("2024-01-01")
    p2 = ingest._mock_weather("2024-01-01")
    # data детерминирована по дате; ingested_at — текущее время и обязан отличаться
    assert p1["data"] == p2["data"], "mock погоды должен быть детерминированным по ds"
    assert p1["ds"] == "2024-01-01"
    assert "ingested_at" in p1 and "source" in p1
    assert len(p1["data"]["hourly"]) == 24
    h0 = p1["data"]["hourly"][0]
    assert set(h0) >= {"hour", "temp_c", "humidity"}
    # разные даты — разные данные
    assert ingest._mock_weather("2024-01-02")["data"] != p1["data"]


def test_mock_rates_structure(ingest):  # type: ignore[no-untyped-def]
    p = ingest._mock_rates("2024-01-01")
    rates = p["data"]["rates"]
    assert p["data"]["base"] == "USD"
    for cur in ("EUR", "RUB", "CNY"):
        assert cur in rates and rates[cur] > 0


def test_fetch_weather_mock_mode(ingest, monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(ingest, "MOCK_MODE", True)
    p = ingest._fetch_weather("2024-01-03")
    assert p["ds"] == "2024-01-03"
    assert "mock" in p["source"]


def test_fetch_weather_api_fallback(ingest, monkeypatch):  # type: ignore[no-untyped-def]
    """При недоступности API должен вернуться mock-fallback, а не исключение."""
    monkeypatch.setattr(ingest, "MOCK_MODE", False)

    def boom(*a, **k):  # type: ignore[no-untyped-def]
        raise ConnectionError("offline")

    monkeypatch.setattr(ingest.requests, "get", boom)
    p = ingest._fetch_weather("2024-01-03")
    assert "mock-fallback" in p["source"]
    assert len(p["data"]["hourly"]) == 24


def test_fetch_rates_api_fallback(ingest, monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(ingest, "MOCK_MODE", False)

    def boom(*a, **k):  # type: ignore[no-untyped-def]
        raise ConnectionError("offline")

    monkeypatch.setattr(ingest.requests, "get", boom)
    p = ingest._fetch_rates("2024-01-03")
    assert "mock-fallback" in p["source"]
    assert p["data"]["rates"]["RUB"] > 0


def test_put_json_writes_to_s3_stub(ingest):  # type: ignore[no-untyped-def]
    uris = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body, **kw):  # type: ignore[no-untyped-def]
            uris.append(f"s3://{Bucket}/{Key}")
            json.loads(Body.decode("utf-8"))  # должен быть валидный JSON
            return {}

    ingest._s3_client = lambda: FakeS3()  # type: ignore[method-assign]
    uri = ingest._put_json("bronze/weather/dt=2024-01-01/w.json", {"a": 1})
    assert uri.startswith("s3://")
    assert uris == [uri]


def test_local_csv_samples_valid():  # type: ignore[no-untyped-def]
    for name in ("weather_sample.csv", "rates_sample.csv"):
        path = REPO / "data" / "raw" / name
        assert path.exists(), f"нет {path}"
        with open(path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) >= 2, f"{name} пустой"
