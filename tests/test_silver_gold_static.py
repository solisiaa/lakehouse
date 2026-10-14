"""Статические тесты Silver/Gold без живого Spark/Trino.

Проверяют наличие ключевых конструкций Iceberg:
MERGE INTO, PARTITIONED BY days(day), REST-каталог, S3-endpoint,
а также консистентность dbt (все ref() разрешаются, sources совпадают).
"""

from __future__ import annotations

import pathlib
import re

import yaml

REPO = pathlib.Path(__file__).resolve().parents[1]


def _read(*parts: str) -> str:
    return (REPO.joinpath(*parts)).read_text(encoding="utf-8")


def test_spark_job_has_merge_and_partitions():  # type: ignore[no-untyped-def]
    src = _read("spark_jobs", "bronze_to_silver.py")
    assert "MERGE INTO rest.silver.weather_daily" in src
    assert "MERGE INTO rest.silver.exchange_rates" in src
    assert "PARTITIONED BY (days(day))" in src
    assert "dropDuplicates" in src  # дедупликация обязательна по ТЗ
    assert "--date" in src  # CLI-аргумент для выборочного прогона


def test_spark_uses_rest_catalog_and_s3():  # type: ignore[no-untyped-def]
    src = _read("spark_jobs", "bronze_to_silver.py")
    for token in (
        "spark.sql.catalog.rest.type",
        "rest",
        "iceberg-rest:8181",
        "S3FileIO",
        "minio:9000",
        "s3a://lakehouse/bronze",
    ):
        assert token in src, f"нет {token}"


def test_maintenance_job_has_expire_and_rewrite():  # type: ignore[no-untyped-def]
    src = _read("spark_jobs", "iceberg_maintenance.py")
    assert "expire_snapshots" in src
    assert "rewrite_data_files" in src


def test_trino_catalog_points_to_rest_and_minio():  # type: ignore[no-untyped-def]
    props = _read("trino", "etc", "catalog", "iceberg.properties")
    assert "iceberg.catalog.type=rest" in props
    assert "http://iceberg-rest:8181" in props
    assert "http://minio:9000" in props
    assert "s3://warehouse/wh/" in props


def test_dbt_refs_resolve_and_sources_match():  # type: ignore[no-untyped-def]
    models = list((REPO / "dbt_project" / "models").rglob("*.sql"))
    assert len(models) >= 6
    defined = {p.stem for p in models}
    refs: set[str] = set()
    for p in models:
        refs.update(re.findall(r"ref\('([^']+)'\)", p.read_text(encoding="utf-8")))
    assert refs - defined == set(), f"висячие ref: {refs - defined}"
    sources = yaml.safe_load(
        (REPO / "dbt_project" / "models" / "staging" / "sources.yml").read_text(
            encoding="utf-8"
        )
    )
    tables = {t["name"] for t in sources["sources"][0]["tables"]}
    assert {"weather_daily", "exchange_rates"} <= tables


def test_dbt_has_quality_tests():  # type: ignore[no-untyped-def]
    schema = _read("dbt_project", "models", "marts", "schema.yml")
    for t in ("unique", "not_null"):
        assert t in schema
    assert (REPO / "dbt_project" / "tests" / "assert_positive_rates.sql").exists()
    assert (REPO / "dbt_project" / "tests" / "assert_reasonable_temp.sql").exists()
