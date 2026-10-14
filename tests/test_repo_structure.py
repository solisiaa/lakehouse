"""Целостность репозитория: файлы, compose-сервисы, конфиги."""

from __future__ import annotations

import pathlib

import yaml

REPO = pathlib.Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "docker-compose.yml",
    ".env.example",
    "Makefile",
    "README.md",
    "dags/ingest_bronze.py",
    "spark_jobs/bronze_to_silver.py",
    "spark_jobs/iceberg_maintenance.py",
    "dbt_project/dbt_project.yml",
    "dbt_project/profiles.yml",
    "trino/etc/catalog/iceberg.properties",
    "notebooks/iceberg_demo.ipynb",
    "docs/architecture.png",
    "scripts/init_minio.sh",
]

REQUIRED_SERVICES = {
    "postgres",
    "minio",
    "minio-init",
    "iceberg-rest",
    "spark-master",
    "spark-worker",
    "trino",
    "airflow-webserver",
    "airflow-scheduler",
    "dbt",
}


def test_required_files_exist():  # type: ignore[no-untyped-def]
    missing = [f for f in REQUIRED_FILES if not (REPO / f).exists()]
    assert not missing, f"отсутствуют файлы: {missing}"


def test_env_example_has_mock_and_buckets():  # type: ignore[no-untyped-def]
    env = (REPO / ".env.example").read_text(encoding="utf-8")
    for key in ("MOCK_MODE", "LAKEHOUSE_BUCKET", "WAREHOUSE_BUCKET", "S3_ENDPOINT"):
        assert key in env, f"нет {key} в .env.example"


def test_compose_services():  # type: ignore[no-untyped-def]
    compose = yaml.safe_load(
        (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    )
    services = set(compose["services"])
    assert REQUIRED_SERVICES <= services, f"нет сервисов: {REQUIRED_SERVICES - services}"
    # Airflow обязан быть на LocalExecutor по ТЗ
    airflow_env = compose["x-airflow-common"]["environment"]
    assert airflow_env.get("AIRFLOW__CORE__EXECUTOR") == "LocalExecutor"


def test_readme_covers_iceberg():  # type: ignore[no-untyped-def]
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    for token in (
        "ALTER TABLE",
        "ADD COLUMN",
        "FOR VERSION AS OF",
        "MERGE INTO",
        "mart_daily_summary",
    ):
        assert token in readme, f"README без {token}"
