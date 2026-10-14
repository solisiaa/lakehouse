"""Pytest-стабы: позволяют импортировать DAG и Spark-джобы без Airflow/boto3/Spark.

Тяжёлые зависимости (airflow, boto3, pyspark) есть только в Docker-образах.
Для локального `pytest` подменяем их лёгкими заглушками.
"""

from __future__ import annotations

import sys
import types


def _ensure_dag_stubs() -> None:
    # --- airflow.decorators.dag/task ---
    if "airflow" not in sys.modules:
        airflow = types.ModuleType("airflow")
        decorators = types.ModuleType("airflow.decorators")

        def dag(*d_args, **d_kwargs):  # type: ignore[no-untyped-def]
            def wrap(fn):  # type: ignore[no-untyped-def]
                fn._is_airflow_dag = True  # type: ignore[attr-defined]
                fn.dag_kwargs = d_kwargs  # type: ignore[attr-defined]
                return fn

            # поддержка @dag без скобок
            if d_args and callable(d_args[0]) and not d_kwargs:
                return wrap(d_args[0])
            return wrap

        def task(*t_args, **t_kwargs):  # type: ignore[no-untyped-def]
            def wrap(fn):  # type: ignore[no-untyped-def]
                fn._is_airflow_task = True  # type: ignore[attr-defined]
                return fn

            if t_args and callable(t_args[0]) and not t_kwargs:
                return wrap(t_args[0])
            return wrap

        decorators.dag = dag  # type: ignore[attr-defined]
        decorators.task = task  # type: ignore[attr-defined]
        airflow.decorators = decorators  # type: ignore[attr-defined]
        sys.modules["airflow"] = airflow
        sys.modules["airflow.decorators"] = decorators

    # --- boto3 ---
    if "boto3" not in sys.modules:
        boto3 = types.ModuleType("boto3")

        class _DummyS3:
            def __init__(self) -> None:
                self.objects: dict[str, bytes] = {}

            def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs):  # type: ignore[no-untyped-def]
                self.objects[f"s3://{Bucket}/{Key}"] = Body
                return {"ETag": '"dummy"'}

        boto3.client = lambda *a, **k: _DummyS3()  # type: ignore[attr-defined]
        sys.modules["boto3"] = boto3

    # --- pyspark (только чтобы импорт spark_jobs не падал; логика тестируется статически) ---
    if "pyspark" not in sys.modules:
        pyspark = types.ModuleType("pyspark")
        sql_mod = types.ModuleType("pyspark.sql")

        class DataFrame:  # type: ignore[no-redef]
            pass

        class SparkSession:  # type: ignore[no-redef]
            pass

        sql_mod.DataFrame = DataFrame  # type: ignore[attr-defined]
        sql_mod.SparkSession = SparkSession  # type: ignore[attr-defined]
        pyspark.sql = sql_mod  # type: ignore[attr-defined]
        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.sql"] = sql_mod
        for name in ("pyspark.sql.functions", "pyspark.sql.types"):
            m = types.ModuleType(name)
            sys.modules[name] = m
        sys.modules["pyspark.sql.types"].DoubleType = object  # type: ignore[attr-defined]
        sys.modules["pyspark.sql.types"].IntegerType = object  # type: ignore[attr-defined]
        sys.modules["pyspark.sql.types"].StringType = object  # type: ignore[attr-defined]


_ensure_dag_stubs()
