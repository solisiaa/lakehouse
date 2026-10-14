.DEFAULT_GOAL := help

help: ## Показать помощь
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Поднять всю платформу
	docker compose up -d --build
	@echo "Airflow:  http://localhost:8080 (airflow/airflow)"
	@echo "MinIO:    http://localhost:9001 (minioadmin/minioadmin123)"
	@echo "Trino:    http://localhost:8082"
	@echo "Spark UI: http://localhost:8085"

init: ## Создать бакеты и Iceberg-таблицы (идемпотентно)
	docker compose up -d postgres minio
	sleep 5
	docker compose up minio-init --build || true
	docker compose up -d iceberg-rest spark-master spark-worker trino
	sleep 10
	docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /home/iceberg/spark_jobs/bronze_to_silver.py --help || true
	@echo "Init done. Дальше: make demo-flow"

down: ## Остановить всё
	docker compose down

clean: ## Остановить всё + удалить volumes
	docker compose down -v

logs: ## Логи всех сервисов
	docker compose logs -f --tail=200

ps: ## Статус сервисов
	docker compose ps

trigger-ingest: ## Ручной запуск DAG ingest_bronze
	docker compose exec airflow-scheduler airflow dags trigger ingest_bronze

spark-silver: ## Прогнать Bronze->Silver (все даты). Можно DATE=2024-01-01
	docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /home/iceberg/spark_jobs/bronze_to_silver.py $(if [ -n "$(DATE)" ]; then echo "--date $(DATE)"; fi)

dbt-run: ## Собрать dbt-модели + тесты
	docker compose exec dbt dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
	docker compose exec dbt dbt test --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt

dbt-docs: ## Сгенерировать и поднять dbt docs (http://localhost:8080 не занимать — docs на 8081 внутри контейнера, пробросим 8081)
	docker compose exec dbt dbt docs generate --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
	docker compose exec -d dbt dbt docs serve --port 8081 --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt || true
	@echo "dbt docs: http://localhost:8081 (нужен порт-маппинг, см. README)"

demo-flow: trigger-ingest spark-silver dbt-run ## End-to-end: ingest -> silver -> gold

trino-cli: ## Открыть trino CLI
	docker compose exec trino trino --server localhost:8080 --catalog iceberg --schema gold

lint-py: ## Быстрая проверка синтаксиса python
	python3 -m py_compile dags/ingest_bronze.py spark_jobs/bronze_to_silver.py spark_jobs/iceberg_maintenance.py scripts/register_iceberg_tables.py && echo OK

test: ## Python-тесты (pytest, без Docker)
	python3 -m pip install -q -r tests/requirements-test.txt
	python3 -m pytest tests -v
