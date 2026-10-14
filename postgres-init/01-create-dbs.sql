-- Создаём БД для Iceberg REST каталога (JDBC backend).
SELECT 'CREATE DATABASE iceberg_catalog'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog')\gexec
