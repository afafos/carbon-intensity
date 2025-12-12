#!/bin/bash

echo "Генерация Elementary Data Quality Report..."

cd /app/dbt_project

echo "Проверка подключения к PostgreSQL..."
dbt --profiles-dir . debug || { echo "Ошибка подключения к БД"; exit 1; }

echo "Установка зависимостей DBT..."
dbt --profiles-dir . deps

echo "Запуск Elementary тестов..."
dbt --profiles-dir . test --select tag:elementary || echo "Некоторые тесты не прошли"

echo "Генерация HTML отчета..."
edr report \
  --profiles-dir /app/dbt_project \
  --profile carbon_intensity \
  --profile-target dev \
  --file-path /app/reports/elementary_report.html

echo "Отчет сгенерирован: /app/reports/elementary_report.html"
echo "Отчет доступен по адресу: http://localhost:8082/elementary_report.html"
