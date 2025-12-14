#!/bin/bash

echo "Генерация Elementary Data Quality Report..."

cd /app/dbt_project

echo "Генерация HTML отчета..."
edr report \
  --profiles-dir /app/dbt_project \
  --profile-target dev \
  --file-path /app/reports/elementary_report.html

echo "Отчет сгенерирован: /app/reports/elementary_report.html"
echo "Отчет доступен по адресу: http://localhost:8082/elementary_report.html"
