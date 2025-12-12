#!/bin/bash
# Скрипт для автоматического обновления Elementary отчета

echo "Обновление Elementary отчета..."

# Запускаем генерацию отчета в контейнере elementary
docker exec carbon_elementary /bin/bash -c "
    cd /app/dbt_project && \
    DBT_PROFILE=carbon_intensity edr report \
        --profiles-dir /app/dbt_project \
        --profile-target dev \
        --file-path /app/reports/elementary_report.html
" 

if [ $? -eq 0 ]; then
    echo "Elementary отчет успешно обновлен!"
    echo "Отчет доступен: http://localhost:8082/elementary_report.html"
else
    echo "Не удалось обновить Elementary отчет"
    exit 0  # Не падаем, чтобы не останавливать DAG
fi
