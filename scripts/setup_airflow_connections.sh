# Скрипт для настройки подключений в Airflow

echo "Настройка подключений Airflow..."

airflow connections delete 'mongo_default' 2>/dev/null || true
airflow connections add 'mongo_default' \
    --conn-type 'mongo' \
    --conn-host 'mongodb' \
    --conn-port 27017 \
    --conn-login 'admin' \
    --conn-password 'admin123' \
    --conn-schema 'carbon_intensity' \
    --conn-extra '{"authSource": "admin"}'

airflow connections add 'postgres_analytics' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-port 5432 \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'analytics' || echo "Подключение postgres_analytics уже существует"

echo "Подключения настроены!"

echo "Список подключений:"
airflow connections list

