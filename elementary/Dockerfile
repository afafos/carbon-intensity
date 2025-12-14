FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Обновляем elementary до версии 0.20.1 для работы отчетов
RUN pip install --no-cache-dir \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.2 \
    elementary-data[postgres]==0.20.1 \
    protobuf==4.25.3


CMD ["tail", "-f", "/dev/null"]
