#!/bin/bash

# Скрипт для автоматического форматирования кода

set -e

echo "Автоматическое форматирование кода..."
echo ""

GREEN='\033[0;32m'
NC='\033[0m'

DIRS="src/ airflow/dags/"

# 1. Black - форматирование
echo "Форматирование кода (black)..."
black $DIRS
echo -e "${GREEN}[OK] Black применен${NC}"
echo ""

# 2. Ruff - автоисправление
echo "Автоисправление проблем (ruff)..."
ruff check --fix $DIRS
echo -e "${GREEN}[OK] Ruff автоисправления применены${NC}"
echo ""

# 3. Ruff - сортировка импортов
echo "Сортировка импортов (ruff)..."
ruff check --select I --fix $DIRS
echo -e "${GREEN}[OK] Импорты отсортированы${NC}"
echo ""

echo -e "${GREEN}Форматирование завершено!${NC}"
echo ""
echo "Запустите проверку: bash scripts/lint.sh"

