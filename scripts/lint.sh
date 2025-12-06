# Скрипт для проверки кода линтерами

set -e 

echo "Проверка кода..."
echo ""

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Проверить что зависимости установлены
if ! command -v ruff &> /dev/null; then
    echo -e "${RED}[ERROR] ruff не установлен${NC}"
    echo "Установите зависимости: pip install -r requirements-dev.txt"
    exit 1
fi

# Директории для проверки
DIRS="src/ airflow/dags/"

echo "Проверяемые директории: $DIRS"
echo ""

# 1. Ruff - линтер
echo "Запуск ruff (линтер)..."
if ruff check $DIRS; then
    echo -e "${GREEN}[OK] ruff: все хорошо${NC}"
else
    echo -e "${RED}[FAIL] ruff: найдены проблемы${NC}"
    echo "Попробуйте автоисправление: ruff check --fix $DIRS"
    exit 1
fi
echo ""

# 2. Black - форматирование
echo "Проверка форматирования (black)..."
if black --check $DIRS; then
    echo -e "${GREEN}[OK] black: форматирование корректно${NC}"
else
    echo -e "${YELLOW}[WARN] black: требуется форматирование${NC}"
    echo "Автоисправление: black $DIRS"
    exit 1
fi
echo ""

echo -e "${GREEN}Все проверки пройдены!${NC}"

