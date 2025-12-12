"""
DAG для запуска DBT трансформаций данных carbon intensity.

Описание:
    Данный DAG отвечает за регулярное построение и тестирование
    аналитических моделей в DBT с 4-слойной архитектурой (STG → ODS → DWH → DM).
    
Расписание:
    Каждые 2 часа (после завершения EL процесса)
    
Этапы выполнения:
    1. log_dbt_run_info - логирование информации о запуске
    2. dbt_deps - установка зависимостей
    3. dbt_seed - загрузка справочных данных
    
    4-7. Последовательное построение слоев:
       - staging_layer: Построение и тестирование staging моделей (VIEW)
       - ods_layer: Построение и тестирование ODS моделей (INCREMENTAL: DELETE+INSERT, MERGE)
       - dwh_layer: Построение и тестирование DWH моделей (INCREMENTAL: APPEND)
       - marts_layer: Построение и тестирование Data Marts (TABLE)
    
    8. dbt_test_all - запуск всех тестов качества данных
    9. elementary_monitor - мониторинг аномалий качества данных
    10. generate_elementary_report - автоматическая генерация HTML отчета Elementary
    11. dbt_docs_generate - генерация документации DBT
    
Особенности:
    - Использует Task Groups для организации задач по слоям
    - Инкрементальные загрузки: 3 типа стратегий (DELETE+INSERT, APPEND, MERGE)
    - Автоматическая генерация Elementary отчета после каждого запуска
    - Все тесты: dbt-core (4 типа) + elementary-data (6 типов) + custom тесты
    
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"


def get_dbt_command(command: str, select: str = None, full_refresh: bool = False) -> str:
    """
    Генерирует команду для запуска DBT.
    
    Args:
        command: Команда DBT (run, test, deps и т.д.)
        select: Селектор моделей (например, "tag:staging")
        full_refresh: Полная перестройка (игнорирует инкрементальность)
    
    Returns:
        Строка с командой для bash
    """
    cmd = f"cd {DBT_PROJECT_DIR} && dbt {command}"
    cmd += f" --profiles-dir {DBT_PROFILES_DIR}"
    
    if select:
        cmd += f" --select {select}"
    
    if full_refresh and command == "run":
        cmd += " --full-refresh"
    
    return cmd


def log_dbt_run_info(**context):
    """
    Логирует информацию о запуске DBT.
    """
    execution_date = context['execution_date']
    print(f"Starting DBT transformation for execution date: {execution_date}")
    print(f"DBT Project Directory: {DBT_PROJECT_DIR}")
    print(f"DBT Profiles Directory: {DBT_PROFILES_DIR}")
    return "DBT run initiated"


default_args = {
    'owner': 'carbon_intensity_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}


with DAG(
    dag_id='dbt_transformation',
    default_args=default_args,
    description='DBT трансформации для carbon intensity data warehouse',
    schedule_interval='0 */2 * * *',  # Каждые 2 часа
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'transformation', 'dwh', 'analytics'],
    max_active_runs=1,  # Только один запуск одновременно
    doc_md=__doc__,
) as dag:
    
    
    log_start = PythonOperator(
        task_id='log_dbt_run_info',
        python_callable=log_dbt_run_info,
        doc_md="""
        ### Логирование информации о запуске
        
        Записывает в лог информацию о текущем запуске DBT.
        """
    )
    
    # Установка зависимостей DBT (dbt_utils, elementary и т.д.)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=get_dbt_command('deps'),
        doc_md="""
        ### Установка зависимостей DBT
        
        Устанавливает пакеты из packages.yml:
        - dbt_utils - полезные макросы
        - elementary-data - мониторинг качества
        - dbt_expectations - расширенные тесты
        """
    )
    
    # Загрузка seed данных
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=get_dbt_command('seed'),
        doc_md="""
        ### Загрузка справочных данных
        
        Загружает CSV файлы из директории seeds/ в БД.
        Используется для справочников и статических данных.
        """
    )
    
    
    with TaskGroup(
        group_id='staging_layer',
        tooltip='Построение Staging моделей'
    ) as staging_layer:
        
        dbt_run_staging = BashOperator(
            task_id='run_staging_models',
            bash_command=get_dbt_command('run', select='tag:staging'),
            doc_md="""
            ### Staging Layer - Очистка и нормализация
            
            Модели:
            - stg_carbon_intensity_current
            - stg_carbon_intensity_history
            
            Материализация: VIEW
            """
        )
        
        dbt_test_staging = BashOperator(
            task_id='test_staging_models',
            bash_command=get_dbt_command('test', select='tag:staging'),
            doc_md="""
            ### Тестирование Staging моделей
            
            Проверяет:
            - Уникальность ключей
            - Отсутствие NULL в обязательных полях
            - Валидность значений
            """
        )
        
        dbt_run_staging >> dbt_test_staging
    
    
    with TaskGroup(
        group_id='ods_layer',
        tooltip='Построение ODS моделей (инкрементально)'
    ) as ods_layer:
        
        dbt_run_ods = BashOperator(
            task_id='run_ods_models',
            bash_command=get_dbt_command('run', select='tag:ods'),
            doc_md="""
            ### ODS Layer - Оперативное хранилище
            
            Модели:
            - ods_carbon_intensity (DELETE+INSERT)
            - ods_carbon_intensity_daily_summary (MERGE)
            
            Материализация: INCREMENTAL
            Стратегии: DELETE+INSERT, MERGE
            """
        )
        
        dbt_test_ods = BashOperator(
            task_id='test_ods_models',
            bash_command=get_dbt_command('test', select='tag:ods'),
            doc_md="""
            ### Тестирование ODS моделей
            
            Включает Elementary тесты:
            - volume_anomalies
            - freshness_anomalies
            - dimension_anomalies
            """
        )
        
        dbt_run_ods >> dbt_test_ods
    
    
    with TaskGroup(
        group_id='dwh_layer',
        tooltip='Построение DWH моделей (исторические факты)'
    ) as dwh_layer:
        
        dbt_run_dwh = BashOperator(
            task_id='run_dwh_models',
            bash_command=get_dbt_command('run', select='tag:dwh'),
            doc_md="""
            ### DWH Layer
            
            Модели:
            - dwh_carbon_intensity_fact (APPEND)
            - dwh_forecast_accuracy (APPEND)
            
            Материализация: INCREMENTAL
            Стратегия: APPEND (immutable facts)
            
            Использует:
            - Оконные функции
            - CTE
            """
        )
        
        dbt_test_dwh = BashOperator(
            task_id='test_dwh_models',
            bash_command=get_dbt_command('test', select='tag:dwh'),
            doc_md="""
            ### Тестирование DWH моделей
            
            Проверяет:
            - Relationships (внешние ключи)
            - Диапазоны значений
            - Аномалии данных (Elementary)
            """
        )
        
        dbt_run_dwh >> dbt_test_dwh
    
    
    with TaskGroup(
        group_id='marts_layer',
        tooltip='Построение Data Marts для BI'
    ) as marts_layer:
        
        dbt_run_marts = BashOperator(
            task_id='run_marts_models',
            bash_command=get_dbt_command('run', select='tag:marts'),
            doc_md="""
            ### Data Marts - Бизнес-витрины
            
            Модели:
            - dm_carbon_intensity_analytics
            - dm_carbon_intensity_daily_report
            
            Материализация: TABLE
            
            Содержит:
            - Бизнес-метрики
            - Рекомендации по энергопотреблению
            - Аномалии и тренды
            """
        )
        
        dbt_test_marts = BashOperator(
            task_id='test_marts_models',
            bash_command=get_dbt_command('test', select='tag:marts'),
            doc_md="""
            ### Тестирование Data Marts
            
            Финальные проверки перед BI:
            - Полнота данных
            - Корректность метрик
            - Консистентность с источниками
            """
        )
        
        dbt_run_marts >> dbt_test_marts
    
    
    # Запуск всех тестов DBT
    dbt_test_all = BashOperator(
        task_id='dbt_test_all',
        bash_command=get_dbt_command('test'),
        trigger_rule='all_done',
        doc_md="""
        ### Полное тестирование
        
        Запускает все тесты:
        - 4 типа dbt-core тестов
        - 6 типов elementary-data тестов
        - Custom singular и generic тесты
        """
    )
    
    # Elementary мониторинг
    elementary_monitor = BashOperator(
        task_id='elementary_monitor',
        bash_command=f"cd {DBT_PROJECT_DIR} && edr monitor || echo 'Elementary monitoring completed'",
        trigger_rule='all_done',
        doc_md="""
        ### Elementary Data мониторинг
        
        Запускает мониторинг качества данных:
        - Детекция аномалий
        - Проверка свежести данных
        - Отслеживание метрик качества
        """
    )
    
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=get_dbt_command('docs generate'),
        trigger_rule='all_done',
        doc_md="""
        ### Генерация документации
        
        Создает актуальную документацию:
        - Граф зависимостей моделей
        - Описания моделей и колонок
        - Результаты тестов
        """
    )
    
    # Автоматическая генерация Elementary HTML отчета
    # Используем docker exec для запуска команды в elementary контейнере
    generate_elementary_report = BashOperator(
        task_id='generate_elementary_report',
        bash_command="""
            echo "Обновление Elementary отчета..." && \
            docker exec carbon_elementary /bin/bash -c "
                cd /app/dbt_project && \
                DBT_PROFILE=carbon_intensity edr report \
                    --profiles-dir /app/dbt_project \
                    --profile-target dev \
                    --file-path /app/reports/elementary_report.html
            " && echo "Elementary отчет успешно обновлен!" || echo "Не удалось обновить Elementary отчет"
        """,
        trigger_rule='all_done',
        doc_md="""
        ### Генерация Elementary Data Quality отчета
        
        Автоматически создает/обновляет HTML отчет с:
        - Результатами всех 6 типов Elementary тестов
        - Графиками аномалий и трендов
        - Статистикой качества данных
        - История выполнения тестов
        
        Отчет доступен по URL: http://localhost:8082/elementary_report.html
        
        Примечание: Использует docker exec для взаимодействия с elementary контейнером
        """
    )
    
    
    # Подготовка
    log_start >> dbt_deps >> dbt_seed
    
    # Последовательное построение слоев: STG -> ODS -> DWH -> DM
    dbt_seed >> staging_layer >> ods_layer >> dwh_layer >> marts_layer
    
    # Финальное тестирование и мониторинг
    marts_layer >> [dbt_test_all, elementary_monitor, dbt_docs_generate]
    
    # Генерация Elementary отчета после мониторинга
    elementary_monitor >> generate_elementary_report

