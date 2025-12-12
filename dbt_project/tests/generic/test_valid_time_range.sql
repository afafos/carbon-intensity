/*
    Generic тест: Проверка валидности временного диапазона
    
    Описание:
    Универсальный тест для проверки, что временные метки находятся в разумном диапазоне.
    Можно применять к любой модели с временными полями.
    
    Тип теста: Generic (многоразовый)
    
    Использование в schema.yml:
    tests:
      - valid_time_range:
          column_name: data_from_dttm
          min_date: '2020-01-01'
          max_date: '2030-12-31'
*/

{% test valid_time_range(model, column_name, min_date, max_date) %}

WITH validation AS (
    SELECT
        {{ column_name }},
        CASE 
            WHEN {{ column_name }} < TIMESTAMP '{{ min_date }}' THEN 'Before min_date'
            WHEN {{ column_name }} > TIMESTAMP '{{ max_date }}' THEN 'After max_date'
            WHEN {{ column_name }} IS NULL THEN 'NULL value'
            ELSE NULL
        END AS validation_error
    FROM {{ model }}
)

SELECT
    {{ column_name }},
    validation_error
FROM validation
WHERE validation_error IS NOT NULL

{% endtest %}
