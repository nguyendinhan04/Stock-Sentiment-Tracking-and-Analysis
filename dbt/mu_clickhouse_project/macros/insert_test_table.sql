{% macro insert_test_table() %}
    INSERT INTO test_table
    SELECT * FROM {{ ref('test_model') }}
{% endmacro %}