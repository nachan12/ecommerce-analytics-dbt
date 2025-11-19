{% macro subtract_days(expression, days) %}
    {{ adapter.dispatch('subtract_days', 'my_new_project')(expression, days) }}
{% endmacro %}

{% macro default__subtract_days(expression, days) %}
    ({{ expression }} - interval '{{ days }} day')
{% endmacro %}

{% macro snowflake__subtract_days(expression, days) %}
    dateadd(day, -{{ days }}, {{ expression }})
{% endmacro %}

{% macro bigquery__subtract_days(expression, days) %}
    date_sub({{ expression }}, interval {{ days }} day)
{% endmacro %}

{% macro spark__subtract_days(expression, days) %}
    date_sub({{ expression }}, {{ days }})
{% endmacro %}

{% macro databricks__subtract_days(expression, days) %}
    {{ spark__subtract_days(expression, days) }}
{% endmacro %}

