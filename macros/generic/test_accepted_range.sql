{% test accepted_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}
    {#
    Generic test to validate that a column's values fall within an acceptable range.
    
    Args:
        model: The model to test
        column_name: The column to validate
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        inclusive: Whether min/max values are inclusive (default: true)
    
    Example:
        - name: unit_price
          tests:
            - accepted_range:
                min_value: 0
                max_value: 1000
                inclusive: true
    #}
    
    select *
    from {{ model }}
    where true
    {% if min_value is not none %}
        {% if inclusive %}
            and {{ column_name }} < {{ min_value }}
        {% else %}
            and {{ column_name }} <= {{ min_value }}
        {% endif %}
    {% endif %}
    {% if max_value is not none %}
        {% if inclusive %}
            and {{ column_name }} > {{ max_value }}
        {% else %}
            and {{ column_name }} >= {{ max_value }}
        {% endif %}
    {% endif %}
    {% if min_value is none and max_value is none %}
        -- If no range specified, test passes (no rows returned)
        and 1 = 0
    {% endif %}
{% endtest %}

