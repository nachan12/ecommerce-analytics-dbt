{% macro safe_divide(numerator, denominator, default_value=0, as_percentage=false) %}
    {#
    Safely divides two numbers, handling division by zero gracefully.
    
    Args:
        numerator: The number to divide (numerator) - can be a column name or expression
        denominator: The number to divide by (denominator) - can be a column name or expression
        default_value: Value to return when denominator is 0 or NULL (default: 0). 
                      Pass the string 'null' to return NULL instead of a number.
        as_percentage: If true, multiplies result by 100 to get a percentage (default: false)
    
    Returns:
        The result of numerator / denominator, or default_value if denominator is 0 or NULL.
        If as_percentage is true, the result is multiplied by 100.
    
    Example Usage:
        -- Calculate return rate as a decimal (0.15 = 15%)
        {{ safe_divide('units_returned', 'total_units_sold') }}
        
        -- Calculate return rate as a percentage (15.0 = 15%)
        {{ safe_divide('units_returned', 'total_units_sold', as_percentage=true) }}
        
        -- Calculate profit margin, return NULL if denominator is 0
        {{ safe_divide('profit', 'revenue', default_value='null') }}
        
        -- Calculate ROI, return 0 if denominator is 0
        {{ safe_divide('gross_profit', 'cogs', default_value=0) }}
        
        -- Use with expressions
        {{ safe_divide('coalesce(sales, 0)', 'total_orders', default_value=0) }}
    #}
    
    {% if default_value == 'null' or default_value is none %}
        case
            when {{ denominator }} = 0 or {{ denominator }} is null then null
            else 
                {% if as_percentage %}
                    ({{ numerator }}::float / {{ denominator }}::float) * 100
                {% else %}
                    {{ numerator }}::float / {{ denominator }}::float
                {% endif %}
        end
    {% else %}
        case
            when {{ denominator }} = 0 or {{ denominator }} is null then {{ default_value }}
            else 
                {% if as_percentage %}
                    ({{ numerator }}::float / {{ denominator }}::float) * 100
                {% else %}
                    {{ numerator }}::float / {{ denominator }}::float
                {% endif %}
        end
    {% endif %}
{% endmacro %}

