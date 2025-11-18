-- Singular test: Validates that gross_item_sales equals quantity * unit_price
-- This ensures our calculated field matches the source data

select
    order_item_id,
    quantity,
    unit_price,
    gross_item_sales,
    quantity * unit_price as calculated_gross_sales,
    abs(gross_item_sales - (quantity * unit_price)) as difference
from {{ ref('stg_order_items') }}
where abs(gross_item_sales - (quantity * unit_price)) > 0.01
-- Allow small floating point differences (0.01)

