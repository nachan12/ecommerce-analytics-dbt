-- Singular test: Validates that net_sales equals gross_item_sales minus total_refund_amount
-- This ensures our calculation logic is correct and there are no data inconsistencies

select
    order_id,
    gross_item_sales,
    total_refund_amount,
    net_sales,
    gross_item_sales - total_refund_amount as calculated_net_sales,
    abs(net_sales - (gross_item_sales - total_refund_amount)) as difference
from {{ ref('int_order_metrics') }}
where abs(net_sales - (gross_item_sales - total_refund_amount)) > 0.01
-- Allow small floating point differences (0.01)

