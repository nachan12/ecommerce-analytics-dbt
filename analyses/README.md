# Analysis Files

This directory contains complex analytical queries that demonstrate advanced SQL patterns and provide business insights. These queries are not materialized as models but can be run directly or compiled using dbt.

## Available Analyses

### `customer_segment_product_performance_analysis.sql`

A comprehensive analysis that combines customer segmentation, product performance, and revenue trends.

**Key Features:**
- **Multiple CTEs**: Modular query structure for readability
- **Window Functions**: Running totals, rankings, and growth calculations
- **Complex Joins**: Joins across fact and dimension tables
- **Cohort Analysis**: Customer lifetime value by segment and category
- **Pareto Analysis**: Identifies top-performing products (80/20 rule)
- **Trend Analysis**: Month-over-month growth rates
- **Business Intelligence**: Flags for growth status, return risk, and opportunities

**Business Questions Answered:**
1. Which products perform best within each customer segment?
2. What is the average order value trend by segment over time?
3. Which customer segments have the highest return rates by product category?
4. What is the customer lifetime value distribution by segment and product category?
5. Which products drive the most revenue from VIP customers?

**SQL Patterns Demonstrated:**
- Window functions (`rank()`, `lag()`, `sum() over()`)
- Conditional aggregations (`case when` with aggregations)
- Percentile calculations (`percentile_cont()`)
- Date functions (`date_trunc()`, `datediff()`)
- Complex joins with multiple conditions
- Subqueries and correlated aggregations

**Usage:**
```bash
# Compile the query (replaces ref() macros with actual table names)
dbt compile --select analyses/customer_segment_product_performance_analysis

# The compiled SQL will be in target/compiled/[project_name]/analyses/
```

**Output Columns:**
- Time dimensions: `order_month`
- Segment dimensions: `customer_segment`, `loyalty_tier`, `primary_product_category`
- Revenue metrics: `total_net_sales`, `segment_running_total_revenue`, `mom_growth_rate_pct`
- Order metrics: `order_count`, `unique_customers`, `avg_order_value`
- Return metrics: `return_rate_pct`, `refund_rate_pct`
- Product rankings: `revenue_rank_in_category`, `pct_of_segment_revenue`, `cumulative_revenue_pct`
- LTV statistics: `avg_category_ltv`, `median_category_ltv`, `p90_category_ltv`
- Business insights: `growth_status`, `pareto_classification`, `return_risk_level`, `opportunity_tier`

## Running Analyses

### Option 1: Compile and Run in SQL Client
```bash
dbt compile --select analyses/customer_segment_product_performance_analysis
# Then copy the SQL from target/compiled/[project_name]/analyses/ and run in your SQL client
```

### Option 2: Use dbt run-operation (if configured)
Some warehouses support running queries directly through dbt, but analyses are typically compiled and run manually.

### Option 3: Convert to a Model
If you want to materialize the results, you can:
1. Move the file from `analyses/` to `models/`
2. Add a `{{ config(materialized='table') }}` or `{{ config(materialized='view') }}` at the top
3. Run `dbt run --select [model_name]`

## Best Practices

1. **Use CTEs**: Break complex queries into logical steps using CTEs
2. **Document Business Logic**: Add comments explaining calculations and business rules
3. **Optimize for Readability**: Use descriptive aliases and format consistently
4. **Test Incrementally**: Test each CTE separately before combining
5. **Consider Performance**: For large datasets, add appropriate filters and indexes

## Adding New Analyses

To add a new analysis:
1. Create a new `.sql` file in the `analyses/` directory
2. Use `{{ ref('model_name') }}` to reference dbt models
3. Add documentation comments explaining the purpose and business questions
4. Update this README with a description of the new analysis

