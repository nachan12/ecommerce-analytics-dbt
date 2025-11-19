# dbt_utils Integration Guide

This document describes how `dbt_utils` is integrated into the ecommerce analytics project and demonstrates its usage across various models and tests.

## Package Installation

The `dbt_utils` package is defined in `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

To install the package, run:
```bash
dbt deps
```

## dbt_utils Usage in This Project

### 1. Generic Tests

#### `dbt_utils.accepted_range`
Validates that numeric values fall within a specified range.

**Usage Examples:**

```yaml
# In schema.yml files
columns:
  - name: quantity
    tests:
      - dbt_utils.accepted_range:
          min_value: 1
          inclusive: true
          
  - name: return_rate
    tests:
      - dbt_utils.accepted_range:
          min_value: 0
          max_value: 1
          inclusive: true
```

**Applied to:**
- `stg_order_items.quantity` (min: 1)
- `stg_order_items.unit_price` (min: 0)
- `stg_products.unit_cost` (min: 0)
- `stg_returns.refund_amount` (min: 0)
- `mart_product_performance.return_rate` (0-1)
- `mart_product_performance.profit_margin_after_returns` (-1 to 1)
- `mart_customer_segment_performance.active_customer_ratio` (0-1)

#### `dbt_utils.unique_combination_of_columns`
Validates that a combination of columns is unique.

**Usage Example:**

```yaml
# In schema.yml
models:
  - name: mart_customer_segment_performance
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customer_segment
            - loyalty_tier
```

**Applied to:**
- `mart_customer_segment_performance` (customer_segment + loyalty_tier)

### 2. Macros in Models

#### `dbt_utils.generate_surrogate_key`
Creates a surrogate key from multiple columns using hashing.

**Usage Example:**

```sql
-- In int_order_metrics.sql
{{ dbt_utils.generate_surrogate_key([
    'o.order_id',
    'o.user_id', 
    'o.order_date'
]) }} as order_surrogate_key
```

**Benefits:**
- Creates consistent surrogate keys across warehouses
- Handles NULL values gracefully
- Works with any number of columns

**Applied to:**
- `int_order_metrics.order_surrogate_key`
- `mart_daily_revenue_summary.daily_revenue_key`

#### `dbt_utils.date_spine`
Generates a complete date series (date spine) for time-based analysis.

**Usage Example:**

```sql
-- In mart_daily_revenue_summary.sql
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-05-01' as date)",
        end_date="cast('2023-08-31' as date)"
    ) }}
)
```

**Benefits:**
- Ensures complete time series coverage
- Fills gaps in data for accurate time-based analysis
- Warehouse-agnostic date generation

**Applied to:**
- `mart_daily_revenue_summary` (ensures every day has a row, even with zero revenue)

#### `dbt_utils.union_relations`
Unions multiple relations (models/seeds) with different schemas, handling column mismatches automatically.

**Usage Example:**

```sql
-- Works with actual relations (models/seeds), not CTEs
{{ dbt_utils.union_relations(
    relations=[
        ref('model1'),
        ref('model2'),
        source('source_name', 'table_name')
    ],
    source_column_name='_dbt_source_relation'
) }}
```

**Benefits:**
- Automatically handles schema differences
- Adds source tracking column
- Works with any number of relations
- Handles missing columns gracefully

**Note:** `union_relations` works with actual relations (models/seeds), not CTEs. For CTEs, use standard `UNION ALL`.

**Demonstrated in:**
- `int_union_orders_returns_demo` (shows the pattern conceptually)

#### `dbt_utils.date_trunc`
Truncates dates to specified granularity (warehouse-agnostic).

**Usage Example:**

```sql
-- In mart_daily_revenue_summary.sql
{{ dbt_utils.date_trunc('week', 'revenue_date') }} as week_start_date
```

**Benefits:**
- Warehouse-agnostic date truncation
- Consistent behavior across platforms

**Applied to:**
- `mart_daily_revenue_summary.week_start_date`

## Demonstration Models

### 1. `mart_daily_revenue_summary`
**Purpose:** Daily revenue summary with complete date coverage

**dbt_utils Features Demonstrated:**
- `date_spine`: Generates complete date series
- `generate_surrogate_key`: Creates composite keys
- `date_trunc`: Week-level aggregation

**Key Benefits:**
- No missing dates in time series
- Consistent surrogate keys
- Warehouse-agnostic date functions

### 2. `int_union_orders_returns_demo`
**Purpose:** Demonstrates unioning orders and returns as unified transactions

**dbt_utils Features Demonstrated:**
- `union_relations`: Combines relations with different schemas

**Key Benefits:**
- Automatic schema alignment
- Source tracking for data lineage

## Comparison: Custom vs dbt_utils

### Custom `accepted_range` Test
We maintain a custom `accepted_range` test in `macros/generic/test_accepted_range.sql` as a fallback, but prefer `dbt_utils.accepted_range` for:
- Better maintenance (community-supported)
- More features and edge case handling
- Consistent behavior across projects

### When to Use dbt_utils vs Custom Macros

**Use dbt_utils when:**
- The functionality is well-established and widely used
- You want community support and updates
- The macro handles warehouse differences automatically
- You want consistency with other dbt projects

**Use custom macros when:**
- You need project-specific logic
- The functionality is unique to your use case
- You want full control over implementation
- You're prototyping before contributing to dbt_utils

## Additional dbt_utils Macros Available

While not currently used in this project, `dbt_utils` provides many other useful macros:

### Data Quality
- `dbt_utils.expression_is_true`: Validates expressions
- `dbt_utils.recency`: Checks data freshness
- `dbt_utils.at_least_one`: Ensures at least one row matches condition

### Data Transformation
- `dbt_utils.pivot`: Pivots data
- `dbt_utils.unpivot`: Unpivots data
- `dbt_utils.get_column_values`: Gets distinct column values
- `dbt_utils.get_relations_by_pattern`: Finds relations by pattern

### Schema Management
- `dbt_utils.get_columns_in_relation`: Gets column list
- `dbt_utils.get_query_results_as_dict`: Executes query and returns dict

### Date/Time
- `dbt_utils.dateadd`: Adds time intervals
- `dbt_utils.datediff`: Calculates date differences
- `dbt_utils.last_day`: Gets last day of period

## Best Practices

1. **Use dbt_utils for Common Patterns**: Don't reinvent the wheel for well-established patterns
2. **Document Usage**: Comment when using dbt_utils macros to explain why
3. **Version Pinning**: Pin dbt_utils version in `packages.yml` for reproducibility
4. **Test Coverage**: Use dbt_utils tests alongside custom tests
5. **Stay Updated**: Regularly update dbt_utils to get bug fixes and new features

## Running Tests with dbt_utils

All dbt_utils tests run automatically with standard dbt test commands:

```bash
# Run all tests (including dbt_utils tests)
dbt test

# Run tests for a specific model
dbt test --select stg_order_items

# Run only dbt_utils tests
dbt test --select test_type:generic test_name:accepted_range
```

## Resources

- [dbt_utils Documentation](https://github.com/dbt-labs/dbt-utils)
- [dbt_utils Macros Reference](https://github.com/dbt-labs/dbt-utils/tree/main/macros)
- [dbt Packages Guide](https://docs.getdbt.com/docs/build/packages)

## Summary

The `dbt_utils` package enhances this project by providing:
- **Robust Testing**: `accepted_range` and `unique_combination_of_columns` tests
- **Data Quality**: Surrogate key generation and date spine creation
- **Data Transformation**: Union relations with schema differences
- **Warehouse Agnosticism**: Date functions that work across platforms
- **Community Support**: Well-maintained, widely-used macros

By integrating `dbt_utils`, we reduce custom code, improve maintainability, and leverage battle-tested utilities from the dbt community.

