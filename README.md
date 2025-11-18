# ecommerce-analytics-dbt

**E-Commerce Analytics Platform (DBT Project)**

This repository contains a dbt project that transforms raw transactional data (users, products, orders, order_items, returns) into analytics-ready marts:
- `staging/` models: basic cleaning and casting
- `intermediate/` models: reusable business logic and aggregation
- `marts/` models: star-schema fact and dimensions, plus analytics

## Quick start

1. Install dbt (recommended: dbt-core + dbt-duckdb or dbt-postgres depending on your warehouse).
2. Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and adjust credentials.
3. Run:
```bash
dbt seed
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Project design

- **Pipeline**: sources (CSV seeds) → staging (raw cleaning) → intermediate (business logic) → marts (facts & dimensions).
- **Modeling approach**: Dimensional modeling (star schema) — fact table `fct_orders` and dimensions `dim_customers`, `dim_products`.
- **Warehouse**: Default example uses DuckDB (local), but code is ANSI SQL and compatible with Postgres.
- **Assumptions**:
  - `orders` can have status `completed` or `cancelled`.
  - Returns reference `order_id` and `order_item_id`.
  - Prices in `raw_order_items` are at the line level (price per unit).
  - Timezone: UTC for timestamps.

## Files of interest
- `seeds/` - raw CSVs (40 users, 30 products, 122 orders, 177 order items, 20 returns).
- `models/staging/` - staging models with field descriptions.
- `models/intermediate/` - business logic.
- `models/marts/` - analytical marts (fact + dims).
- `analyses/` - complex analytical queries demonstrating advanced SQL patterns.
- `macros/` - custom macro(s).
- `snapshots/` - Type-2 snapshot for product price changes.
- `tests/` - custom tests.
- `scripts/init_repo.sh` - creates a sample git commit history (run locally).

## Testing & Quality
- Generic tests (unique, not_null, relationships).
- Custom tests provided under `tests/` and `macros/tests/`.
- **dbt_utils integration**: Uses `dbt_utils.accepted_range` and `dbt_utils.unique_combination_of_columns` tests.
  See [docs/DBT_UTILS_INTEGRATION.md](docs/DBT_UTILS_INTEGRATION.md) for details.

## Performance Optimization

This project implements a comprehensive performance optimization strategy across all layers:

### Materialization Strategy
- **Staging Layer**: Views for lightweight transformations and data freshness
- **Intermediate Layer**: Tables to cache expensive aggregations
- **Mart Layer**: Tables for analytical queries, with `fct_orders` using incremental materialization

### Key Optimizations
- **Incremental Processing**: `fct_orders` uses incremental materialization with merge strategy, processing only the last 14 days by default
- **Pre-aggregation**: Intermediate models pre-compute metrics to avoid repeated calculations
- **Partitioning**: Recommended partitioning by `order_date` for fact tables (warehouse-specific)
- **Clustering**: Recommended clustering by `customer_id` and `order_date` for join performance

### Warehouse-Specific Recommendations
- **BigQuery**: Partition by `order_date`, cluster by `customer_id` and `order_date`
- **Snowflake**: Cluster by `order_date` and `customer_id`
- **Redshift**: Use KEY distribution on `customer_id`, sort by `order_date` and `customer_id`
- **Postgres**: Create indexes on foreign keys and date columns

For detailed optimization strategies, see [docs/PERFORMANCE_OPTIMIZATION.md](docs/PERFORMANCE_OPTIMIZATION.md).

## How to reproduce a shareable commit history
Run:
```bash
bash scripts/init_repo.sh
```
This will create a sequence of developer-style commits (local) to show the project evolution.

For more details see model-level docs in `models/` and `models/schema.yml` files.
