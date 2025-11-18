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
- `macros/` - custom macro(s).
- `snapshots/` - Type-2 snapshot for product price changes.
- `tests/` - custom tests.
- `scripts/init_repo.sh` - creates a sample git commit history (run locally).

## Testing & Quality
- Generic tests (unique, not_null, relationships).
- Custom tests provided under `tests/` and `macros/tests/`.

## Performance considerations
- Materializations chosen: staging (view), intermediate (ephemeral/table), marts (table/incremental).
- Incremental model example included (`fct_orders` incremental).
- If using Postgres/BigQuery/Snowflake, consider partitioning and clustering on order_date and customer_id.

## How to reproduce a shareable commit history
Run:
```bash
bash scripts/init_repo.sh
```
This will create a sequence of developer-style commits (local) to show the project evolution.

For more details see model-level docs in `models/` and `models/schema.yml` files.
