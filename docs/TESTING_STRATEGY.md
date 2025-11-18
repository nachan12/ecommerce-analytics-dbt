# Testing Strategy

This document outlines the comprehensive testing strategy for the ecommerce analytics dbt project, including the types of tests implemented and the data quality issues they guard against.

## Overview

Our testing strategy follows a multi-layered approach to ensure data quality, referential integrity, and business logic correctness across all layers of the data transformation pipeline.

## Test Categories

### 1. Generic Tests (Schema Tests)

Generic tests are reusable tests defined in schema.yml files that can be applied to any column or model. They validate structural and referential integrity.

#### 1.1 Unique Tests
- **Purpose**: Ensures primary keys and unique identifiers are truly unique
- **Applied to**: 
  - All primary key columns (user_id, product_id, order_id, etc.)
- **Guards against**: Duplicate records, data ingestion errors, ETL process failures

#### 1.2 Not Null Tests
- **Purpose**: Validates that critical fields are never null
- **Applied to**: 
  - Primary keys
  - Foreign keys
  - Required business fields (email, order_date, return_date, etc.)
- **Guards against**: Missing data, incomplete records, data quality issues

#### 1.3 Relationship Tests
- **Purpose**: Validates referential integrity between tables
- **Applied to**: 
  - Foreign key relationships (user_id → stg_users, order_id → stg_orders, etc.)
- **Guards against**: Orphaned records, broken relationships, data integration issues

#### 1.4 Accepted Values Tests
- **Purpose**: Ensures categorical fields only contain valid values
- **Applied to**: 
  - Order status: ['completed', 'cancelled', 'pending', 'processing']
  - Return reasons: ['defective', 'changed_mind', 'wrong_item', 'damaged_shipping']
  - Customer segments: ['Premium', 'Standard', 'Basic']
  - Loyalty tiers: ['VIP', 'GROWTH', 'NEW', 'PROSPECT']
  - Engagement bands: ['LOYAL', 'ACTIVE', 'NEW', 'INACTIVE']
- **Guards against**: Invalid enum values, data entry errors, inconsistent categorization

#### 1.5 Accepted Range Tests (Custom Generic Test)
- **Purpose**: Validates that numeric values fall within acceptable bounds
- **Implementation**: Custom generic test in `macros/generic/test_accepted_range.sql`
- **Applied to**: 
  - Quantity: min_value = 1 (must be positive)
  - Unit price: min_value = 0 (cannot be negative)
  - Unit cost: min_value = 0 (cannot be negative)
  - Refund amount: min_value = 0 (cannot be negative)
  - Return rate: min_value = 0, max_value = 1 (must be between 0 and 1)
  - Profit margin: min_value = -1, max_value = 1 (must be between -100% and 100%)
- **Guards against**: Negative quantities, invalid percentages, calculation errors, data entry mistakes

### 2. Singular Tests (Custom Data Tests)

Singular tests are custom SQL queries that validate specific business logic and data quality rules. They are stored in the `tests/` directory.

#### 2.1 Net Sales Consistency Test
- **File**: `tests/test_net_sales_consistency.sql`
- **Purpose**: Validates that net_sales = gross_item_sales - total_refund_amount
- **Business Logic**: Ensures our revenue calculation is mathematically correct
- **Guards against**: Calculation errors, data transformation bugs, aggregation mistakes

#### 2.2 Return Date After Order Date Test
- **File**: `tests/test_return_date_after_order_date.sql`
- **Purpose**: Validates that returns cannot occur before the order was placed
- **Business Logic**: Ensures temporal consistency in return processing
- **Guards against**: Data entry errors, system clock issues, logical inconsistencies

#### 2.3 Gross Item Sales Calculation Test
- **File**: `tests/test_gross_item_sales_calculation.sql`
- **Purpose**: Validates that gross_item_sales = quantity * unit_price
- **Business Logic**: Ensures calculated fields match source data
- **Guards against**: Calculation errors, data corruption, transformation bugs

## Testing Layers

### Staging Layer Tests
- **Focus**: Data quality, referential integrity, value validation
- **Tests**: Unique, not_null, relationships, accepted_values, accepted_range
- **Purpose**: Catch issues early in the pipeline before they propagate

### Intermediate Layer Tests
- **Focus**: Business logic correctness, calculation accuracy
- **Tests**: Singular tests for calculated metrics
- **Purpose**: Validate transformations and aggregations

### Mart Layer Tests
- **Focus**: Final output quality, business rule compliance
- **Tests**: Unique, relationships, accepted_values, accepted_range
- **Purpose**: Ensure downstream consumers receive high-quality data

## Data Quality Issues Guarded Against

### 1. Structural Issues
- **Duplicate records**: Caught by unique tests
- **Missing required fields**: Caught by not_null tests
- **Broken relationships**: Caught by relationship tests

### 2. Data Integrity Issues
- **Invalid categorical values**: Caught by accepted_values tests
- **Out-of-range numeric values**: Caught by accepted_range tests
- **Temporal inconsistencies**: Caught by singular tests (return_date_after_order_date)

### 3. Business Logic Issues
- **Calculation errors**: Caught by singular tests (net_sales_consistency, gross_item_sales_calculation)
- **Aggregation mistakes**: Caught by singular tests
- **Formula inconsistencies**: Caught by singular tests

### 4. Data Entry Issues
- **Negative quantities/prices**: Caught by accepted_range tests
- **Invalid status codes**: Caught by accepted_values tests
- **Impossible dates**: Caught by singular tests

## Running Tests

### Run all tests
```bash
dbt test
```

### Run tests for a specific model
```bash
dbt test --select stg_orders
```

### Run only generic tests
```bash
dbt test --select test_type:generic
```

### Run only singular tests
```bash
dbt test --select test_type:singular
```

### Run tests for a specific layer
```bash
dbt test --select Staging
dbt test --select Intermediate
dbt test --select Mart
```

## Test Maintenance

### Adding New Tests
1. **Generic tests**: Add to schema.yml files in the appropriate model directory
2. **Singular tests**: Create new .sql files in the `tests/` directory
3. **Custom generic tests**: Add to `macros/generic/` directory

### Updating Tests
- Review test failures regularly
- Update accepted_values lists when business rules change
- Adjust range tests when business thresholds change
- Document any test exceptions or known issues

## Best Practices

1. **Test Early**: Add tests as you build models, not after
2. **Test Comprehensively**: Cover all critical business logic
3. **Test Relationships**: Ensure referential integrity across layers
4. **Document Exceptions**: If a test must be skipped, document why
5. **Review Failures**: Investigate and fix test failures promptly
6. **Update Tests**: Keep tests in sync with business rule changes

## Future Enhancements

Potential additions to the testing strategy:
- Data freshness tests (already configured in sources.yml)
- Volume tests (row count thresholds)
- Distribution tests (statistical validation)
- Custom tests for specific business rules
- Integration tests for end-to-end data flow

