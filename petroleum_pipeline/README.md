# Petroleum Pipeline - dbt Project

## Overview
A dbt project for petroleum industry financial audit and operational data, built on **Snowflake** using the **Bronze → Silver → Gold** medallion architecture with a star schema in the Gold layer.

## Architecture

```
S3 (petroleumdb)          Snowflake (PETROLEUM_AUDIT.BRONZE)
       │                              │
       ▼                              ▼
┌─────────────────────────────────────────────┐
│              BRONZE LAYER                   │
│  (Incremental loads, raw data ingestion)    │
│                                             │
│  bronze_gl (watermark on posting_date)      │
│  bronze_vendor_master (unique: vendor_id)   │
│  bronze_customer_master (unique: customer_id)│
│  bronze_product_master (unique: product_id) │
│  bronze_subsidiary_master (unique: sub_id)  │
│  bronze_chart_of_accounts (unique: acct_no) │
│  bronze_well_production (view from seed)    │
│  bronze_pipeline_operations (view from seed)│
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│              SILVER LAYER                   │
│  (Cleaning, Standardization, Business Logic)│
│                                             │
│  gl_transactions_enriched (table)           │
│    → Joins GL with all master tables        │
│  vendor_payments (table)                    │
│    → Aggregated vendor payment amounts      │
│  silver_general_ledger (view)               │
│    → Full enrichment with business flags    │
│  silver_vendor_master (view)                │
│  silver_customer_master (view)              │
│    → Credit tier classification             │
│  silver_product_master (view)               │
│  silver_subsidiary_master (view)            │
│  silver_chart_of_accounts (view)            │
│    → Statement code (BS/IS/OTHER)           │
│  silver_well_production (view)              │
│    → Water cut %, total liquid calculation  │
│  silver_pipeline_operations (view)          │
│    → Pressure drop severity classification  │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│              GOLD LAYER                     │
│  (Star Schema - Facts & Dimensions)         │
│                                             │
│  FACT TABLES:                               │
│    fact_general_ledger                       │
│    fact_vendor_payments                      │
│    fact_revenue (with gross margin calc)     │
│    gold_daily_production_summary             │
│    gold_pipeline_health                      │
│                                             │
│  DIMENSION TABLES:                          │
│    dim_vendor                                │
│    dim_customer                              │
│    dim_product                               │
│    dim_subsidiary                            │
│    dim_chart_of_accounts                     │
│    dim_date (2020-2029, 3650 rows)           │
└─────────────────────────────────────────────┘
```

## Snowflake Configuration

| Setting       | Value                                    |
|---------------|------------------------------------------|
| Account       | KKTZVDG-MO79591                          |
| User          | RAHULTIWARY10                            |
| Role          | ACCOUNTADMIN                             |
| Warehouse     | COMPUTE_WH                               |
| Database      | PETROLEUM_DB                             |
| Source DB     | PETROLEUM_AUDIT                          |
| Auth          | Password (via env var SNOWFLAKE_PASSWORD) |

## Schemas

| Schema      | Purpose                              |
|-------------|--------------------------------------|
| BRONZE      | Raw/ingested data models             |
| SILVER      | Cleaned, enriched, standardized data |
| GOLD        | Star schema (facts & dimensions)     |
| SNAPSHOTS   | SCD Type 2 snapshots                 |
| PUBLIC      | Seeds and default schema             |

Custom schema macro (`macros/generate_schema_name.sql`) ensures models deploy to exact schema names (e.g., `bronze` not `public_bronze`).

## Data Sources

### PETROLEUM_AUDIT.BRONZE (Snowflake)
| Table                        | Description                    |
|------------------------------|--------------------------------|
| general_ledger_transactions  | GL journal entries (17 cols)   |
| vendor_master                | Vendor details (4 cols)        |
| customer_master              | Customer details (5 cols)      |
| product_master               | Product catalog (4 cols)       |
| subsidiary_master            | Subsidiary info (3 cols)       |
| chart_of_accounts            | COA definitions (6 cols)       |

### S3 (s3://petroleumdb/)
| File         | Description                         |
|--------------|-------------------------------------|
| vendors.csv  | New vendor data loaded via stage     |

Snowflake stage: `petroleum_stage` (points to `s3://petroleumdb/`)

### Seeds (Local CSV)
| Seed                      | Description                    |
|---------------------------|--------------------------------|
| seed_well_production      | Sample well production (12 rows)|
| seed_pipeline_operations  | Sample pipeline data (12 rows) |

## Incremental Loading Strategy

| Model                    | Unique Key       | Strategy                         |
|--------------------------|------------------|----------------------------------|
| bronze_gl                | journal_id       | Watermark on posting_date        |
| bronze_vendor_master     | vendor_id        | New rows (NOT IN existing)       |
| bronze_customer_master   | customer_id      | New rows (NOT IN existing)       |
| bronze_product_master    | product_id       | New rows (NOT IN existing)       |
| bronze_subsidiary_master | subsidiary_id    | New rows (NOT IN existing)       |
| bronze_chart_of_accounts | account_number   | New rows (NOT IN existing)       |

## Snapshots (SCD Type 2)

| Snapshot          | Strategy | Tracked Columns                        |
|-------------------|----------|----------------------------------------|
| vendor_snapshot   | check    | vendor_name, service_type, location    |

## Tests

### Schema Tests (28 total)
- **not_null**: Applied on all primary keys and critical columns
- **unique**: On gold layer primary keys (production_date, pipeline_id)
- **accepted_values**: Status fields (ACTIVE/SHUT_IN, OPERATIONAL/MAINTENANCE, NORMAL/MEDIUM/HIGH)
- **relationships**: GL account_number → chart_of_accounts

### Singular Tests (4 custom SQL tests)
| Test                          | Description                                      |
|-------------------------------|--------------------------------------------------|
| trial_balance                 | Debits must equal credits per journal entry       |
| assert_no_negative_oil_volume | No negative oil production values                 |
| assert_no_negative_pressure_drop | Outlet pressure cannot exceed inlet pressure  |
| assert_water_cut_in_range     | Water cut percentage must be 0-100%               |

## Project Structure

```
petroleum_pipeline/
├── dbt_project.yml
├── models/
│   ├── bronze/
│   │   ├── _bronze_schema.yml
│   │   ├── _bronze_sources.yml
│   │   ├── bronze_gl.sql
│   │   ├── bronze_vendor_master.sql
│   │   ├── bronze_customer_master.sql
│   │   ├── bronze_product_master.sql
│   │   ├── bronze_subsidiary_master.sql
│   │   ├── bronze_chart_of_accounts.sql
│   │   ├── bronze_general_ledger_transactions.sql
│   │   ├── bronze_well_production.sql
│   │   └── bronze_pipeline_operations.sql
│   ├── silver/
│   │   ├── _silver_schema.yml
│   │   ├── gl_transactions_enriched.sql
│   │   ├── vendor_payments.sql
│   │   ├── silver_general_ledger.sql
│   │   ├── silver_vendor_master.sql
│   │   ├── silver_customer_master.sql
│   │   ├── silver_product_master.sql
│   │   ├── silver_subsidiary_master.sql
│   │   ├── silver_chart_of_accounts.sql
│   │   ├── silver_well_production.sql
│   │   └── silver_pipeline_operations.sql
│   └── gold/
│       ├── _gold_schema.yml
│       ├── fact_general_ledger.sql
│       ├── fact_vendor_payments.sql
│       ├── fact_revenue.sql
│       ├── gold_daily_production_summary.sql
│       ├── gold_pipeline_health.sql
│       ├── dim_vendor.sql
│       ├── dim_customer.sql
│       ├── dim_product.sql
│       ├── dim_subsidiary.sql
│       ├── dim_chart_of_accounts.sql
│       └── dim_date.sql
├── snapshots/
│   └── vendor_snapshot.sql
├── seeds/
│   ├── seed_well_production.csv
│   ├── seed_pipeline_operations.csv
│   └── seed_properties.yml
├── tests/
│   ├── trial_balance.sql
│   ├── assert_no_negative_oil_volume.sql
│   ├── assert_no_negative_pressure_drop.sql
│   └── assert_water_cut_in_range.sql
├── macros/
│   └── generate_schema_name.sql
└── analyses/
```

## How to Run

```bash
# Set password
export SNOWFLAKE_PASSWORD='<your_password>'

# Full build (seeds + models + tests + snapshots)
dbt build

# Individual commands
dbt seed                          # Load seed data
dbt run                           # Run all models
dbt test                          # Run all tests
dbt snapshot                      # Run SCD Type 2 snapshots

# Selective runs
dbt run --select bronze_vendor_master       # Single model
dbt run --select bronze_vendor_master+      # Model + downstream
dbt run --full-refresh --select bronze_gl   # Full refresh incremental

# Load new data from S3
# Data lands in s3://petroleumdb/ → Snowflake stage: petroleum_stage
# Then run incremental models to pick up new rows
```

## Git Repository
https://github.com/tiwaryrahul/Petroleum-.git
