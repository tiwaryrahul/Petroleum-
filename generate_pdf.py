from fpdf import FPDF
import os

class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Petroleum Pipeline - dbt Project Documentation", align="C", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def chapter_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(0, 51, 102)
        self.cell(0, 12, title, new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def section_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(0, 102, 153)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def code_block(self, code, title=""):
        if title:
            self.set_font("Helvetica", "B", 9)
            self.set_text_color(0, 0, 0)
            self.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
            self.ln(1)
        self.set_fill_color(245, 245, 245)
        self.set_font("Courier", "", 7.5)
        self.set_text_color(30, 30, 30)
        lines = code.strip().split("\n")
        for line in lines:
            safe_line = line.replace("\t", "    ")
            self.cell(0, 4, safe_line, new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(4)

    def table_header(self, cols, widths):
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 51, 102)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 7, col, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(0, 0, 0)

    def table_row(self, cols, widths):
        self.set_font("Helvetica", "", 8)
        self.set_fill_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 6, str(col), border=1, align="L")
        self.ln()


def read_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except:
        return "-- File not found --"


pdf = PDF()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=20)

BASE = "c:/dev_work/petroleum_pipeline/petroleum_pipeline"
AIRFLOW = "c:/dev_work/petroleum_pipeline/airflow"
GH = "c:/dev_work/petroleum_pipeline/.github/workflows"

# ============ COVER PAGE ============
pdf.add_page()
pdf.ln(40)
pdf.set_font("Helvetica", "B", 28)
pdf.set_text_color(0, 51, 102)
pdf.cell(0, 15, "Petroleum Pipeline", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.set_font("Helvetica", "B", 18)
pdf.set_text_color(0, 102, 153)
pdf.cell(0, 12, "dbt + Snowflake Data Pipeline", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(10)
pdf.set_font("Helvetica", "", 12)
pdf.set_text_color(80, 80, 80)
pdf.cell(0, 8, "Bronze - Silver - Gold Medallion Architecture", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 8, "Star Schema (Facts & Dimensions)", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 8, "Incremental Loading | SCD Type 2 Snapshots", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 8, "CI/CD with GitHub Actions | Airflow Orchestration", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(20)
pdf.set_font("Helvetica", "", 10)
pdf.cell(0, 8, "Author: Rahul Tiwary", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 8, "Repository: github.com/tiwaryrahul/Petroleum-", align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 8, "Date: March 2026", align="C", new_x="LMARGIN", new_y="NEXT")

# ============ TABLE OF CONTENTS ============
pdf.add_page()
pdf.chapter_title("Table of Contents")
toc = [
    "1. Project Overview & Architecture",
    "2. Snowflake Configuration",
    "3. Project Structure",
    "4. Bronze Layer - Raw Data Ingestion",
    "5. Silver Layer - Cleaning & Enrichment",
    "6. Gold Layer - Star Schema",
    "7. Snapshots (SCD Type 2)",
    "8. Tests & Data Quality",
    "9. Seeds - Sample Data",
    "10. Macros",
    "11. CI/CD - GitHub Actions",
    "12. Orchestration - Apache Airflow",
    "13. End-to-End Workflow",
    "14. Commands Reference",
]
for item in toc:
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7, item, new_x="LMARGIN", new_y="NEXT")

# ============ 1. PROJECT OVERVIEW ============
pdf.add_page()
pdf.chapter_title("1. Project Overview & Architecture")
pdf.body_text(
    "This project implements a complete data pipeline for petroleum industry financial audit "
    "and operational data using dbt (Data Build Tool) on Snowflake. The architecture follows "
    "the Bronze-Silver-Gold medallion pattern with a star schema in the Gold layer."
)
pdf.section_title("Architecture Flow")
pdf.body_text(
    "S3 (petroleumdb) + Snowflake (PETROLEUM_AUDIT.BRONZE)\n"
    "        |\n"
    "    BRONZE LAYER (Incremental loads, raw data ingestion)\n"
    "        |\n"
    "    SILVER LAYER (Cleaning, Standardization, Business Logic)\n"
    "        |\n"
    "    GOLD LAYER (Star Schema - Facts & Dimensions)\n"
)
pdf.section_title("Key Features")
pdf.body_text(
    "- Medallion Architecture: Bronze -> Silver -> Gold\n"
    "- Incremental Loading: Watermark & key-based strategies\n"
    "- Star Schema: 5 Fact tables + 6 Dimension tables\n"
    "- SCD Type 2: Vendor snapshot with check strategy\n"
    "- Data Quality: 28 schema tests + 4 custom SQL tests\n"
    "- CI/CD: GitHub Actions (daily scheduled + manual trigger)\n"
    "- Orchestration: Apache Airflow DAG\n"
    "- Custom Schema Macro: Clean schema names (bronze/silver/gold)"
)

# ============ 2. SNOWFLAKE CONFIG ============
pdf.add_page()
pdf.chapter_title("2. Snowflake Configuration")
pdf.section_title("Connection Details")
widths = [50, 80]
pdf.table_header(["Setting", "Value"], widths)
for row in [
    ["Account", "KKTZVDG-MO79591"],
    ["User", "RAHULTIWARY10"],
    ["Role", "ACCOUNTADMIN"],
    ["Warehouse", "COMPUTE_WH"],
    ["Database", "PETROLEUM_DB"],
    ["Source DB", "PETROLEUM_AUDIT"],
    ["Auth", "Password (env var)"],
]:
    pdf.table_row(row, widths)
pdf.ln(5)

pdf.section_title("Schemas in PETROLEUM_DB")
widths2 = [50, 100]
pdf.table_header(["Schema", "Purpose"], widths2)
for row in [
    ["BRONZE", "Raw/ingested data models"],
    ["SILVER", "Cleaned, enriched data"],
    ["GOLD", "Star schema (facts & dims)"],
    ["SNAPSHOTS", "SCD Type 2 snapshots"],
    ["PUBLIC", "Seeds and default schema"],
]:
    pdf.table_row(row, widths2)
pdf.ln(5)

pdf.section_title("profiles.yml")
pdf.code_block(read_file(f"{BASE}/profiles.yml"))

pdf.section_title("dbt_project.yml")
pdf.code_block(read_file(f"{BASE}/dbt_project.yml"))

# ============ 3. PROJECT STRUCTURE ============
pdf.add_page()
pdf.chapter_title("3. Project Structure")
pdf.code_block("""petroleum_pipeline/
|-- dbt_project.yml
|-- profiles.yml
|-- models/
|   |-- bronze/
|   |   |-- _bronze_schema.yml
|   |   |-- _bronze_sources.yml
|   |   |-- bronze_gl.sql
|   |   |-- bronze_vendor_master.sql
|   |   |-- bronze_customer_master.sql
|   |   |-- bronze_product_master.sql
|   |   |-- bronze_subsidiary_master.sql
|   |   |-- bronze_chart_of_accounts.sql
|   |   |-- bronze_general_ledger_transactions.sql
|   |   |-- bronze_well_production.sql
|   |   |-- bronze_pipeline_operations.sql
|   |-- silver/
|   |   |-- _silver_schema.yml
|   |   |-- gl_transactions_enriched.sql
|   |   |-- vendor_payments.sql
|   |   |-- silver_general_ledger.sql
|   |   |-- silver_vendor_master.sql
|   |   |-- silver_customer_master.sql
|   |   |-- silver_product_master.sql
|   |   |-- silver_subsidiary_master.sql
|   |   |-- silver_chart_of_accounts.sql
|   |   |-- silver_well_production.sql
|   |   |-- silver_pipeline_operations.sql
|   |-- gold/
|       |-- _gold_schema.yml
|       |-- fact_general_ledger.sql
|       |-- fact_vendor_payments.sql
|       |-- fact_revenue.sql
|       |-- gold_daily_production_summary.sql
|       |-- gold_pipeline_health.sql
|       |-- dim_vendor.sql
|       |-- dim_customer.sql
|       |-- dim_product.sql
|       |-- dim_subsidiary.sql
|       |-- dim_chart_of_accounts.sql
|       |-- dim_date.sql
|-- snapshots/
|   |-- vendor_snapshot.sql
|-- seeds/
|   |-- seed_well_production.csv
|   |-- seed_pipeline_operations.csv
|   |-- seed_properties.yml
|-- tests/
|   |-- trial_balance.sql
|   |-- assert_no_negative_oil_volume.sql
|   |-- assert_no_negative_pressure_drop.sql
|   |-- assert_water_cut_in_range.sql
|-- macros/
|   |-- generate_schema_name.sql
|-- analyses/
""")

# ============ 4. BRONZE LAYER ============
pdf.add_page()
pdf.chapter_title("4. Bronze Layer - Raw Data Ingestion")
pdf.body_text(
    "The Bronze layer ingests raw data from PETROLEUM_AUDIT.BRONZE using incremental models. "
    "Each model uses a unique key to prevent duplicates. The GL model uses watermark-based "
    "incremental loading on posting_date."
)

pdf.section_title("Source Definition")
pdf.code_block(read_file(f"{BASE}/models/bronze/_bronze_sources.yml"), "_bronze_sources.yml")

pdf.section_title("Schema & Tests")
pdf.code_block(read_file(f"{BASE}/models/bronze/_bronze_schema.yml"), "_bronze_schema.yml")

bronze_models = [
    ("bronze_gl.sql", "Watermark incremental on posting_date"),
    ("bronze_vendor_master.sql", "Incremental by vendor_id"),
    ("bronze_customer_master.sql", "Incremental by customer_id"),
    ("bronze_product_master.sql", "Incremental by product_id"),
    ("bronze_subsidiary_master.sql", "Incremental by subsidiary_id"),
    ("bronze_chart_of_accounts.sql", "Incremental by account_number"),
    ("bronze_general_ledger_transactions.sql", "View passthrough"),
    ("bronze_well_production.sql", "View from seed"),
    ("bronze_pipeline_operations.sql", "View from seed"),
]

for fname, desc in bronze_models:
    pdf.add_page()
    pdf.section_title(f"{fname} - {desc}")
    pdf.code_block(read_file(f"{BASE}/models/bronze/{fname}"), fname)

# ============ 5. SILVER LAYER ============
pdf.add_page()
pdf.chapter_title("5. Silver Layer - Cleaning & Enrichment")
pdf.body_text(
    "The Silver layer performs data cleaning (null handling, trimming), standardization "
    "(uppercase, initcap), and business logic (calculated fields, classifications, joins)."
)

pdf.section_title("Schema & Tests")
pdf.code_block(read_file(f"{BASE}/models/silver/_silver_schema.yml"), "_silver_schema.yml")

silver_models = [
    "gl_transactions_enriched.sql",
    "vendor_payments.sql",
    "silver_general_ledger.sql",
    "silver_vendor_master.sql",
    "silver_customer_master.sql",
    "silver_product_master.sql",
    "silver_subsidiary_master.sql",
    "silver_chart_of_accounts.sql",
    "silver_well_production.sql",
    "silver_pipeline_operations.sql",
]

for fname in silver_models:
    pdf.add_page()
    pdf.section_title(fname)
    pdf.code_block(read_file(f"{BASE}/models/silver/{fname}"), fname)

# ============ 6. GOLD LAYER ============
pdf.add_page()
pdf.chapter_title("6. Gold Layer - Star Schema")
pdf.body_text(
    "The Gold layer implements a star schema with Fact and Dimension tables, "
    "materialized as tables for optimal query performance."
)

pdf.section_title("Schema & Tests")
pdf.code_block(read_file(f"{BASE}/models/gold/_gold_schema.yml"), "_gold_schema.yml")

gold_models = [
    "fact_general_ledger.sql",
    "fact_vendor_payments.sql",
    "fact_revenue.sql",
    "gold_daily_production_summary.sql",
    "gold_pipeline_health.sql",
    "dim_vendor.sql",
    "dim_customer.sql",
    "dim_product.sql",
    "dim_subsidiary.sql",
    "dim_chart_of_accounts.sql",
    "dim_date.sql",
]

for fname in gold_models:
    pdf.add_page()
    pdf.section_title(fname)
    pdf.code_block(read_file(f"{BASE}/models/gold/{fname}"), fname)

# ============ 7. SNAPSHOTS ============
pdf.add_page()
pdf.chapter_title("7. Snapshots (SCD Type 2)")
pdf.body_text(
    "The vendor_snapshot captures slowly changing dimensions using the 'check' strategy. "
    "It monitors vendor_name, service_type, and location for changes and maintains history "
    "with dbt_valid_from and dbt_valid_to columns."
)
pdf.code_block(read_file(f"{BASE}/snapshots/vendor_snapshot.sql"), "vendor_snapshot.sql")

# ============ 8. TESTS ============
pdf.add_page()
pdf.chapter_title("8. Tests & Data Quality")
pdf.body_text("28 schema tests + 4 custom singular tests ensure data quality across all layers.")

pdf.section_title("Schema Tests Summary")
widths3 = [50, 50, 60]
pdf.table_header(["Test Type", "Count", "Applied To"], widths3)
for row in [
    ["not_null", "18", "All primary keys & critical columns"],
    ["unique", "2", "Gold layer primary keys"],
    ["accepted_values", "3", "Status & severity fields"],
    ["relationships", "1", "GL account_number -> COA"],
]:
    pdf.table_row(row, widths3)
pdf.ln(5)

pdf.section_title("Custom Tests")
tests = [
    ("trial_balance.sql", "Debits must equal credits per journal entry"),
    ("assert_no_negative_oil_volume.sql", "No negative oil production"),
    ("assert_no_negative_pressure_drop.sql", "Outlet pressure <= inlet pressure"),
    ("assert_water_cut_in_range.sql", "Water cut 0-100%"),
]
for fname, desc in tests:
    pdf.section_title(f"{fname} - {desc}")
    pdf.code_block(read_file(f"{BASE}/tests/{fname}"), fname)

# ============ 9. SEEDS ============
pdf.add_page()
pdf.chapter_title("9. Seeds - Sample Data")
pdf.code_block(read_file(f"{BASE}/seeds/seed_properties.yml"), "seed_properties.yml")
pdf.code_block(read_file(f"{BASE}/seeds/seed_well_production.csv"), "seed_well_production.csv")
pdf.add_page()
pdf.code_block(read_file(f"{BASE}/seeds/seed_pipeline_operations.csv"), "seed_pipeline_operations.csv")

# ============ 10. MACROS ============
pdf.add_page()
pdf.chapter_title("10. Macros")
pdf.body_text(
    "Custom generate_schema_name macro overrides dbt's default behavior of prefixing "
    "schemas with target_schema. Models deploy to exact schema names: bronze, silver, gold."
)
pdf.code_block(read_file(f"{BASE}/macros/generate_schema_name.sql"), "generate_schema_name.sql")

# ============ 11. CI/CD ============
pdf.add_page()
pdf.chapter_title("11. CI/CD - GitHub Actions")
pdf.body_text(
    "The pipeline is automated via GitHub Actions with daily scheduled runs at 6 AM UTC "
    "and manual trigger support with optional full-refresh. Each layer runs and tests "
    "sequentially: Seed -> Bronze -> Silver -> Gold."
)
pdf.code_block(read_file(f"{GH}/dbt_pipeline.yml"), "dbt_pipeline.yml")

# ============ 12. AIRFLOW ============
pdf.add_page()
pdf.chapter_title("12. Orchestration - Apache Airflow")
pdf.body_text(
    "An Airflow DAG orchestrates the dbt pipeline with 10 tasks running sequentially. "
    "Docker Compose is provided for local Airflow deployment."
)
pdf.section_title("Airflow DAG")
pdf.code_block(read_file(f"{AIRFLOW}/dags/petroleum_pipeline_dag.py"), "petroleum_pipeline_dag.py")
pdf.add_page()
pdf.section_title("Docker Compose")
pdf.code_block(read_file(f"{AIRFLOW}/docker-compose.yml"), "docker-compose.yml")

# ============ 13. END TO END WORKFLOW ============
pdf.add_page()
pdf.chapter_title("13. End-to-End Workflow")

pdf.section_title("Step 1: Environment Setup")
pdf.code_block("""# Install dbt
pip install dbt-snowflake

# Set Snowflake password
export SNOWFLAKE_PASSWORD='<your_password>'

# Verify connection
cd petroleum_pipeline
dbt debug""")

pdf.section_title("Step 2: Load Seed Data")
pdf.code_block("""dbt seed
# Loads seed_well_production (12 rows)
# Loads seed_pipeline_operations (12 rows)""")

pdf.section_title("Step 3: Run Snapshots (SCD Type 2)")
pdf.code_block("""dbt snapshot
# Captures vendor_master state for change tracking""")

pdf.section_title("Step 4: Build Bronze Layer (Incremental)")
pdf.code_block("""dbt run --select models/bronze
# Incremental loads from PETROLEUM_AUDIT.BRONZE
# 6 incremental models + 3 views

dbt test --select models/bronze
# Tests: not_null, accepted_values on status fields""")

pdf.section_title("Step 5: Build Silver Layer (Transform)")
pdf.code_block("""dbt run --select models/silver
# Joins, cleaning, standardization, business logic
# gl_transactions_enriched, vendor_payments, etc.

dbt test --select models/silver
# Tests: not_null, relationships, trial_balance""")

pdf.section_title("Step 6: Build Gold Layer (Star Schema)")
pdf.code_block("""dbt run --select models/gold
# Fact tables: fact_general_ledger, fact_revenue, etc.
# Dimension tables: dim_vendor, dim_customer, dim_date, etc.

dbt test --select models/gold
# Tests: unique, not_null on primary keys""")

pdf.section_title("Step 7: Full Build (All at Once)")
pdf.code_block("""dbt build
# Runs: seeds + models + tests + snapshots
# Result: 61/61 PASS""")

pdf.section_title("Step 8: Load New Data from S3")
pdf.code_block("""# New data arrives in s3://petroleumdb/vendors.csv
# Snowflake stage: petroleum_stage

COPY INTO vendor_master
FROM @petroleum_stage/vendors.csv
FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1);

# Run incremental model + downstream
dbt run --select bronze_vendor_master+""")

pdf.section_title("Step 9: CI/CD Automation")
pdf.code_block("""# GitHub Actions runs daily at 6 AM UTC
# Manual trigger: GitHub Actions > Run workflow
# Secret required: SNOWFLAKE_PASSWORD in repo settings""")

# ============ 14. COMMANDS REFERENCE ============
pdf.add_page()
pdf.chapter_title("14. Commands Reference")
widths4 = [80, 80]
pdf.table_header(["Command", "Description"], widths4)
for row in [
    ["dbt debug", "Test Snowflake connection"],
    ["dbt seed", "Load CSV seed data"],
    ["dbt run", "Run all models"],
    ["dbt test", "Run all tests"],
    ["dbt snapshot", "Run SCD Type 2 snapshots"],
    ["dbt build", "Seed + Run + Test + Snapshot"],
    ["dbt run --select model_name", "Run specific model"],
    ["dbt run --select model_name+", "Model + all downstream"],
    ["dbt run --full-refresh", "Rebuild incremental models"],
    ["dbt docs generate", "Generate documentation"],
    ["dbt docs serve", "Serve docs locally"],
]:
    pdf.table_row(row, widths4)

# ============ 15. ARCHITECTURE DIAGRAM ============
pdf.add_page()
pdf.chapter_title("15. Architecture Diagram")
pdf.body_text("Complete end-to-end architecture of the Petroleum Pipeline project:")
pdf.ln(2)
pdf.image("c:/dev_work/petroleum_pipeline/Architecture_Diagram.png", x=10, w=190)

# ============ SAVE ============
output_path = "c:/dev_work/petroleum_pipeline/Petroleum_Pipeline_Documentation.pdf"
pdf.output(output_path)
print(f"PDF generated: {output_path}")
