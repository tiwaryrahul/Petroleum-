import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(1, 1, figsize=(22, 28))
ax.set_xlim(0, 22)
ax.set_ylim(0, 28)
ax.axis('off')
fig.patch.set_facecolor('white')

# Colors
C_TITLE = '#0A1628'
C_S3 = '#FF9900'
C_S3_BG = '#FFF3E0'
C_SF = '#29B5E8'
C_SF_BG = '#E3F2FD'
C_BRONZE = '#CD7F32'
C_BRONZE_BG = '#FFF8F0'
C_SILVER = '#808080'
C_SILVER_BG = '#F5F5F5'
C_GOLD = '#FFD700'
C_GOLD_BG = '#FFFDE7'
C_TEST = '#4CAF50'
C_TEST_BG = '#E8F5E9'
C_CICD = '#9C27B0'
C_CICD_BG = '#F3E5F5'
C_SNAP = '#E91E63'
C_SNAP_BG = '#FCE4EC'
C_ARROW = '#37474F'
C_WHITE = '#FFFFFF'

def draw_box(x, y, w, h, color, bg, title, items=None, radius=0.3):
    box = FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad={radius}",
                         facecolor=bg, edgecolor=color, linewidth=2.5)
    ax.add_patch(box)
    ax.text(x + w/2, y + h - 0.35, title, ha='center', va='top',
            fontsize=11, fontweight='bold', color=color)
    if items:
        for i, item in enumerate(items):
            ax.text(x + 0.3, y + h - 0.85 - i*0.38, item, ha='left', va='top',
                    fontsize=7.5, color='#333333', fontfamily='monospace')

def draw_arrow(x1, y1, x2, y2, label=""):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=C_ARROW, lw=2))
    if label:
        mx, my = (x1+x2)/2, (y1+y2)/2
        ax.text(mx + 0.15, my, label, fontsize=7, color=C_ARROW, fontstyle='italic')

# ==================== TITLE ====================
ax.text(11, 27.5, 'Petroleum Pipeline - End-to-End Architecture',
        ha='center', va='center', fontsize=20, fontweight='bold', color=C_TITLE)
ax.text(11, 27.05, 'dbt + Snowflake | Bronze-Silver-Gold Medallion | Star Schema',
        ha='center', va='center', fontsize=11, color='#546E7A')

# ==================== DATA SOURCES (Top) ====================
# S3 Bucket
draw_box(1, 25.2, 4.5, 1.5, C_S3, C_S3_BG, 'AWS S3 Bucket',
         ['s3://petroleumdb/', 'vendors.csv, ...', 'Stage: petroleum_stage'])

# Snowflake Source DB
draw_box(7, 25.2, 7.5, 1.5, C_SF, C_SF_BG, 'PETROLEUM_AUDIT.BRONZE (Source)',
         ['general_ledger_transactions', 'vendor_master | customer_master',
          'product_master | subsidiary_master', 'chart_of_accounts'])

# Seeds
draw_box(16, 25.2, 5, 1.5, '#795548', '#EFEBE9', 'Seeds (CSV)',
         ['seed_well_production.csv', 'seed_pipeline_operations.csv', '24 sample rows'])

# Arrows from sources to Bronze
draw_arrow(3.25, 25.2, 5.5, 23.8, "COPY INTO")
draw_arrow(10.75, 25.2, 10, 23.8, "source()")
draw_arrow(18.5, 25.2, 15, 23.8, "ref()")

# ==================== BRONZE LAYER ====================
draw_box(1, 20.5, 19, 3.3, C_BRONZE, C_BRONZE_BG, 'BRONZE LAYER  |  Schema: BRONZE  |  Incremental Loading', [])

# Bronze sub-boxes
draw_box(1.5, 20.7, 5.5, 2.3, C_BRONZE, C_WHITE, 'Incremental Models', [
    'bronze_gl (watermark: posting_date)',
    'bronze_vendor_master (key: vendor_id)',
    'bronze_customer_master (key: customer_id)',
    'bronze_product_master (key: product_id)',
    'bronze_subsidiary_master (key: subsidiary_id)',
])

draw_box(7.5, 20.7, 5.5, 2.3, C_BRONZE, C_WHITE, 'View Models', [
    'bronze_general_ledger_transactions',
    'bronze_well_production',
    'bronze_pipeline_operations',
    '',
    'bronze_chart_of_accounts (key: acct_no)',
])

draw_box(13.5, 20.7, 6, 2.3, C_TEST, C_TEST_BG, 'Bronze Tests', [
    'not_null: well_id, pipeline_id, ...',
    'accepted_values: status fields',
    '  ACTIVE | SHUT_IN | ABANDONED',
    '  OPERATIONAL | MAINTENANCE',
    '',
])

# Arrow Bronze -> Silver
draw_arrow(10.5, 20.5, 10.5, 19.3, "ref()")

# ==================== SILVER LAYER ====================
draw_box(1, 15.5, 19, 3.8, C_SILVER, C_SILVER_BG, 'SILVER LAYER  |  Schema: SILVER  |  Cleaning + Standardization + Business Logic', [])

draw_box(1.5, 15.7, 6, 2.8, C_SILVER, C_WHITE, 'Enriched Models (Tables)', [
    'gl_transactions_enriched',
    '  -> Joins GL + COA + Customer',
    '     + Vendor + Product',
    'vendor_payments',
    '  -> Aggregated payments by vendor',
    '',
])

draw_box(8, 15.7, 5.5, 2.8, C_SILVER, C_WHITE, 'Cleansed Masters (Views)', [
    'silver_general_ledger',
    'silver_vendor_master',
    'silver_customer_master (credit_tier)',
    'silver_product_master',
    'silver_subsidiary_master',
    'silver_chart_of_accounts (stmt_code)',
])

draw_box(14, 15.7, 5.5, 2.8, C_SILVER, C_WHITE, 'Operational Models (Views)', [
    'silver_well_production',
    '  -> water_cut_pct, total_liquid',
    'silver_pipeline_operations',
    '  -> pressure_drop_severity',
    '     HIGH | MEDIUM | NORMAL',
    '',
])

# Arrow Silver -> Gold
draw_arrow(10.5, 15.5, 10.5, 14.3, "ref()")

# ==================== GOLD LAYER ====================
draw_box(1, 10.5, 19, 3.8, C_GOLD, C_GOLD_BG, 'GOLD LAYER  |  Schema: GOLD  |  Star Schema (Facts & Dimensions)', [])

draw_box(1.5, 10.7, 8, 2.8, '#B8860B', C_WHITE, 'FACT TABLES (Materialized: table)', [
    'fact_general_ledger (net_amount calc)',
    'fact_vendor_payments (debit > 0)',
    'fact_revenue (gross_margin calc)',
    'gold_daily_production_summary',
    'gold_pipeline_health (severity counts)',
    '',
])

draw_box(10, 10.7, 9.5, 2.8, '#B8860B', C_WHITE, 'DIMENSION TABLES (Materialized: table)', [
    'dim_vendor (7 -> 15 rows after S3 load)',
    'dim_customer (7 rows, credit_limit)',
    'dim_product (6 rows, cost_per_barrel)',
    'dim_subsidiary (3 rows, currency)',
    'dim_chart_of_accounts (16 rows, acct_type)',
    'dim_date (3,650 rows: 2020-2029)',
])

# ==================== TESTS ====================
draw_box(1, 8.2, 9, 2, C_TEST, C_TEST_BG, 'DATA QUALITY TESTS (28 Schema + 4 Custom)', [
    'trial_balance: debit = credit per journal',
    'assert_no_negative_oil_volume',
    'assert_no_negative_pressure_drop',
    'assert_water_cut_in_range (0-100%)',
])

# ==================== SNAPSHOTS ====================
draw_box(11, 8.2, 9, 2, C_SNAP, C_SNAP_BG, 'SNAPSHOTS (SCD Type 2)', [
    'vendor_snapshot',
    '  strategy: check',
    '  columns: vendor_name, service_type, location',
    '  target_schema: snapshots',
])

# Arrow Gold -> Tests
draw_arrow(5.5, 10.5, 5.5, 10.2)
draw_arrow(15.5, 10.5, 15.5, 10.2)

# ==================== CI/CD & ORCHESTRATION ====================
draw_box(1, 5.5, 9, 2.4, C_CICD, C_CICD_BG, 'CI/CD - GitHub Actions', [
    'Schedule: Daily 6 AM UTC (cron)',
    'Manual trigger with full-refresh option',
    'Flow: Seed -> Bronze -> Silver -> Gold',
    'Each layer: run + test sequentially',
    'Secret: SNOWFLAKE_PASSWORD',
])

draw_box(11, 5.5, 9, 2.4, '#FF5722', '#FBE9E7', 'Orchestration - Apache Airflow', [
    'DAG: petroleum_pipeline',
    'Schedule: Daily 6 AM UTC',
    '10 Tasks: deps->seed->snapshot->',
    '  bronze->test->silver->test->gold->test->docs',
    'Docker Compose for local deployment',
])

# ==================== WORKFLOW ====================
draw_box(1, 2.5, 19, 2.7, C_TITLE, '#ECEFF1', 'END-TO-END WORKFLOW', [])

# Workflow steps
steps = [
    ('1', 'Setup\ndbt debug', '#2196F3'),
    ('2', 'Seed\nCSV Load', '#795548'),
    ('3', 'Snapshot\nSCD T2', C_SNAP),
    ('4', 'Bronze\nIncremental', C_BRONZE),
    ('5', 'Test\nBronze', C_TEST),
    ('6', 'Silver\nTransform', C_SILVER),
    ('7', 'Test\nSilver', C_TEST),
    ('8', 'Gold\nStar Schema', '#B8860B'),
    ('9', 'Test\nGold', C_TEST),
    ('10', 'Docs\nGenerate', C_CICD),
]

sx = 1.7
for i, (num, label, color) in enumerate(steps):
    bx = sx + i * 1.85
    box = FancyBboxPatch((bx, 2.8), 1.6, 1.8, boxstyle="round,pad=0.15",
                         facecolor=color, edgecolor='white', linewidth=1.5, alpha=0.85)
    ax.add_patch(box)
    ax.text(bx + 0.8, 4.2, num, ha='center', va='center',
            fontsize=10, fontweight='bold', color='white')
    ax.text(bx + 0.8, 3.5, label, ha='center', va='center',
            fontsize=7, fontweight='bold', color='white')
    if i < len(steps) - 1:
        ax.annotate("", xy=(bx + 1.75, 3.7), xytext=(bx + 1.6, 3.7),
                    arrowprops=dict(arrowstyle="-|>", color='white', lw=1.5))

# ==================== FOOTER ====================
ax.text(11, 1.8, 'Repository: github.com/tiwaryrahul/Petroleum-  |  Database: Snowflake  |  Tool: dbt 1.11.7',
        ha='center', va='center', fontsize=9, color='#78909C')
ax.text(11, 1.4, 'Result: 61/61 PASS  |  30 Models  |  28 Tests  |  2 Seeds  |  1 Snapshot  |  6 Sources',
        ha='center', va='center', fontsize=9, fontweight='bold', color=C_TEST)

plt.tight_layout()
plt.savefig('c:/dev_work/petroleum_pipeline/Architecture_Diagram.png', dpi=200, bbox_inches='tight',
            facecolor='white', edgecolor='none')
print("Architecture diagram saved!")
