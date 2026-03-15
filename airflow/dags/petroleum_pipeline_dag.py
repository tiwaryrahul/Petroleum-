from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Path to your dbt project
DBT_PROJECT_DIR = "c:/dev_work/petroleum_pipeline/petroleum_pipeline"

default_args = {
    "owner": "rahul",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="petroleum_pipeline",
    default_args=default_args,
    description="dbt Petroleum Pipeline - Bronze to Silver to Gold",
    schedule="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "petroleum", "snowflake"],
) as dag:

    env = {"SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}"}

    # Step 1: dbt deps - install packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
        env=env,
    )

    # Step 2: dbt seed - load seed data
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed",
        env=env,
    )

    # Step 3: dbt snapshot - SCD Type 2
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
        env=env,
    )

    # Step 4: Bronze layer - incremental loads
    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select models/bronze",
        env=env,
    )

    # Step 5: Bronze tests
    dbt_test_bronze = BashOperator(
        task_id="dbt_test_bronze",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select models/bronze",
        env=env,
    )

    # Step 6: Silver layer
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select models/silver",
        env=env,
    )

    # Step 7: Silver tests
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select models/silver",
        env=env,
    )

    # Step 8: Gold layer - star schema
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select models/gold",
        env=env,
    )

    # Step 9: Gold tests
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select models/gold",
        env=env,
    )

    # Step 10: Generate dbt docs
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate",
        env=env,
    )

    # DAG flow
    dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run_bronze >> dbt_test_bronze
    dbt_test_bronze >> dbt_run_silver >> dbt_test_silver
    dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> dbt_docs_generate
