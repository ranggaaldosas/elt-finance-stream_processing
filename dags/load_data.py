from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python_operator import PythonOperator

@dag(
    start_date=datetime(2024, 5, 14),
    schedule="@daily",
    catchup=False,
    tags=["airbyte", "airflow", "finance", "staging"],
)
def finance_staging_dag():
    ingest_csv_to_pubsub = AirbyteTriggerSyncOperator(
        task_id="ingest_csv_to_pubsub",
        airbyte_conn_id="airbyte",
        connection_id="ccbc7522-875b-4edb-af1e-44409f33f437",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )

    def run_dbt_finance_staging():
        import os
        os.system("dbt run --select staging.finance_staging --profiles-dir /opt/airflow/include/dbt/finance --project-dir /opt/airflow/include/dbt/finance")

    dbt_finance_staging_task = PythonOperator(
        task_id="run_dbt_finance_staging",
        python_callable=run_dbt_finance_staging,
    )

    ingest_csv_to_pubsub >> dbt_finance_staging_task

finance_staging_dag()
