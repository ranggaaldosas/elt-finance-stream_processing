import os
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator

@dag(
    start_date=datetime(2024, 5, 14),
    schedule_interval="@daily",
    catchup=False,
    tags=["airbyte", "airflow", "finance", "staging"],
)
def finance_stream_batch_processing():
    # Task untuk melakukan sinkronisasi data dengan Airbyte
    ingest_csv_to_pubsub = AirbyteTriggerSyncOperator(
        task_id="ingest_csv_to_pubsub",
        airbyte_conn_id="airbyte",
        connection_id="ccbc7522-875b-4edb-af1e-44409f33f437",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )

    # Task untuk menjalankan model finance_staging DBT
    dbt_finance_staging_task = BashOperator(
        task_id="run_dbt_finance_staging",
        bash_command="dbt run --select finance_staging --profiles-dir /opt/airflow/include/dbt/finance --project-dir /opt/airflow/include/dbt/finance",
    )

    # Task untuk menjalankan model finance_dwh DBT
    dbt_finance_dwh_task = BashOperator(
        task_id="run_dbt_finance_dwh",
        bash_command="dbt run --select finance_dwh --profiles-dir /opt/airflow/include/dbt/finance --project-dir /opt/airflow/include/dbt/finance",
    )

    # Definisi urutan eksekusi tasks
    ingest_csv_to_pubsub >> dbt_finance_staging_task >> dbt_finance_dwh_task

finance_stream_batch_processing()
