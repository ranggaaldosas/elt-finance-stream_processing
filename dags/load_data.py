import os
import requests
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

# Retrieve webhook URLs from Airflow variables
DISCORD_SUCCESS_WEBHOOK_URL = Variable.get("DISCORD_SUCCESS_WEBHOOK_URL")
DISCORD_FAILURE_WEBHOOK_URL = Variable.get("DISCORD_FAILURE_WEBHOOK_URL")

def send_discord_notification(message: str, webhook_url: str):
    if not webhook_url:
        raise ValueError("The webhook URL is not set.")
    try:
        data = {
            "content": message,
        }
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send notification to Discord: {e}")
        raise

def send_failure_notification(context):
    execution_date = context.get('execution_date')
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Captain Hook: The finance_stream_batch_processing DAG has failed at {current_time}. Execution date was {execution_date}."
    send_discord_notification(message, DISCORD_FAILURE_WEBHOOK_URL)

def send_start_notification():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Captain Hook: The finance_stream_batch_processing DAG has started at {current_time}."
    send_discord_notification(message, DISCORD_SUCCESS_WEBHOOK_URL)

def send_success_notification():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"Captain Hook: The finance_stream_batch_processing DAG has completed successfully at {current_time}."
    send_discord_notification(message, DISCORD_SUCCESS_WEBHOOK_URL)

@dag(
    start_date=datetime(2024, 5, 14),
    schedule_interval="@daily",
    catchup=False,
    tags=["airbyte", "airflow", "finance", "staging"],
)
def finance_stream_batch_processing():
    start_notification = PythonOperator(
        task_id="start_notification",
        python_callable=send_start_notification,
    )

    ingest_csv_to_pubsub = AirbyteTriggerSyncOperator(
        task_id="ingest_csv_to_pubsub",
        airbyte_conn_id="airbyte",
        connection_id="ccbc7522-875b-4edb-af1e-44409f33f437",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )

    dbt_finance_staging_task = BashOperator(
        task_id="run_dbt_finance_staging",
        bash_command="dbt run --select finance_staging --profiles-dir /opt/airflow/include/dbt/finance --project-dir /opt/airflow/include/dbt/finance",
    )

    dbt_finance_dwh_task = BashOperator(
        task_id="run_dbt_finance_dwh",
        bash_command="dbt run --select finance_dwh --profiles-dir /opt/airflow/include/dbt/finance --project-dir /opt/airflow/include/dbt/finance",
    )

    success_notification = PythonOperator(
        task_id="success_notification",
        python_callable=send_success_notification,
    )

    failure_notification = PythonOperator(
        task_id="failure_notification",
        python_callable=send_failure_notification,
        provide_context=True,
        trigger_rule="one_failed",
    )

    start_notification >> ingest_csv_to_pubsub >> dbt_finance_staging_task >> dbt_finance_dwh_task
    dbt_finance_dwh_task >> success_notification
    ingest_csv_to_pubsub >> failure_notification
    dbt_finance_staging_task >> failure_notification
    dbt_finance_dwh_task >> failure_notification

finance_stream_batch_processing()
