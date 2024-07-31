import os
import re
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from discord_webhook import DiscordWebhook, DiscordEmbed
import logging
from typing import Optional
from airflow.models.taskinstance import TaskInstance

logging.basicConfig(level=logging.INFO)

def send_discord_embed(title: str, description: str, color: str, fields: dict, webhook_url: str):
    try:
        webhook = DiscordWebhook(url=webhook_url)
        embed = DiscordEmbed(
            title=title,
            description=description,
            color=color
        )
        for name, value in fields.items():
            embed.add_embed_field(name=name, value=value, inline=False)
        webhook.add_embed(embed)
        response = webhook.execute()
        logging.info(f"Discord notification sent: {response.status_code}, {response.content}")
    except Exception as e:
        logging.error(f"Failed to send Discord notification: {e}")

def send_start_notification():
    webhook_url = Variable.get("DISCORD_SUCCESS_WEBHOOK_URL")
    title = "Airflow Notification"
    description = "Airflow Started ✅ -> The finance_stream_batch_processing DAG has started."
    fields = {
        "Start Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    logging.info(f"Sending start notification to {webhook_url}")
    send_discord_embed(title, description, '00FF00', fields, webhook_url)

def send_success_notification():
    webhook_url = Variable.get("DISCORD_SUCCESS_WEBHOOK_URL")
    title = "Airflow Notification"
    description = "Airflow Succes ✅ The finance_stream_batch_processing DAG has completed successfully."
    fields = {
        "Completion Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    logging.info(f"Sending success notification to {webhook_url}")
    send_discord_embed(title, description, '00FF00', fields, webhook_url)

def send_failure_notification(context):
    webhook_url = Variable.get("DISCORD_FAILURE_WEBHOOK_URL")
    last_task: Optional[TaskInstance] = context.get('task_instance')
    task_name = last_task.task_id
    dag_name = last_task.dag_id
    log_link = last_task.log_url
    execution_date = context.get('execution_date')

    try:
        error_message = str(context["exception"])
        error_message = error_message[:1000] + (error_message[1000:] and '...')
    except:
        error_message = "Some error that cannot be extracted has occurred. Visit the logs!"

    title = "Airflow Alert - Task has failed!"
    description = "Airflow Failure ⛔ _>The finance_stream_batch_processing DAG has failed."
    fields = {
        "DAG": dag_name,
        "Task": task_name,
        "Execution Date": execution_date.isoformat() if execution_date else "N/A",
        "Error": error_message,
        "Log URL": log_link
    }
    logging.info(f"Sending failure notification to {webhook_url}")
    send_discord_embed(title, description, 'FF0000', fields, webhook_url)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime("2022-04-11 20:00:00", "%Y-%m-%d %H:%M:%S"),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
    'depends_on_past': False,
    'on_failure_callback': send_failure_notification
}

@dag(
    default_args=default_args,
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
