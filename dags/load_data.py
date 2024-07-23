from datetime import datetime

from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


@dag(
    start_date=datetime(2024, 5, 14),
    schedule="@daily",
    catchup=False,
    tags=["airbyte", "airflow"],
)
def dataIngestion():
    csv_to_pubsub = AirbyteTriggerSyncOperator(
        task_id="ingest_csv_to_pubsub",
        airbyte_conn_id="airbyte",
        connection_id="ccbc7522-875b-4edb-af1e-44409f33f437",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )


dataIngestion()
