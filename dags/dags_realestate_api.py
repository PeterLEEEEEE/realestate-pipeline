import httpx
from airflow.sdk import DAG
from airflow.decorators import task_group, dag
from pendulum import datetime


@dag(
    dag_id="dags_realestate_api_to_postgres",
    schedule="0 22 * * *",
    catchup=False,
)
def realestate_api_postgres_dag():
    @task_group(group_id="fetch_and_store_listings")
    def fetch():
        