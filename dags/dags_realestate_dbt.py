from airflow.sdk import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime

# Docker 컨테이너 내부 경로 (docker-compose.override.yml에서 마운트됨)
DBT_PROJECT_PATH = "/usr/local/rs_dbt/rs_dbt"
DBT_VENV = "/usr/local/rs_dbt/.venv/bin/activate"
DBT_PROFILES_DIR = "./proflies"


@dag(
    dag_id="dags_realestate_dbt",
    start_date=datetime(2023, 4, 1, tz="Asia/Seoul"),
    schedule="0 1 * * *",  # 매일 01:00 (API 수집 DAG 완료 후)
    catchup=False,
    tags=["realestate", "dbt", "silver", "gold"],
    description="dbt를 사용하여 Bronze → Silver → Gold 변환",
)
def realestate_dbt_dag():
    """
    Bronze (raw) → Silver (staging) → Gold (marts) 변환 DAG

    dbt run/test를 BashOperator로 실행
    호스트 머신의 dbt 환경을 직접 사용
    """

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"source {DBT_VENV} && "
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt run --select staging --profiles-dir {DBT_PROFILES_DIR} --target docker"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"source {DBT_VENV} && "
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt test --select staging source:raw --profiles-dir {DBT_PROFILES_DIR} --target docker"
        ),
    )

    dbt_run_staging >> dbt_test_staging


realestate_dbt_dag()
