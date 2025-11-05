from airflow.sdk import dag, task_group
from pendulum import datetime, duration

from common_tasks import get_target_complex_ids_task, chunk_complex_ids
from operators.api_to_postgres_ops import (
    ApiToPostgresOperator,
    ComplexDetailPostgresOperator,
    RealPricePostgresOperator,
)


@dag(
    dag_id="dags_realestate_api_to_postgres",
    start_date=datetime(2023, 4, 1, tz="Asia/Seoul"),
    schedule="0 23 * * *",
    catchup=False,
    tags=["realestate", "daily", "postgres"],
    description="네이버 부동산 API에서 매일 실거래가 증분 수집",
)
def realestate_api_postgres_dag():
    """
    매일 자동 수집 DAG

    1. raw.complexes에서 totalHouseholdCount >= 1000인 단지 조회
    2. 10개씩 청크로 나눔
    3. 실거래가 증분 수집:
       - 첫 수집: 2023년부터 전체 수집
       - 이후 수집: 지난 달 1일 ~ 실행일까지 수집 (약 2개월)
       - DB에 이미 있는 데이터는 UNIQUE 제약으로 자동 중복 제거
    """

    @task_group(group_id="realestate_api_postgres_pipeline")
    def realestate_api_pipeline():
        # 1. 수집 대상 단지 ID 조회
        complex_ids = get_target_complex_ids_task()

        # 2. 10개씩 청크로 나눔
        id_chunks = chunk_complex_ids(complex_ids)

        # 3. 매물 데이터 수집 (동적 Task Mapping)
        ApiToPostgresOperator.partial(
            task_id="fetch_articles_postgres",
            postgres_conn_id="postgres_default",
            trade_type="A1",  # 매매
            pool="api_pool",  # API rate limiting
            max_pages=5,
            page_sleep_ms_min=10,
            page_sleep_ms_max=15,
            retries=3,
            retry_delay=duration(minutes=5),
            retry_exponential_backoff=True,
            max_retry_delay=duration(minutes=30),
        ).expand(complex_nos=id_chunks)

        # 4. 단지 상세정보 수집 (동적 Task Mapping)
        # ComplexDetailPostgresOperator.partial(
        #     task_id="fetch_complex_detail_postgres",
        #     postgres_conn_id="postgres_default",
        #     pool="api_pool",  # API rate limiting
        #     retries=3,
        #     retry_delay=duration(minutes=5),
        # ).expand(complex_nos=id_chunks)

        # 5. 실거래가 증분 수집 (동적 Task Mapping)
        RealPricePostgresOperator.partial(
            task_id="fetch_real_prices_postgres",
            postgres_conn_id="postgres_default",
            pool="api_pool",  # API rate limiting
            retries=3,
            sleep_min_sec=10,
            sleep_max_sec=15,
            filter_by_execution_date=True,  # 증분 수집 모드: 지난 달 1일 ~ 오늘
            retry_delay=duration(minutes=10),
            retry_exponential_backoff=True,
            max_retry_delay=duration(minutes=30),
        ).expand(complex_nos=id_chunks)

    realestate_api_pipeline()


realestate_api_postgres_dag()