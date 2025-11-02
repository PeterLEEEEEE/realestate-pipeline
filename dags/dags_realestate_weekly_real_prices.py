from airflow.sdk import dag
from pendulum import datetime, duration

from common_tasks import chunk_complex_ids, get_target_complex_ids_task
from operators.api_to_postgres_ops import RealPricePostgresOperator


@dag(
    dag_id="dags_realestate_weekly_real_prices",
    start_date=datetime(2025, 11, 1, tz="Asia/Seoul"),
    schedule="0 0 * * 0",  # 매주 일요일 00:00 (KST)
    catchup=False,
    tags=["realestate", "weekly", "real_prices", "incremental"],
    description="매주 일요일 실거래가 증분 수집 (마지막 수집일 + 1 ~ 오늘)",
)
def weekly_real_price_dag():
    """
    매주 일요일 실거래가 증분 수집 DAG

    수집 전략:
    1. raw.complexes에서 totalHouseholdCount >= 1000인 단지 조회
    2. 10개씩 청크로 나눔
    3. 각 단지의 마지막 수집 날짜 조회
    4. 마지막 수집일 + 1일 ~ 오늘까지 증분 수집
    5. 첫 수집인 경우 전체 데이터 수집 (2025년 기준)

    예시:
    - 마지막 수집: 10월 28일
    - 오늘: 11월 3일
    - 수집 범위: 10월 29일 ~ 11월 3일 (6일치)

    장점:
    - DAG 실패 시에도 누락 없음 (다음 실행 시 누락 기간 포함)
    - API 호출 최소화 (증분만 수집)
    - 메모리 효율적
    """

    # 1. 수집 대상 단지 ID 조회
    complex_ids = get_target_complex_ids_task(household_count=1000)

    # 2. 10개씩 청크로 나눔
    id_chunks = chunk_complex_ids(complex_ids, chunk_size=10)

    # 3. 실거래가 증분 수집 (동적 Task Mapping)
    RealPricePostgresOperator.partial(
        task_id="collect_real_prices_incremental",
        postgres_conn_id="postgres_default",
        trade_type="A1",  # 매매
        pool="api_pool",  # API rate limiting
        filter_by_execution_date=True,  # 증분 수집 모드
        sleep_min_sec=2,
        sleep_max_sec=5,
        retries=3,
        retry_delay=duration(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=duration(minutes=30),
    ).expand(complex_nos=id_chunks)


weekly_real_price_dag()
