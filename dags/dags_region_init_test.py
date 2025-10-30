from airflow.sdk import dag
from pendulum import datetime, duration

from operators.region_init_operator import RegionInitOperator


# 테스트용: 지역 1개만 수집
TEST_REGIONS = [
    "서울시 용산구",  # 상대적으로 작은 지역으로 빠른 테스트
]


@dag(
    dag_id="dags_region_init_test",
    start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,  # Manual trigger만 가능
    catchup=False,
    tags=["realestate", "test", "region"],
    description="[테스트] 지역별 단지 메타데이터 수집 테스트 (1개 지역만)",
)
def region_init_test_dag():
    """
    지역별 초기 수집 테스트 DAG

    - 용산구 1개 지역만 수집하여 빠르게 테스트
    - raw.complexes 테이블 확인용
    - Manual trigger로만 실행
    """

    collect_test_region = RegionInitOperator(
        task_id="collect_test_region",
        postgres_conn_id="postgres_default",
        regions=TEST_REGIONS,
        sleep_min_sec=2,  # 테스트용으로 대기 시간 단축
        sleep_max_sec=5,
        retries=1,  # 테스트용으로 재시도 줄임
        retry_delay=duration(minutes=1),
    )

    collect_test_region


region_init_test_dag()
