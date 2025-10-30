from airflow.sdk import dag
from pendulum import datetime, duration

from operators.region_init_operator import RegionInitOperator


# 수집할 지역 리스트
REGIONS = [
    "서울시 송파구",
    "서울시 강남구",
    "서울시 서초구",
    "서울시 용산구",
]


@dag(
    dag_id="dags_region_init_yearly",
    start_date=datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="@yearly",  # 매년 1월 1일 실행 (또는 "0 0 1 1 *")
    catchup=False,  # Manual trigger 가능
    tags=["realestate", "init", "region"],
    description="네이버 부동산 API에서 지역별 단지 메타데이터 초기 수집 (1년에 1회)",
)
def region_init_dag():
    """
    지역별 단지 메타데이터 초기 수집 DAG

    - 네이버 부동산 API /search 엔드포인트 사용
    - APT(아파트), JGC(재건축), ABYG(아파트분양권)만 수집
    - raw.complexes 테이블에 저장 (Raw Layer)
    - 매년 1회 실행 (또는 Manual trigger 가능)
    """

    collect_regions = RegionInitOperator(
        task_id="collect_region_complexes",
        postgres_conn_id="postgres_default",
        regions=REGIONS,
        sleep_min_sec=5,  # 페이지 간 최소 대기 시간 (초)
        sleep_max_sec=20,  # 페이지 간 최대 대기 시간 (초)
        retries=2,
        retry_delay=duration(minutes=10),
    )

    collect_regions


region_init_dag()
