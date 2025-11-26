from airflow.sdk import dag
from pendulum import datetime, duration

from common_tasks import get_target_complex_ids_task, chunk_complex_ids
from operators.region_init_operator import RegionInitOperator
from operators.api_to_postgres_ops import ComplexDetailPostgresOperator


# 수집할 지역 리스트
REGIONS = [
    # "서울시 송파구",
    # "서울시 강남구",
    # "서울시 서초구",
    # "서울시 용산구",
    # "서울시 성동구", # 20251127 완료
    "서울시 마포구",
    # "서울시 동작구",
    # "서울시 영등포구",
    # "서울시 광진구",
    # "서울시 강동구",
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

    1. 네이버 부동산 API /search 엔드포인트로 지역별 단지 목록 수집
    2. 수집된 단지들의 상세정보(pyeong 등) 수집

    - APT(아파트), JGC(재건축), ABYG(아파트분양권)만 수집
    - raw.complexes, raw.complex_details 테이블에 저장 (Raw Layer)
    - 매년 1회 실행 (또는 Manual trigger 가능)
    """

    # 1. 지역별 단지 목록 수집
    collect_regions = RegionInitOperator(
        task_id="collect_region_complexes",
        postgres_conn_id="postgres_default",
        regions=REGIONS,
        sleep_min_sec=10,
        sleep_max_sec=20,
        retries=2,
        retry_delay=duration(minutes=10),
    )

    # 2. 수집된 단지 ID 조회 (세대수 1000 이상 필터 적용)
    complex_ids = get_target_complex_ids_task(household_count=1000, regions=REGIONS)

    # 3. 10개씩 청크로 나눔
    id_chunks = chunk_complex_ids(complex_ids)

    # 4. 단지 상세정보 수집 (동적 Task Mapping)
    fetch_details = ComplexDetailPostgresOperator.partial(
        task_id="fetch_complex_details",
        postgres_conn_id="postgres_default",
        retries=3,
        retry_delay=duration(minutes=5),
    ).expand(complex_nos=id_chunks)

    # 순차 실행: 단지 목록 수집 → 단지 ID 조회 → 상세정보 수집
    collect_regions >> complex_ids >> id_chunks >> fetch_details


region_init_dag()
