from airflow.sdk import task
from pendulum import duration

from hooks.custom_postgres_hook import CustomPostgresHook


@task(
    task_id="get_target_complex_ids",
    retries=3,
    retry_delay=duration(minutes=5),
)
def get_target_complex_ids_task(household_count: int = 1000) -> list[str]:
    """
    매일 수집 대상 단지 ID 조회
    raw.complexes 테이블에서 totalHouseholdCount >= household_count인 단지만 반환
    """
    hook = CustomPostgresHook("postgres_default")
    ids = hook.get_target_complex_ids(household_count=household_count)
    hook.close()
    return ids


@task(task_id="chunk_complex_ids")
def chunk_complex_ids(ids: list[str], chunk_size: int = 10) -> list[list[str]]:
    """
    단지 ID 리스트를 chunk_size 개씩 나눔
    동적 Task Mapping을 위한 청킹
    """
    return [ids[i : i + chunk_size] for i in range(0, len(ids), chunk_size)]
