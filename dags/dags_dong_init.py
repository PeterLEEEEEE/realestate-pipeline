# from airflow.sdk import dag, task_group
# from pendulum import datetime, duration

# from common_tasks import get_target_complex_ids_task, chunk_complex_ids
# from operators.api_to_postgres_ops import *


# @dag(
#     dag_id="dags_rs_dong_init",
# )
# def dags_rs_dong_init():
    
    
#     @task_group(group_id="rs_dong_init_pipeline")
#     def rs_dong_init_pipeline():
#         compiex_ids = get_target_complex_ids_task(household_count=1000)
#         id_chunks = chunk_complex_ids(compiex_ids, chunk_size=10)
    
#     rs_dong_init_pipeline()

# dags_rs_dong_init()