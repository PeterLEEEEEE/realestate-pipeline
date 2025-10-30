import time
import random
from datetime import datetime
from typing import Sequence

import httpx
from airflow.sdk import BaseOperator

from hooks.custom_postgres_hook import CustomPostgresHook
from src.models.model import RealEstateComplex


class RegionInitOperator(BaseOperator):
    """
    지역별로 단지 메타데이터를 초기 수집하는 Operator
    네이버 부동산 API의 /search 엔드포인트 사용
    """

    template_fields = ("regions",)

    def __init__(
        self,
        postgres_conn_id: str,
        regions: Sequence[str],
        sleep_min_sec: int = 5,
        sleep_max_sec: int = 20,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.regions = list(regions) if regions is not None else []
        self.sleep_min_sec = sleep_min_sec
        self.sleep_max_sec = sleep_max_sec

    def execute(self, context):
        from src.utils.urls import BASE_URL, complex_name_url
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        cookies, headers = get_cookies_headers()
        total_collected = 0

        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for region in self.regions:
                region_count = self._fetch_region_complexes(
                    client=client,
                    region=region,
                    base_url=BASE_URL,
                    complex_name_url_template=complex_name_url,
                    pg_hook=pg_hook,
                )
                total_collected += region_count
                self.log.info(
                    "region=%s collected=%s total=%s",
                    region,
                    region_count,
                    total_collected,
                )

        pg_hook.close()
        self.log.info("All regions processed. Total complexes: %s", total_collected)
        return total_collected

    def _fetch_region_complexes(
        self,
        client: httpx.Client,
        region: str,
        base_url: str,
        complex_name_url_template: str,
        pg_hook: CustomPostgresHook,
    ) -> int:
        """단일 지역에 대한 단지 정보 수집"""
        page_no = 1
        total_processed = 0

        while True:
            url = base_url + complex_name_url_template.format(
                region_name=region, page_no=page_no
            )
            self.log.info("[%s] Fetching page %s: %s", region, page_no, url)

            try:
                response = client.get(url)
                response.raise_for_status()

                data = response.json()
                complexes = data.get("complexes", [])

                if not complexes:
                    self.log.info(
                        "[%s] page %s에 단지 정보 없음 → 종료", region, page_no
                    )
                    break

                # 데이터 처리 및 저장
                docs = self._process_complexes(complexes)
                if docs:
                    pg_hook.upsert_complexes(docs)
                    total_processed += len(docs)
                    self.log.info(
                        "[%s] page %s: 누적 처리 %s건 (+%s)",
                        region,
                        page_no,
                        total_processed,
                        len(docs),
                    )

                # 더 이상 페이지 없음
                if data.get("isMoreData") is False:
                    self.log.info(
                        "[%s] 모든 페이지 크롤링 완료 (총 %s건)",
                        region,
                        total_processed,
                    )
                    break

            except httpx.HTTPStatusError as e:
                self.log.error(
                    "[%s] HTTP 오류 %s: %s",
                    region,
                    e.response.status_code,
                    e.request.url,
                )
                break
            except httpx.RequestError as e:
                self.log.error("[%s] 네트워크 오류: %s", region, e)
                break

            # 페이지 간 랜덤 sleep
            time.sleep(random.randint(self.sleep_min_sec, self.sleep_max_sec))
            page_no += 1

        return total_processed

    def _process_complexes(self, complexes: list[dict]) -> list[dict]:
        """
        API 응답의 complexes 배열을 처리
        realEstateTypeCode가 APT, JGC, ABYG인 것만 필터링
        """
        docs = []
        now = datetime.now()

        for data in complexes:
            try:
                # APT(아파트), JGC(재건축), ABYG(아파트분양권)만 수집
                if data.get("realEstateTypeCode") in ["APT", "JGC", "ABYG"]:
                    # Pydantic 모델로 검증
                    model = RealEstateComplex(**data)
                    doc = model.model_dump(by_alias=True, exclude_unset=True)

                    # 추가 필드
                    doc["crawled_at"] = now.isoformat()
                    doc["low_floor"] = data.get("lowFloor", 0)
                    doc["high_floor"] = data.get("highFloor", 0)

                    docs.append(doc)
            except Exception as e:
                self.log.warning("데이터 처리 오류: %s", e)
                continue

        return docs
