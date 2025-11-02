from typing import Sequence

import httpx
from airflow.models import BaseOperator

from hooks.custom_postgres_hook import CustomPostgresHook


class ApiToPostgresOperator(BaseOperator):
    """
    매물 데이터를 수집하여 raw.articles 테이블에 저장하는 Operator (Raw Layer)
    """

    template_fields = ("complex_nos", "trade_type", "page_no", "max_pages")

    def __init__(
        self,
        postgres_conn_id: str,
        complex_nos: Sequence[int] | None = None,
        trade_type: str = "A1",
        page_no: int = 1,
        max_pages: int = 3,
        page_sleep_ms_min: int = 8,
        page_sleep_ms_max: int = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.complex_nos = list(complex_nos) if complex_nos is not None else None
        self.trade_type = trade_type
        self.page_no = page_no
        self.max_pages = max_pages
        self.page_sleep_ms_min = page_sleep_ms_min
        self.page_sleep_ms_max = page_sleep_ms_max

    def execute(self, context):
        import time

        from src.core.fetch import enrich_articles_with_price_history, fetch_articles_for_areas
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        if self.complex_nos is None:
            raise ValueError("complex_nos must be provided")

        ids: list[int | str] = list(self.complex_nos)
        complex_pyeongs = pg_hook.get_complex_pyeongs(ids)
        cookies, headers = get_cookies_headers()

        total_saved = 0
        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for cid in ids:
                pyeong_infos = complex_pyeongs.get(str(cid), [])
                if not pyeong_infos:
                    self.log.warning("complex=%s has no pyeong info, skipping", cid)
                    continue

                # 평형 번호 추출
                pyeong_nums = [p.get("pyeongNo") for p in pyeong_infos if p.get("pyeongNo")]
                if not pyeong_nums:
                    self.log.warning("complex=%s has no valid pyeongNo, skipping", cid)
                    continue

                # 평형을 4개씩 chunk로 나누기
                chunk_size = 4
                chunks = [pyeong_nums[i:i+chunk_size] for i in range(0, len(pyeong_nums), chunk_size)]

                complex_total = 0
                for chunk_idx, chunk in enumerate(chunks, 1):
                    self.log.info(
                        "complex=%s processing chunk %d/%d (areas=%s)",
                        cid,
                        chunk_idx,
                        len(chunks),
                        chunk,
                    )

                    # 1. 매물 리스트 수집 (원본 데이터만)
                    articles = fetch_articles_for_areas(
                        client=client,
                        complex_id=cid,
                        area_nos=chunk,
                        trade_type=self.trade_type,
                        max_pages=self.max_pages,
                        sleep_ms_min=self.page_sleep_ms_min,
                        sleep_ms_max=self.page_sleep_ms_max,
                    )

                    # 2. 가격 히스토리 추가 및 날짜 변환
                    enriched_articles = enrich_articles_with_price_history(
                        client=client,
                        articles=articles,
                        complex_id=cid,
                    )

                    # 3. DB 저장
                    if enriched_articles:
                        pg_hook.upsert_articles(enriched_articles)
                        complex_total += len(enriched_articles)

                    self.log.info(
                        "complex=%s chunk %d/%d: fetched=%d enriched=%d",
                        cid,
                        chunk_idx,
                        len(chunks),
                        len(articles),
                        len(enriched_articles),
                    )

                total_saved += complex_total
                self.log.info("complex=%s total_saved=%d", cid, complex_total)

        self.log.info("Total articles saved across all complexes: %s", total_saved)
        pg_hook.close()


class ComplexDetailPostgresOperator(BaseOperator):
    """
    단지 상세정보를 수집하여 raw.complex_details 테이블에 저장하는 Operator (Raw Layer)
    """

    template_fields = ("complex_nos", "trade_type")

    def __init__(
        self,
        postgres_conn_id: str,
        complex_nos: Sequence[int] | None = None,
        trade_type: str = "A1",
        sleep_min_sec: int = 2,
        sleep_max_sec: int = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.complex_nos = list(complex_nos) if complex_nos is not None else None
        self.trade_type = trade_type
        self.sleep_min_sec = sleep_min_sec
        self.sleep_max_sec = sleep_max_sec

    def execute(self, context):
        from src.core.fetch import fetch_complex_detail
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        if self.complex_nos is None:
            raise ValueError("complex_nos must be provided")

        ids = list(self.complex_nos)

        details: list[dict] = []
        cookies, headers = get_cookies_headers()
        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for cid in ids:
                detail = fetch_complex_detail(
                    client=client,
                    complex_id=cid,
                    sleep_min_sec=self.sleep_min_sec,
                    sleep_max_sec=self.sleep_max_sec,
                )
                if detail:
                    if not detail.get("complexNo"):
                        detail["complexNo"] = str(cid)
                    details.append(detail)

        if details:
            pg_hook.upsert_complex_details(details)

        self.log.info("complex_count=%s saved=%s", len(ids), len(details))
        pg_hook.close()


class RealPricePostgresOperator(BaseOperator):
    """
    실거래가 데이터를 수집하여 raw.real_prices 테이블에 저장하는 Operator (Raw Layer)
    """

    template_fields = ("complex_nos", "trade_type")

    def __init__(
        self,
        postgres_conn_id: str,
        complex_nos: Sequence[int] | None = None,
        trade_type: str = "A1",
        sleep_min_sec: int = 2,
        sleep_max_sec: int = 5,
        filter_by_execution_date: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.complex_nos = list(complex_nos) if complex_nos is not None else None
        self.trade_type = trade_type
        self.sleep_min_sec = sleep_min_sec
        self.sleep_max_sec = sleep_max_sec
        self.filter_by_execution_date = filter_by_execution_date

    def execute(self, context):
        from datetime import timedelta

        from src.core.fetch import (
            fetch_complex_real_price,
            fetch_complex_real_price_range,
        )
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        if self.complex_nos is None:
            raise ValueError("complex_nos must be provided")

        ids: list[int | str] = list(self.complex_nos)
        complex_pyeongs = pg_hook.get_complex_pyeongs(ids)
        cookies, headers = get_cookies_headers()

        # execution_date 기준 연월일 추출 (한국 시간 기준)
        end_year = None
        end_month = None
        end_day = None
        if self.filter_by_execution_date:
            # data_interval_end 또는 execution_date 사용
            exec_date = context.get("data_interval_end") or context.get("execution_date")
            if exec_date:
                # 한국 시간(KST, UTC+9)으로 변환
                kst_date = exec_date.in_timezone("Asia/Seoul")
                end_year = str(kst_date.year)
                end_month = str(kst_date.month)
                end_day = kst_date.day
                self.log.info(
                    "Incremental collection mode - end date (KST): %s-%s-%s",
                    end_year,
                    end_month,
                    end_day,
                )
            else:
                self.log.warning("filter_by_execution_date=True but no execution_date in context")
                self.filter_by_execution_date = False

        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for complex_no in (str(i) for i in ids):
                pyeong_infos: list[dict] = complex_pyeongs.get(complex_no, [])
                if not pyeong_infos:
                    self.log.warning("complex_no=%s pyeong 정보 없음, 스킵", complex_no)
                    continue

                pyeong_cnt = len(pyeong_infos)

                # 증분 수집 모드
                if self.filter_by_execution_date and end_year and end_month and end_day:
                    # 단지의 가장 최근 수집 날짜 조회 (첫번째 평형 기준)
                    first_area_no = str(pyeong_infos[0].get("pyeongNo", "1"))
                    last_collected = pg_hook.get_last_collected_date(complex_no, first_area_no)

                    if last_collected:
                        # 마지막 수집일 + 1일부터 오늘까지
                        last_year, last_month, last_day = last_collected

                        # 다음날 계산
                        from datetime import date
                        last_date = date(int(last_year), int(last_month), last_day)
                        start_date = last_date + timedelta(days=1)

                        start_year = str(start_date.year)
                        start_month = str(start_date.month)
                        start_day = start_date.day

                        self.log.info(
                            "complex=%s incremental collection: %s-%s-%s ~ %s-%s-%s",
                            complex_no,
                            start_year,
                            start_month,
                            start_day,
                            end_year,
                            end_month,
                            end_day,
                        )

                        complex_sold_infos = fetch_complex_real_price_range(
                            client=client,
                            complex_no=complex_no,
                            pyeong_infos=pyeong_infos,
                            pyeong_cnt=pyeong_cnt,
                            start_year=start_year,
                            start_month=start_month,
                            start_day=start_day,
                            end_year=end_year,
                            end_month=end_month,
                            end_day=end_day,
                            trade_type=self.trade_type,
                            sleep_min_sec=self.sleep_min_sec,
                            sleep_max_sec=self.sleep_max_sec,
                        )
                    else:
                        # 첫 수집 → 전체 수집
                        self.log.info("complex=%s first collection (no previous data)", complex_no)
                        complex_sold_infos = fetch_complex_real_price(
                            client=client,
                            complex_no=complex_no,
                            pyeong_infos=pyeong_infos,
                            pyeong_cnt=pyeong_cnt,
                            trade_type=self.trade_type,
                            sleep_min_sec=self.sleep_min_sec,
                            sleep_max_sec=self.sleep_max_sec,
                        )

                    self.log.info(
                        "complex=%s collected=%s",
                        complex_no,
                        len(complex_sold_infos),
                    )
                else:
                    # 전체 수집 모드
                    complex_sold_infos = fetch_complex_real_price(
                        client=client,
                        complex_no=complex_no,
                        pyeong_infos=pyeong_infos,
                        pyeong_cnt=pyeong_cnt,
                        trade_type=self.trade_type,
                        sleep_min_sec=self.sleep_min_sec,
                        sleep_max_sec=self.sleep_max_sec,
                    )
                    self.log.info(
                        "complex=%s all_collected=%s",
                        complex_no,
                        len(complex_sold_infos),
                    )

                if complex_sold_infos:
                    for sold_info in complex_sold_infos:
                        sold_info.setdefault("tradeType", self.trade_type)
                    pg_hook.upsert_real_prices(complex_sold_infos)

        pg_hook.close()