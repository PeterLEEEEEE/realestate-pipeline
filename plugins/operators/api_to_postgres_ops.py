from datetime import datetime, timedelta
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
        page_sleep_ms_min: int = 200,
        page_sleep_ms_max: int = 500,
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
        from src.core.fetch import fetch_article_price_history, fetch_articles_paged
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        if self.complex_nos is None:
            raise ValueError("complex_nos must be provided")

        ids = list(self.complex_nos)

        cookies, headers = get_cookies_headers()

        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for cid in ids:
                articles = fetch_articles_paged(
                    client=client,
                    complex_id=cid,
                    trade_type=self.trade_type,
                    max_pages=self.max_pages,
                    sleep_ms_min=self.page_sleep_ms_min,
                    sleep_ms_max=self.page_sleep_ms_max,
                )

                norm_docs: list[dict] = []
                for doc in articles:
                    article_no = doc.get("articleNo")
                    if article_no is None:
                        continue

                    initial_price = None
                    price_history_list = []
                    if doc.get("priceChangeState") != "SAME":
                        history = fetch_article_price_history(
                            client=client,
                            article_no=article_no,
                        )
                        if isinstance(history, dict):
                            initial_price = history.get("initialPrice")
                            price_history_list = history.get("priceHistoryList") or []

                    payload = {**doc}
                    if initial_price is not None:
                        payload["initialPrice"] = initial_price
                    if price_history_list:
                        payload["priceHistoryList"] = price_history_list
                    if not payload.get("complexNo"):
                        payload["complexNo"] = str(cid)

                    ymd = payload.get("articleConfirmYmd")
                    if isinstance(ymd, str) and len(ymd) == 8 and ymd.isdigit():
                        try:
                            confirm_dt = datetime.strptime(ymd, "%Y%m%d")
                            payload["articleConfirmDate"] = confirm_dt
                            payload["expireAt"] = confirm_dt + timedelta(days=28)
                        except Exception:
                            pass

                    norm_docs.append(payload)

                if norm_docs:
                    pg_hook.upsert_articles(norm_docs)

                self.log.info(
                    "complex=%s processed=%s (max_pages=%s)",
                    cid,
                    len(norm_docs),
                    self.max_pages,
                )

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
        sleep_min_sec: int = 5,
        sleep_max_sec: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.complex_nos = list(complex_nos) if complex_nos is not None else None
        self.trade_type = trade_type
        self.sleep_min_sec = sleep_min_sec
        self.sleep_max_sec = sleep_max_sec

    def execute(self, context):
        from src.core.fetch import fetch_complex_real_price
        from src.utils.headers import get_cookies_headers

        pg_hook = CustomPostgresHook(self.postgres_conn_id)
        pg_hook.ensure_tables()

        if self.complex_nos is None:
            raise ValueError("complex_nos must be provided")

        ids: list[int | str] = list(self.complex_nos)

        complex_pyeongs = pg_hook.get_complex_pyeongs(ids)
        cookies, headers = get_cookies_headers()

        with httpx.Client(headers=headers, cookies=cookies, timeout=15.0) as client:
            for complex_no in (str(i) for i in ids):
                pyeong_infos: list[dict] = complex_pyeongs.get(complex_no, [])
                if not pyeong_infos:
                    self.log.warning("complex_no=%s pyeong 정보 없음, 스킵", complex_no)
                    continue

                pyeong_cnt = len(pyeong_infos)

                complex_sold_infos = fetch_complex_real_price(
                    client=client,
                    complex_no=complex_no,
                    pyeong_infos=pyeong_infos,
                    pyeong_cnt=pyeong_cnt,
                    trade_type=self.trade_type,
                    sleep_min_sec=self.sleep_min_sec,
                    sleep_max_sec=self.sleep_max_sec,
                )

                if complex_sold_infos:
                    for sold_info in complex_sold_infos:
                        sold_info.setdefault("tradeType", self.trade_type)
                    pg_hook.upsert_real_prices(complex_sold_infos)

                self.log.info(
                    "complex=%s pyeong_count=%s",
                    complex_no,
                    len(complex_sold_infos),
                )

        pg_hook.close()