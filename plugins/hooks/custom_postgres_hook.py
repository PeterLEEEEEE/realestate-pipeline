from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Mapping

import psycopg2
from airflow.sdk.bases.hook import BaseHook
from psycopg2.extras import Json, execute_values

from src.utils.queries import (
    ARTICLE_DDL,
    COMPLEX_DETAILS_DDL,
    REAL_PRICE_DDL,
    COMPLEX_DDL
)

class CustomPostgresHook(BaseHook):
    def __init__(self, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.postgres_conn: psycopg2.extensions.connection | None = None
        self._json_tables_ready = False
    
    def get_conn(self):
        if self.postgres_conn:
            return self.postgres_conn

        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            port=self.port,
        )
        self.postgres_conn.autocommit = False
        self._json_tables_ready = False
        return self.postgres_conn

    
    def ensure_tables(self):
        if self._json_tables_ready:
            return

        conn = self.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(COMPLEX_DDL)
            cursor.execute(ARTICLE_DDL)
            cursor.execute(COMPLEX_DETAILS_DDL)
            cursor.execute(REAL_PRICE_DDL)
            conn.commit()

        self._json_tables_ready = True

    def close(self) -> None:
        if self.postgres_conn:
            try:
                self.postgres_conn.close()
            finally:
                self.postgres_conn = None
                self._json_tables_ready = False

    def upsert_complexes(self, docs: Sequence[Mapping[str, Any]]) -> None:
        """단지 메타데이터 upsert (raw layer)"""
        if not docs:
            return

        conn = self.get_conn()
        self.ensure_tables()

        now = datetime.now(timezone.utc)
        records = []
        for doc in docs:
            complex_no = doc.get("complexNo") or doc.get("complex_no")
            if complex_no is None:
                continue
            records.append(
                (
                    str(complex_no),
                    Json(self._json_ready(doc)),
                    now,
                    now,
                )
            )

        if not records:
            return

        insert_sql = """
            INSERT INTO raw.complexes (
                complex_no,
                payload,
                created_at,
                updated_at
            )
            VALUES %s
            ON CONFLICT (complex_no)
            DO UPDATE
            SET
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
        """
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
        conn.commit()

    def upsert_articles(self, docs: Sequence[Mapping[str, Any]]) -> None:
        """매물 데이터 upsert (raw layer)"""
        if not docs:
            return

        conn = self.get_conn()
        self.ensure_tables()

        now = datetime.now(timezone.utc)
        records = []
        for doc in docs:
            complex_no = doc.get("complexNo") or doc.get("complex_no")
            article_no = doc.get("articleNo") or doc.get("article_no")
            if complex_no is None or article_no is None:
                continue

            confirm = self._ensure_datetime(doc.get("articleConfirmDate"))
            expire = self._ensure_datetime(doc.get("expireAt"))

            records.append(
                (
                    str(complex_no),
                    str(article_no),
                    Json(self._json_ready(doc)),
                    confirm,
                    expire,
                    now,
                    now,
                )
            )

        if not records:
            return

        insert_sql = """
            INSERT INTO raw.articles (
                complex_no,
                article_no,
                payload,
                article_confirm_date,
                expire_at,
                created_at,
                updated_at
            )
            VALUES %s
            ON CONFLICT (complex_no, article_no)
            DO UPDATE
            SET
                payload = EXCLUDED.payload,
                article_confirm_date = CASE
                    WHEN raw.articles.article_confirm_date IS NULL THEN EXCLUDED.article_confirm_date
                    WHEN EXCLUDED.article_confirm_date IS NULL THEN raw.articles.article_confirm_date
                    WHEN EXCLUDED.article_confirm_date > raw.articles.article_confirm_date THEN EXCLUDED.article_confirm_date
                    ELSE raw.articles.article_confirm_date
                END,
                expire_at = CASE
                    WHEN raw.articles.expire_at IS NULL THEN EXCLUDED.expire_at
                    WHEN EXCLUDED.expire_at IS NULL THEN raw.articles.expire_at
                    WHEN EXCLUDED.expire_at > raw.articles.expire_at THEN EXCLUDED.expire_at
                    ELSE raw.articles.expire_at
                END,
                updated_at = EXCLUDED.updated_at
        """
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
        conn.commit()

    def upsert_complex_details(self, docs: Sequence[Mapping[str, Any]]) -> None:
        """단지 상세정보 upsert (raw layer)"""
        if not docs:
            return

        conn = self.get_conn()
        self.ensure_tables()

        now = datetime.now(timezone.utc)
        records = []
        for doc in docs:
            complex_no = doc.get("complexNo") or doc.get("complex_no")
            if complex_no is None:
                continue
            records.append(
                (
                    str(complex_no),
                    Json(self._json_ready(doc)),
                    now,
                    now,
                )
            )

        if not records:
            return

        insert_sql = """
            INSERT INTO raw.complex_details (
                complex_no,
                payload,
                created_at,
                updated_at
            )
            VALUES %s
            ON CONFLICT (complex_no)
            DO UPDATE
            SET
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
        """
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
        conn.commit()

    def upsert_real_prices(self, docs: Sequence[Mapping[str, Any]]) -> None:
        """
        실거래가 데이터 삽입 (raw layer)

        UNIQUE 제약으로 중복 방지 (complex_no, area_no, floor, deal_price, trade_date)
        중복 시 ON CONFLICT DO NOTHING으로 무시
        """
        if not docs:
            return

        conn = self.get_conn()
        self.ensure_tables()

        now = datetime.now(timezone.utc)
        records = []
        skipped_count = 0
        for doc in docs:
            complex_no = doc.get("complexNo") or doc.get("complex_no")
            area_no = doc.get("areaNo") or doc.get("area_no")
            formatted = doc.get("formattedTradeYearMonth") or doc.get("formatted_trade_year_month")
            trade_date_str = doc.get("tradeDate")

            if complex_no is None or area_no is None or formatted is None or trade_date_str is None:
                skipped_count += 1
                if skipped_count == 1:  # 첫 스킵 데이터만 로그
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Skipping record - complexNo: {complex_no}, areaNo: {area_no}, formatted: {formatted}, tradeDate: {trade_date_str}")
                    logger.warning(f"Available keys: {list(doc.keys())}")
                    logger.warning(f"Sample data: {dict(list(doc.items())[:10])}")
                continue

            floor = doc.get("floor")
            deal_price = doc.get("dealPrice") or doc.get("deal_price")

            # trade_date 생성: formattedTradeYearMonth (2025.10.21) → 2025-10-21
            try:
                trade_date = formatted.replace(".", "-")  # "2025.10.21" -> "2025-10-21"

                # 날짜 유효성 검증
                from datetime import date as dt_date
                dt_date.fromisoformat(trade_date)  # 유효하지 않으면 ValueError
            except (ValueError, AttributeError):
                continue

            records.append(
                (
                    str(complex_no),
                    str(area_no),
                    None if floor is None else str(floor),
                    None if deal_price is None else str(deal_price),
                    trade_date,
                    Json(self._json_ready(doc)),
                    now,
                    now,
                )
            )

        if not records:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"No valid records to insert. Total docs: {len(docs)}, Skipped: {skipped_count}")
            return

        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Inserting {len(records)} real_price records (skipped {skipped_count} invalid records)")

        insert_sql = """
            INSERT INTO raw.real_prices (
                complex_no,
                area_no,
                floor,
                deal_price,
                trade_date,
                payload,
                created_at,
                updated_at
            )
            VALUES %s
            ON CONFLICT (complex_no, area_no, floor, deal_price, trade_date) DO NOTHING
        """
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
        conn.commit()
        logger.info(f"Successfully committed {len(records)} real_price records")

    def get_complex_pyeongs(self, complex_nos: Sequence[int | str]) -> dict[str, list]:
        """단지별 pyeong 정보 조회 (실거래가 수집 시 필요)"""
        if not complex_nos:
            return {}

        conn = self.get_conn()
        self.ensure_tables()

        normalized = [str(value) for value in complex_nos if value is not None]
        if not normalized:
            return {}

        query = """
            SELECT complex_no, payload
            FROM raw.complex_details
            WHERE complex_no = ANY(%s)
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (normalized,))
            rows = cursor.fetchall()

        results: dict[str, list] = {}
        for complex_no, payload in rows:
            if isinstance(payload, str):
                try:
                    payload_dict = json.loads(payload)
                except (TypeError, json.JSONDecodeError):
                    payload_dict = {}
            elif isinstance(payload, dict):
                payload_dict = payload
            else:
                payload_dict = {}
            pyeongs = payload_dict.get("pyeongs") or []
            results[str(complex_no)] = pyeongs

        return results

    def get_target_complex_ids(self, household_count: int = 1000) -> list[str]:
        """
        매일 수집 대상 단지 ID 조회
        totalHouseholdCount >= household_count인 단지만 반환
        """
        conn = self.get_conn()
        self.ensure_tables()

        query = """
            SELECT complex_no
            FROM raw.complexes
            WHERE (payload->>'totalHouseholdCount')::int >= %s
            ORDER BY complex_no
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (household_count,))
            rows = cursor.fetchall()

        return [row[0] for row in rows]

    def get_last_collected_date(self, complex_no: str, area_no: str) -> tuple[str, str, int] | None:
        """
        특정 단지+평형의 가장 최근 수집된 거래 날짜 조회

        Args:
            complex_no: 단지 번호
            area_no: 평형 번호

        Returns:
            (year, month, day) 튜플 또는 None
            예: ("2025", "10", 28)
        """
        conn = self.get_conn()
        self.ensure_tables()

        query = """
            SELECT
                EXTRACT(YEAR FROM trade_date)::TEXT as year,
                EXTRACT(MONTH FROM trade_date)::TEXT as month,
                EXTRACT(DAY FROM trade_date)::INTEGER as day
            FROM raw.real_prices
            WHERE complex_no = %s AND area_no = %s
            ORDER BY trade_date DESC
            LIMIT 1
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (str(complex_no), str(area_no)))
            row = cursor.fetchone()

        if row:
            return (row[0], row[1], row[2])
        return None

    @staticmethod
    def _json_ready(doc: Mapping[str, Any]) -> dict[str, Any]:
        """datetime을 ISO 포맷으로 변환하여 JSON 직렬화 준비"""
        ready: dict[str, Any] = {}
        for key, value in doc.items():
            if isinstance(value, datetime):
                target = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
                ready[key] = target.astimezone(timezone.utc).isoformat()
            else:
                ready[key] = value
        return ready

    @staticmethod
    def _ensure_datetime(value: Any) -> datetime | None:
        """값을 datetime으로 변환"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None