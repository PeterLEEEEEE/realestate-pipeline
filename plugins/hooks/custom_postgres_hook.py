from __future__ import annotations

import json
import uuid
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

        각 거래 데이터에 고유한 ID 생성 (complexNo_tradeType_YYYYMMDD_randomHash)
        """
        if not docs:
            return

        conn = self.get_conn()
        self.ensure_tables()

        now = datetime.now(timezone.utc)
        records = []
        for doc in docs:
            complex_no = doc.get("complexNo") or doc.get("complex_no")
            area_no = doc.get("areaNo") or doc.get("area_no")
            formatted = doc.get("formattedTradeYearMonth") or doc.get("formatted_trade_year_month")
            if complex_no is None or area_no is None or formatted is None:
                continue

            floor = doc.get("floor")
            deal_price = doc.get("dealPrice") or doc.get("deal_price")

            # ID 생성
            record_id = self._generate_real_price_id(doc)

            records.append(
                (
                    record_id,
                    str(complex_no),
                    str(area_no),
                    None if floor is None else str(floor),
                    None if deal_price is None else str(deal_price),
                    str(formatted),
                    Json(self._json_ready(doc)),
                    now,
                    now,
                )
            )

        if not records:
            return

        insert_sql = """
            INSERT INTO raw.real_prices (
                trade_id,
                complex_no,
                area_no,
                floor,
                deal_price,
                formatted_trade_year_month,
                payload,
                created_at,
                updated_at
            )
            VALUES %s
        """
        with conn.cursor() as cursor:
            execute_values(cursor, insert_sql, records)
        conn.commit()

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
                SUBSTRING(payload->>'formattedTradeYearMonth', 1, 4) as year,
                SUBSTRING(payload->>'formattedTradeYearMonth', 6, 2) as month,
                CAST(payload->>'tradeDate' as INTEGER) as day
            FROM raw.real_prices
            WHERE complex_no = %s AND area_no = %s
            ORDER BY
                SUBSTRING(payload->>'formattedTradeYearMonth', 1, 4) DESC,
                SUBSTRING(payload->>'formattedTradeYearMonth', 6, 2) DESC,
                CAST(payload->>'tradeDate' as INTEGER) DESC
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

    @staticmethod
    def _generate_real_price_id(doc: Mapping[str, Any]) -> str:
        """
        실거래가 데이터 ID 생성
        Format: {complexNo}_{tradeType}_{YYYYMMDD}_{random_hash}
        예: 236_A1_20250530_a3f2c1d8
        """
        complex_no = doc.get("complexNo") or doc.get("complex_no") or "unknown"
        trade_type = doc.get("tradeType") or "A1" # 디폴트 매매, 추후 전세 등 확장 예정

        # 거래연월일 조합
        trade_date = (doc.get("formattedTradeYearMonth") or "").replace(".", "")

        # 랜덤 해시 (8자리)
        random_hash = uuid.uuid4().hex[:8]

        return f"{complex_no}_{trade_type}_{trade_date}_{random_hash}"