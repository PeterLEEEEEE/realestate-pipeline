from __future__ import annotations

import psycopg2
from airflow.hooks.base import BaseHook

from src.utils.queries import (
    ARTICLE_DDL,
    COMPLEX_DETAILS_DDL,
    REAL_PRICE_DDL
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
            cursor.execute(ARTICLE_DDL)
            cursor.execute(COMPLEX_DETAILS_DDL)
            cursor.execute(REAL_PRICE_DDL)
            conn.commit()
        
        self._json_tables_ready = True