-- PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- SCHEMA 생성
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;      -- Raw Layer: API 원본 데이터
CREATE SCHEMA IF NOT EXISTS staging;  -- Staging Layer: 변환된 데이터 (dbt)
CREATE SCHEMA IF NOT EXISTS mart;     -- Mart Layer: 비즈니스 로직 (dbt)

-- =============================================================================
-- RAW LAYER: API 원본 데이터 (JSONB)
-- - 파싱/변환 없음
-- - API 응답 그대로 저장
-- - dbt에서 Staging으로 변환
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.complexes (
    complex_no TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- JSONB 전체 필드 인덱스 (유연한 쿼리용)
CREATE INDEX IF NOT EXISTS idx_raw_complexes_payload_gin
ON raw.complexes USING GIN (payload);

-- 자주 쿼리하는 필드에 Expression 인덱스
CREATE INDEX IF NOT EXISTS idx_raw_complexes_household_count
ON raw.complexes (((payload->>'totalHouseholdCount')::int));

CREATE INDEX IF NOT EXISTS idx_raw_complexes_type_code
ON raw.complexes ((payload->>'realEstateTypeCode'));

CREATE INDEX IF NOT EXISTS idx_raw_complexes_address
ON raw.complexes ((payload->>'cortarAddress'));


CREATE TABLE IF NOT EXISTS raw.articles (
    complex_no TEXT NOT NULL,
    article_no TEXT NOT NULL,
    payload JSONB NOT NULL,
    article_confirm_date TIMESTAMPTZ,
    expire_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (complex_no, article_no)
);

CREATE INDEX IF NOT EXISTS idx_raw_articles_payload_gin
ON raw.articles USING GIN (payload);


CREATE TABLE IF NOT EXISTS raw.complex_details (
    complex_no TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_complex_details_payload_gin
ON raw.complex_details USING GIN (payload);


CREATE TABLE IF NOT EXISTS raw.real_prices (
    complex_no TEXT NOT NULL,
    area_no TEXT NOT NULL,
    floor TEXT,
    deal_price TEXT,
    formatted_trade_year_month TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT raw_real_prices_unique
        UNIQUE (complex_no, area_no, floor, deal_price, formatted_trade_year_month)
);

CREATE INDEX IF NOT EXISTS idx_raw_real_prices_payload_gin
ON raw.real_prices USING GIN (payload);