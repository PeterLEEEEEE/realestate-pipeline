COMPLEX_DDL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.complexes (
        complex_no TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_complexes_created_at ON raw.complexes(created_at);
"""

ARTICLE_DDL = """
    CREATE SCHEMA IF NOT EXISTS raw;
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
    CREATE INDEX IF NOT EXISTS idx_articles_complex_no ON raw.articles(complex_no);
    CREATE INDEX IF NOT EXISTS idx_articles_article_confirm_date ON raw.articles(article_confirm_date);
    CREATE INDEX IF NOT EXISTS idx_articles_expire_at ON raw.articles(expire_at);
"""

COMPLEX_DETAILS_DDL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.complex_details (
        complex_no TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_complex_details_created_at ON raw.complex_details(created_at);
"""

REAL_PRICE_DDL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.real_prices (
        trade_id TEXT PRIMARY KEY,
        complex_no TEXT NOT NULL,
        area_no TEXT NOT NULL,
        floor TEXT,
        deal_price TEXT,
        formatted_trade_year_month TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_real_prices_complex_no ON raw.real_prices(complex_no);
    CREATE INDEX IF NOT EXISTS idx_real_prices_area_no ON raw.real_prices(area_no);
    CREATE INDEX IF NOT EXISTS idx_real_prices_formatted_trade_year_month ON raw.real_prices(formatted_trade_year_month);
    CREATE INDEX IF NOT EXISTS idx_real_prices_created_at ON raw.real_prices(created_at);
"""
