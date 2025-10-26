ARTICLE_DDL = """
    CREATE TABLE IF NOT EXISTS stg_articles (
        complex_no TEXT NOT NULL,
        article_no TEXT NOT NULL,
        payload JSONB NOT NULL,
        article_confirm_date TIMESTAMPTZ,
        expire_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (complex_no, article_no)
    );
"""

COMPLEX_DETAILS_DDL = """
    CREATE TABLE IF NOT EXISTS stg_complex_details (
        complex_no TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""

REAL_PRICE_DDL = """
    CREATE TABLE IF NOT EXISTS stg_real_prices (
        complex_no TEXT NOT NULL,
        area_no TEXT NOT NULL,
        floor TEXT,
        deal_price TEXT,
        formatted_trade_year_month TEXT NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        
        CONSTRAINT stg_real_prices_unique
            UNIQUE (complex_no, area_no, floor, deal_price, formatted_trade_year_month)
    );
"""