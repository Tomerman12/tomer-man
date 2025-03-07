-- DIMENSIONS

        CREATE TABLE IF NOT EXISTS dim_stock (
            stock_id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            company_name VARCHAR(255),
            CONSTRAINT ticker_unique UNIQUE (ticker)
        );
        


        CREATE TABLE IF NOT EXISTS dim_currency (
            currency_id SERIAL PRIMARY KEY,
            currency_code VARCHAR(3) NOT NULL,
            currency_name VARCHAR(100),
            is_base_currency BOOLEAN DEFAULT FALSE,
            CONSTRAINT currency_code_unique UNIQUE (currency_code)
        );
        

-- FACTS

        CREATE TABLE IF NOT EXISTS fact_stock_price (
            price_id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            stock_id INTEGER NOT NULL REFERENCES dim_stock(stock_id),
            open DECIMAL(18, 6) NOT NULL,
            high DECIMAL(18, 6) NOT NULL,
            low DECIMAL(18, 6) NOT NULL,
            close DECIMAL(18, 6) NOT NULL,
            volume BIGINT NOT NULL,
            CONSTRAINT stock_date_unique UNIQUE (stock_id, trade_date)
        );
        


        CREATE TABLE IF NOT EXISTS fact_exchange_rate (
            rate_id SERIAL PRIMARY KEY,
            rate_date DATE NOT NULL,
            from_currency_id INTEGER NOT NULL REFERENCES dim_currency(currency_id),
            to_currency_id INTEGER NOT NULL REFERENCES dim_currency(currency_id),
            rate DECIMAL(18, 6) NOT NULL,
            CONSTRAINT exchange_rate_unique UNIQUE (rate_date, from_currency_id, to_currency_id)
        );
        

-- VIEWS

        CREATE OR REPLACE VIEW vw_stock_price_any_currency AS
        SELECT 
            sp.trade_date,
            s.ticker,
            fc.currency_code as base_currency,
            tc.currency_code as target_currency,
            sp.open,
            sp.high,
            sp.low,
            sp.close,
            sp.volume,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.open
                ELSE sp.open * er.rate
            END as open_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.high
                ELSE sp.high * er.rate
            END as high_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.low
                ELSE sp.low * er.rate
            END as low_converted,
            CASE 
                WHEN fc.currency_code = tc.currency_code THEN sp.close
                ELSE sp.close * er.rate
            END as close_converted
        FROM 
            fact_stock_price sp
        JOIN 
            dim_stock s ON sp.stock_id = s.stock_id
        CROSS JOIN 
            dim_currency tc
        JOIN 
            dim_currency fc ON fc.is_base_currency = TRUE
        LEFT JOIN 
            fact_exchange_rate er 
            ON sp.trade_date = er.rate_date 
            AND fc.currency_id = er.from_currency_id 
            AND tc.currency_id = er.to_currency_id;
        

-- INDEXES
CREATE INDEX idx_fact_stock_price_trade_date ON fact_stock_price(trade_date);

CREATE INDEX idx_fact_stock_price_stock_id ON fact_stock_price(stock_id);

CREATE INDEX idx_fact_exchange_rate_date ON fact_exchange_rate(rate_date);

CREATE INDEX idx_fact_exchange_rate_from_currency ON fact_exchange_rate(from_currency_id);

CREATE INDEX idx_fact_exchange_rate_to_currency ON fact_exchange_rate(to_currency_id);

