CREATE TABLE dim_companies
(
  company_id    TEXT NOT NULL,
  company_name  TEXT NULL,
  industry_id   INT  NOT NULL,
  sector_id     TEXT NOT NULL,
  country_id    TEXT NOT NULL,
  founded_year  INT  NULL,
  ceo_name      TEXT NULL,
  PRIMARY KEY (company_id)
);

CREATE TABLE dim_countries
(
  country_id    TEXT NOT NULL,
  country_name  TEXT NULL,
  continent     TEXT NULL,
  PRIMARY KEY (country_id)
);

CREATE TABLE dim_economic_indicators
(
  economic_indicator_id TEXT NOT NULL,
  indicator_name         TEXT NULL,
  value                  FLOAT NULL,
  measurement_date       TIMESTAMP NULL,
  country_id             TEXT NOT NULL,
  source                 TEXT NULL,
  PRIMARY KEY (economic_indicator_id)
);

CREATE TABLE dim_exchange_rates
(
  exchange_rate_id  TEXT NOT NULL,
  currency_pair     TEXT NULL,
  rate              FLOAT NULL,
  effective_date    TIMESTAMP NULL,
  source            TEXT NULL,
  PRIMARY KEY (exchange_rate_id)
);

CREATE TABLE dim_exchanges
(
  exchange_id    TEXT NOT NULL,
  exchange_name  TEXT NULL,
  country_id     TEXT NOT NULL,
  founded_year   INT  NULL,
  PRIMARY KEY (exchange_id)
);

CREATE TABLE dim_industries
(
  industry_id   INT  NOT NULL,
  industry_name TEXT NULL,
  sector_id     TEXT NOT NULL,
  PRIMARY KEY (industry_id)
);

CREATE TABLE dim_interest_rates
(
  interest_rate_id  TEXT NOT NULL,
  rate              FLOAT NULL,
  description       TEXT NULL,
  effective_date    TIMESTAMP NULL,
  country_id        TEXT NOT NULL,
  PRIMARY KEY (interest_rate_id)
);

CREATE TABLE dim_sectors
(
  sector_id    TEXT NOT NULL,
  sector_name  TEXT NULL,
  PRIMARY KEY (sector_id)
);

CREATE TABLE dim_stocks
(
  stock_id      TEXT NOT NULL,
  stock_symbol  TEXT NULL,
  company_id    TEXT NOT NULL,
  exchange_id   TEXT NOT NULL,
  isin          TEXT NULL,
  market_cap    BIGINT NULL,
  PRIMARY KEY (stock_id)
);

CREATE TABLE dim_time
(
  transaction_time TIMESTAMP NOT NULL,
  year             INT NULL,
  quarter          INT NULL,
  month            INT NULL,
  day              INT NULL,
  day_of_week      INT NULL,
  hour             INT NULL,
  minute           INT NULL,
  PRIMARY KEY (transaction_time)
);

CREATE TABLE IF NOT EXISTS FACT_STOCK_MARKET
(
  transaction_id         TEXT NOT NULL,
  price                  FLOAT NULL,
  volume                 INT NULL,
  transaction_time       TIMESTAMP NOT NULL,
  interest_rate_id       TEXT NOT NULL,
  industry_id            INT NOT NULL,
  country_id             TEXT NOT NULL,
  exchange_id            TEXT NOT NULL,
  economic_indicator_id  TEXT NOT NULL,
  exchange_rate_id       TEXT NOT NULL,
  sector_id              TEXT NOT NULL,
  stock_id               TEXT NOT NULL,
  company_id             TEXT NOT NULL,
  transaction_type       TEXT NULL,
  PRIMARY KEY (transaction_id, transaction_time)
) PARTITION BY RANGE (transaction_time);


ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_time_TO_FACT_STOCK_MARKET
    FOREIGN KEY (transaction_time)
    REFERENCES dim_time (transaction_time);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_interest_rates_TO_FACT_STOCK_MARKET
    FOREIGN KEY (interest_rate_id)
    REFERENCES dim_interest_rates (interest_rate_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_industries_TO_FACT_STOCK_MARKET
    FOREIGN KEY (industry_id)
    REFERENCES dim_industries (industry_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_countries_TO_FACT_STOCK_MARKET
    FOREIGN KEY (country_id)
    REFERENCES dim_countries (country_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_exchanges_TO_FACT_STOCK_MARKET
    FOREIGN KEY (exchange_id)
    REFERENCES dim_exchanges (exchange_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_economic_indicators_TO_FACT_STOCK_MARKET
    FOREIGN KEY (economic_indicator_id)
    REFERENCES dim_economic_indicators (economic_indicator_id);

ALTER TABLE dim_economic_indicators
  ADD CONSTRAINT FK_dim_countries_TO_dim_economic_indicators
    FOREIGN KEY (country_id)
    REFERENCES dim_countries (country_id);

ALTER TABLE dim_exchanges
  ADD CONSTRAINT FK_dim_countries_TO_dim_exchanges
    FOREIGN KEY (country_id)
    REFERENCES dim_countries (country_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_exchange_rates_TO_FACT_STOCK_MARKET
    FOREIGN KEY (exchange_rate_id)
    REFERENCES dim_exchange_rates (exchange_rate_id);

ALTER TABLE dim_interest_rates
  ADD CONSTRAINT FK_dim_countries_TO_dim_interest_rates
    FOREIGN KEY (country_id)
    REFERENCES dim_countries (country_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_sectors_TO_FACT_STOCK_MARKET
    FOREIGN KEY (sector_id)
    REFERENCES dim_sectors (sector_id);

ALTER TABLE dim_companies
  ADD CONSTRAINT FK_dim_industries_TO_dim_companies
    FOREIGN KEY (industry_id)
    REFERENCES dim_industries (industry_id);

ALTER TABLE dim_companies
  ADD CONSTRAINT FK_dim_sectors_TO_dim_companies
    FOREIGN KEY (sector_id)
    REFERENCES dim_sectors (sector_id);

ALTER TABLE dim_companies
  ADD CONSTRAINT FK_dim_countries_TO_dim_companies
    FOREIGN KEY (country_id)
    REFERENCES dim_countries (country_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_stocks_TO_FACT_STOCK_MARKET
    FOREIGN KEY (stock_id)
    REFERENCES dim_stocks (stock_id);

ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT FK_dim_companies_TO_FACT_STOCK_MARKET
    FOREIGN KEY (company_id)
    REFERENCES dim_companies (company_id);

ALTER TABLE dim_stocks
  ADD CONSTRAINT FK_dim_companies_TO_dim_stocks
    FOREIGN KEY (company_id)
    REFERENCES dim_companies (company_id);

ALTER TABLE dim_stocks
  ADD CONSTRAINT FK_dim_exchanges_TO_dim_stocks
    FOREIGN KEY (exchange_id)
    REFERENCES dim_exchanges (exchange_id);

ALTER TABLE dim_industries
  ADD CONSTRAINT FK_dim_sectors_TO_dim_industries
    FOREIGN KEY (sector_id)
    REFERENCES dim_sectors (sector_id);

-- Thêm ràng buộc CHECK cho bảng dim_companies
ALTER TABLE dim_companies
  ADD CONSTRAINT chk_company_founded_year CHECK (founded_year > 0);

-- Thêm ràng buộc CHECK cho bảng dim_economic_indicators
ALTER TABLE dim_economic_indicators
  ADD CONSTRAINT chk_economic_indicator_value CHECK (value >= 0);

-- Thêm ràng buộc CHECK cho bảng dim_exchange_rates
ALTER TABLE dim_exchange_rates
  ADD CONSTRAINT chk_exchange_rate_rate CHECK (rate >= 0);

-- Thêm ràng buộc CHECK cho bảng dim_exchanges
ALTER TABLE dim_exchanges
  ADD CONSTRAINT chk_exchange_founded_year CHECK (founded_year > 0);

-- Thêm ràng buộc CHECK cho bảng dim_interest_rates
ALTER TABLE dim_interest_rates
  ADD CONSTRAINT chk_interest_rate_rate CHECK (rate >= 0);

-- Thêm ràng buộc CHECK cho bảng dim_stocks
ALTER TABLE dim_stocks
  ADD CONSTRAINT chk_stock_market_cap CHECK (market_cap >= 0);

-- Thêm ràng buộc CHECK cho bảng dim_time
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_year CHECK (year >= 0);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_quarter CHECK (quarter BETWEEN 1 AND 4);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_month CHECK (month BETWEEN 1 AND 12);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_day CHECK (day BETWEEN 1 AND 31);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_day_of_week CHECK (day_of_week BETWEEN 1 AND 7);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_hour CHECK (hour BETWEEN 0 AND 23);
ALTER TABLE dim_time
  ADD CONSTRAINT chk_time_minute CHECK (minute BETWEEN 0 AND 59);

-- Thêm ràng buộc CHECK cho bảng FACT_STOCK_MARKET
ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT chk_fact_stock_market_price CHECK (price >= 0);
ALTER TABLE FACT_STOCK_MARKET
  ADD CONSTRAINT chk_fact_stock_market_volume CHECK (volume >= 0);

