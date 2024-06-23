-- create index for dim_companies
CREATE INDEX idx_dim_companies_company_name ON dim_companies (company_name);
CREATE INDEX idx_dim_companies_industry_id ON dim_companies (industry_id);
CREATE INDEX idx_dim_companies_sector_id ON dim_companies (sector_id);
CREATE INDEX idx_dim_companies_country_id ON dim_companies (country_id);

-- create index for dim_countries
CREATE INDEX idx_dim_countries_country_name ON dim_countries (country_name);
CREATE INDEX idx_dim_countries_continent ON dim_countries (continent);

-- create index for dim_economic_indicators
CREATE INDEX idx_dim_economic_indicators_indicator_name ON dim_economic_indicators (indicator_name);
CREATE INDEX idx_dim_economic_indicators_measurement_date ON dim_economic_indicators (measurement_date);
CREATE INDEX idx_dim_economic_indicators_country_id ON dim_economic_indicators (country_id);

-- create index for dim_exchange_rates
CREATE INDEX idx_dim_exchange_rates_currency_pair ON dim_exchange_rates (currency_pair);
CREATE INDEX idx_dim_exchange_rates_effective_date ON dim_exchange_rates (effective_date);

-- create index for dim_exchanges
CREATE INDEX idx_dim_exchanges_exchange_name ON dim_exchanges (exchange_name);
CREATE INDEX idx_dim_exchanges_country_id ON dim_exchanges (country_id);

-- create index for dim_industries
CREATE INDEX idx_dim_industries_industry_name ON dim_industries (industry_name);
CREATE INDEX idx_dim_industries_sector_id ON dim_industries (sector_id);

-- create index for dim_interest_rates
CREATE INDEX idx_dim_interest_rates_rate ON dim_interest_rates (rate);
CREATE INDEX idx_dim_interest_rates_effective_date ON dim_interest_rates (effective_date);
CREATE INDEX idx_dim_interest_rates_country_id ON dim_interest_rates (country_id);

-- create index for dim_sectors
CREATE INDEX idx_dim_sectors_sector_name ON dim_sectors (sector_name);

-- create index for dim_stocks
CREATE INDEX idx_dim_stocks_stock_symbol ON dim_stocks (stock_symbol);
CREATE INDEX idx_dim_stocks_company_id ON dim_stocks (company_id);
CREATE INDEX idx_dim_stocks_exchange_id ON dim_stocks (exchange_id);
CREATE INDEX idx_dim_stocks_isin ON dim_stocks (isin);

-- create index for dim_time
CREATE INDEX idx_dim_time_year ON dim_time (year);
CREATE INDEX idx_dim_time_quarter ON dim_time (quarter);
CREATE INDEX idx_dim_time_month ON dim_time (month);
CREATE INDEX idx_dim_time_day ON dim_time (day);
CREATE INDEX idx_dim_time_day_of_week ON dim_time (day_of_week);
CREATE INDEX idx_dim_time_hour ON dim_time (hour);
CREATE INDEX idx_dim_time_minute ON dim_time (minute);

-- create index for FACT_STOCK_MARKET
CREATE INDEX idx_fact_stock_market_price ON FACT_STOCK_MARKET (price);
CREATE INDEX idx_fact_stock_market_volume ON FACT_STOCK_MARKET (volume);
CREATE INDEX idx_fact_stock_market_transaction_time ON FACT_STOCK_MARKET (transaction_time);
CREATE INDEX idx_fact_stock_market_interest_rate_id ON FACT_STOCK_MARKET (interest_rate_id);
CREATE INDEX idx_fact_stock_market_industry_id ON FACT_STOCK_MARKET (industry_id);
CREATE INDEX idx_fact_stock_market_country_id ON FACT_STOCK_MARKET (country_id);
CREATE INDEX idx_fact_stock_market_exchange_id ON FACT_STOCK_MARKET (exchange_id);
CREATE INDEX idx_fact_stock_market_economic_indicator_id ON FACT_STOCK_MARKET (economic_indicator_id);
CREATE INDEX idx_fact_stock_market_exchange_rate_id ON FACT_STOCK_MARKET (exchange_rate_id);
CREATE INDEX idx_fact_stock_market_sector_id ON FACT_STOCK_MARKET (sector_id);
CREATE INDEX idx_fact_stock_market_stock_id ON FACT_STOCK_MARKET (stock_id);
CREATE INDEX idx_fact_stock_market_company_id ON FACT_STOCK_MARKET (company_id);
CREATE INDEX idx_fact_stock_market_transaction_type ON FACT_STOCK_MARKET (transaction_type);
