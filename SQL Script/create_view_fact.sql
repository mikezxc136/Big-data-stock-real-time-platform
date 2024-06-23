CREATE MATERIALIZED VIEW market.mv_stock_summary AS
SELECT
  stock_id,
  AVG(price) AS avg_price,
  SUM(volume) AS total_volume
FROM
  market.FACT_STOCK_MARKET
GROUP BY
  stock_id;
