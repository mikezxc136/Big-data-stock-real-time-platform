SELECT
    c.relname AS partition_name,
    c.reltuples AS row_count,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM
    pg_class c
JOIN
    pg_inherits i ON c.oid = i.inhrelid
WHERE
    i.inhparent = 'market.fact_stock_market'::regclass;
