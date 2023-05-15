-- BigQuery
-- ========

SELECT * FROM   
  `lens-public-data.polygon.INFORMATION_SCHEMA.TABLES`

SELECT *
FROM `lens-public-data.polygon.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'public_profile'

SELECT 
  sum(size_bytes)/pow(10,9) as size_gb,
  sum(size_bytes)/pow(10,6) as size_mb
FROM
  `lens-public-data.polygon`.__TABLES__
WHERE 
  table_id = 'public_profile'

SELECT 
  table_id,
  sum(size_bytes)/pow(10,9) as size_gb,
  sum(size_bytes)/pow(10,6) as size_mb
FROM
  `lens-public-data.polygon`.__TABLES__
GROUP BY 
  table_id
ORDER BY
  size_gb DESC

-- 
-- 
-- PostgreSQL
-- ==========
SELECT
    pg_size_pretty (pg_relation_size('posts'));


SELECT
    pg_size_pretty (
        pg_total_relation_size ('posts')
    );

SELECT
    pg_size_pretty (
        pg_database_size ('lens')
    );

SELECT
    pg_database.datname,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS size
    FROM pg_database;

SELECT
    relname AS "relation",
    pg_size_pretty (
        pg_total_relation_size (C .oid)
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema'
    )
AND C .relkind <> 'i'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size (C .oid) DESC
LIMIT 20;










