-- models/finance_dwh.sql

{{ config(materialized='table') }}

SELECT
  CAST(REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.number'), '"', '') AS INT64) AS number,
  PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.date'), '"', '')) AS date,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.company'), '"', '') AS company,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.sector'), '"', '') AS sector,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.stock_exhange'), '"', '') AS stock_exhange,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.tick'), '"', '') AS tick,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.emiten_code'), '"', '') AS emiten_code,
  CAST(REPLACE(REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.last_price'), '"', ''), ',', '') AS FLOAT64) AS last_price,
  CAST(REPLACE(REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.change_value'), '"', ''), '%', '') AS FLOAT64) AS change_value,
  CAST(REPLACE(REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.temp_market_cap'), '"', ''), ',', '') AS INT64) AS temp_market_cap,
  REPLACE(JSON_EXTRACT_SCALAR(to_json_string(t), '$.market_cap'), '"', '') AS market_cap
FROM
  {{ ref('finance_staging') }} t
