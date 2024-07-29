{{ config(materialized='table') }}

SELECT
  parsed_data.number,
  parsed_data.date,
  parsed_data.company,
  parsed_data.sector,
  parsed_data.stock_exhange,
  parsed_data.tick,
  parsed_data.emiten_code,
  parsed_data.last_price,
  parsed_data.`change` AS change_value,
  parsed_data.temp_market_cap,
  parsed_data.market_cap
FROM
  {{ ref('finance_raw') }},
  UNNEST([STRUCT(
    SAFE.PARSE_JSON(data)._airbyte_data.no AS number,
    SAFE.PARSE_JSON(data)._airbyte_data.date AS date,
    SAFE.PARSE_JSON(data)._airbyte_data.company AS company,
    SAFE.PARSE_JSON(data)._airbyte_data.sector AS sector,
    SAFE.PARSE_JSON(data)._airbyte_data.stock_exhange AS stock_exhange,
    SAFE.PARSE_JSON(data)._airbyte_data.tick AS tick,
    SAFE.PARSE_JSON(data)._airbyte_data.emiten_code AS emiten_code,
    SAFE.PARSE_JSON(data)._airbyte_data.last_price AS last_price,
    SAFE.PARSE_JSON(data)._airbyte_data.`change` AS `change`,
    SAFE.PARSE_JSON(data)._airbyte_data.temp_market_cap AS temp_market_cap,
    SAFE.PARSE_JSON(data)._airbyte_data.market_cap AS market_cap
  )]) AS parsed_data
