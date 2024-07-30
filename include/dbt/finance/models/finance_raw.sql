-- models/finance_raw.sql

{{ config(materialized='table') }}

SELECT *
FROM `data-eng-study-421915.astro_team2.finance_raw`
