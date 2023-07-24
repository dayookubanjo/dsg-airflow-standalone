{{ config(
    materialized = 'table'
) }}

SELECT IP, MAX(DATE_UPDATED) AS DATE_UPDATED, ANY_VALUE(NORMALIZED_COUNTRY_CODE) AS NORMALIZED_COUNTRY_CODE, 
       ANY_VALUE(NORMALIZED_REGION_CODE) AS NORMALIZED_REGION_CODE, 
       ANY_VALUE(NORMALIZED_CITY_NAME) AS NORMALIZED_CITY_NAME, 
       ANY_VALUE(NORMALIZED_ZIP) AS NORMALIZED_ZIP
       FROM {{ ref('src_ipflow_normalized_location') }}
       GROUP BY IP
