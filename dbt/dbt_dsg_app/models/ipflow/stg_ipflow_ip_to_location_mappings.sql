{{
    config(
        materialized='incremental',
        unique_key='IP',
        incremental_strategy='merge'
    )
}}

WITH 

using_clause AS (

    SELECT 
    IP,
    DATE_UPDATED,
    NORMALIZED_COUNTRY_CODE,
    NORMALIZED_REGION_CODE,
    NORMALIZED_CITY_NAME,
    NORMALIZED_ZIP
    FROM 
    {{ ref('src_ipflow_ip_to_location') }}

),

updates AS (

    SELECT 
    IP,
    DATE_UPDATED,
    NORMALIZED_COUNTRY_CODE,
    NORMALIZED_REGION_CODE,
    NORMALIZED_CITY_NAME,
    NORMALIZED_ZIP 
    FROM 
    using_clause 


    {% if is_incremental() %}

        WHERE IP IN (SELECT IP FROM {{ this }})

    {% endif %}

),

inserts AS (

    SELECT 
    IP,
    DATE_UPDATED,
    NORMALIZED_COUNTRY_CODE,
    NORMALIZED_REGION_CODE,
    NORMALIZED_CITY_NAME,
    NORMALIZED_ZIP 
    FROM 
    using_clause 


    WHERE IP NOT IN (SELECT IP FROM updates)

)

SELECT * FROM updates 
UNION 
SELECT * FROM inserts
