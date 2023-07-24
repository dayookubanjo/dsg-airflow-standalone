{{
    config(
        materialized='incremental',
        unique_key=['IP', 'SOURCE'],
        incremental_strategy='merge'
    )
}}

WITH 

using_clause AS (

    SELECT 
    IP,
    NORMALIZED_COMPANY_DOMAIN,
    SOURCE,
    SOURCE_CONFIDENCE,
    LAST_QUERY_DATE 
    FROM 
    {{ ref('src_ipflow_merge_domain_observations') }} 

),

updates AS (

    SELECT 
    IP, 
    NORMALIZED_COMPANY_DOMAIN, 
    SOURCE, 
    SOURCE_CONFIDENCE, 
    LAST_QUERY_DATE 
    FROM 
    using_clause 


    {% if is_incremental() %}

        WHERE IP || SOURCE IN (SELECT IP || SOURCE FROM {{ this }})

    {% endif %}

),

inserts AS (

    SELECT
    IP, 
    NORMALIZED_COMPANY_DOMAIN, 
    SOURCE, 
    SOURCE_CONFIDENCE, 
    LAST_QUERY_DATE 
    FROM 
    using_clause 


    WHERE IP || SOURCE NOT IN (SELECT IP || SOURCE FROM updates)

)

SELECT * FROM updates 
UNION 
SELECT * FROM inserts
