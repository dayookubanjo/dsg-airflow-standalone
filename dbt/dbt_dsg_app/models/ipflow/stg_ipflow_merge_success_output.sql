{{
    config(
        materialized='incremental',
        unique_key='USER_IP',
        incremental_strategy='merge'
    )
}}

WITH 

using_clause AS (

    SELECT 
    USER_IP,
    LAST_RESPONSE_CODE,
    LAST_QUERY_DATE,
    API_RESPONSE 
    FROM 
    {{ ref('src_ipflow_merge_success_output') }}

),

updates AS (

    SELECT 
    USER_IP,
    LAST_RESPONSE_CODE,
    LAST_QUERY_DATE,
    API_RESPONSE 
    FROM 
    using_clause 


    {% if is_incremental() %}

        WHERE USER_IP IN (SELECT USER_IP FROM {{ this }})

    {% endif %}

),

inserts AS (

    SELECT 
    USER_IP,
    LAST_RESPONSE_CODE,
    LAST_QUERY_DATE,
    API_RESPONSE 
    FROM 
    using_clause 


    WHERE USER_IP NOT IN (SELECT USER_IP FROM updates)

)

SELECT * FROM updates 
UNION 
SELECT * FROM inserts
