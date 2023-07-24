

WITH 

using_clause AS (

    SELECT 
    USER_IP,
    LAST_RESPONSE_CODE,
    LAST_QUERY_DATE,
    API_RESPONSE 
    FROM 
    DEV_QA.DBT_POC.src_ipflow_merge_success_output

),

updates AS (

    SELECT 
    USER_IP,
    LAST_RESPONSE_CODE,
    LAST_QUERY_DATE,
    API_RESPONSE 
    FROM 
    using_clause 


    

        WHERE USER_IP IN (SELECT USER_IP FROM DEV_QA.DBT_POC.stg_ipflow_merge_success_output)

    

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