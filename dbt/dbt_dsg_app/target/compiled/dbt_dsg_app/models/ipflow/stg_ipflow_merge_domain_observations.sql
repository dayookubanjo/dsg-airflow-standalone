

WITH 

using_clause AS (

    SELECT 
    IP,
    NORMALIZED_COMPANY_DOMAIN,
    SOURCE,
    SOURCE_CONFIDENCE,
    LAST_QUERY_DATE 
    FROM 
    DEV_QA.DBT_POC.src_ipflow_merge_domain_observations 

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


    

        WHERE IP || SOURCE IN (SELECT IP || SOURCE FROM DEV_QA.DBT_POC.stg_ipflow_merge_domain_observations)

    

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