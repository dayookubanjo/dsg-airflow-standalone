

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
    DEV_QA.DBT_POC.src_ipflow_ip_to_location

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


    

        WHERE IP IN (SELECT IP FROM DEV_QA.DBT_POC.stg_ipflow_ip_to_location_mappings)

    

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