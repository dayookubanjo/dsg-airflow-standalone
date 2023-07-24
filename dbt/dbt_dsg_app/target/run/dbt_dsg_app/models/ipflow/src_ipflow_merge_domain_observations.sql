
  
    

        create or replace transient table DEV_QA.DBT_POC.src_ipflow_merge_domain_observations
         as
        (

WITH source as (
SELECT 
DISTINCT 
USER_IP as IP,
MIN(LAST_RESPONSE_CODE) AS LAST_RESPONSE_CODE,
 MAX(LAST_QUERY_DATE) AS LAST_QUERY_DATE, 
 ANY_VALUE(API_RESPONSE) AS API_RESPONSE, 
 'IP FLOW' AS SOURCE 
 FROM  DEV_QA.DBT_POC.stg_ipflow_success_data 
GROUP BY USER_IP
)

SELECT 
IP,
SPLIT_PART(PARSE_JSON(API_RESPONSE):website::string, '.', 2) || '.' || SPLIT_PART(PARSE_JSON(API_RESPONSE):website::string, '.', 3) AS NORMALIZED_COMPANY_DOMAIN,
SOURCE,
NULL as SOURCE_CONFIDENCE,
LAST_QUERY_DATE 
from 
source
        );
      
  