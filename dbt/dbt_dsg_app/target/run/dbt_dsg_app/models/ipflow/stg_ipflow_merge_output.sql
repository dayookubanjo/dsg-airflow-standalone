-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DEV_QA.DBT_POC.stg_ipflow_merge_output as DBT_INTERNAL_DEST
        using DEV_QA.DBT_POC.stg_ipflow_merge_output__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.USER_IP = DBT_INTERNAL_DEST.USER_IP
            )

    
    when matched then update set
        "USER_IP" = DBT_INTERNAL_SOURCE."USER_IP","LAST_RESPONSE_CODE" = DBT_INTERNAL_SOURCE."LAST_RESPONSE_CODE","LAST_QUERY_DATE" = DBT_INTERNAL_SOURCE."LAST_QUERY_DATE","API_RESPONSE" = DBT_INTERNAL_SOURCE."API_RESPONSE"
    

    when not matched then insert
        ("USER_IP", "LAST_RESPONSE_CODE", "LAST_QUERY_DATE", "API_RESPONSE")
    values
        ("USER_IP", "LAST_RESPONSE_CODE", "LAST_QUERY_DATE", "API_RESPONSE")

;
    commit;