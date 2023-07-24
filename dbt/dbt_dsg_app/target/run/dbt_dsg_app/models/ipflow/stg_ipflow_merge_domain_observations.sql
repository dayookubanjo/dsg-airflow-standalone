-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into DEV_QA.DBT_POC.stg_ipflow_merge_domain_observations as DBT_INTERNAL_DEST
        using DEV_QA.DBT_POC.stg_ipflow_merge_domain_observations__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.IP = DBT_INTERNAL_DEST.IP
                ) and (
                    DBT_INTERNAL_SOURCE.SOURCE = DBT_INTERNAL_DEST.SOURCE
                )

    
    when matched then update set
        "IP" = DBT_INTERNAL_SOURCE."IP","NORMALIZED_COMPANY_DOMAIN" = DBT_INTERNAL_SOURCE."NORMALIZED_COMPANY_DOMAIN","SOURCE" = DBT_INTERNAL_SOURCE."SOURCE","SOURCE_CONFIDENCE" = DBT_INTERNAL_SOURCE."SOURCE_CONFIDENCE","LAST_QUERY_DATE" = DBT_INTERNAL_SOURCE."LAST_QUERY_DATE"
    

    when not matched then insert
        ("IP", "NORMALIZED_COMPANY_DOMAIN", "SOURCE", "SOURCE_CONFIDENCE", "LAST_QUERY_DATE")
    values
        ("IP", "NORMALIZED_COMPANY_DOMAIN", "SOURCE", "SOURCE_CONFIDENCE", "LAST_QUERY_DATE")

;
    commit;