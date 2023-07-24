-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DEV_QA.DBT_POC.stg_ipflow_ip_to_location_mappings as DBT_INTERNAL_DEST
        using DEV_QA.DBT_POC.stg_ipflow_ip_to_location_mappings__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.IP = DBT_INTERNAL_DEST.IP
            )

    
    when matched then update set
        "IP" = DBT_INTERNAL_SOURCE."IP","DATE_UPDATED" = DBT_INTERNAL_SOURCE."DATE_UPDATED","NORMALIZED_COUNTRY_CODE" = DBT_INTERNAL_SOURCE."NORMALIZED_COUNTRY_CODE","NORMALIZED_REGION_CODE" = DBT_INTERNAL_SOURCE."NORMALIZED_REGION_CODE","NORMALIZED_CITY_NAME" = DBT_INTERNAL_SOURCE."NORMALIZED_CITY_NAME","NORMALIZED_ZIP" = DBT_INTERNAL_SOURCE."NORMALIZED_ZIP"
    

    when not matched then insert
        ("IP", "DATE_UPDATED", "NORMALIZED_COUNTRY_CODE", "NORMALIZED_REGION_CODE", "NORMALIZED_CITY_NAME", "NORMALIZED_ZIP")
    values
        ("IP", "DATE_UPDATED", "NORMALIZED_COUNTRY_CODE", "NORMALIZED_REGION_CODE", "NORMALIZED_CITY_NAME", "NORMALIZED_ZIP")

;
    commit;