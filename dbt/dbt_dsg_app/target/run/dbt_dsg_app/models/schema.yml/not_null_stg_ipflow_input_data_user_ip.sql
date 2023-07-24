select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_ip
from DEV_QA.DBT_POC.stg_ipflow_input_data
where user_ip is null



      
    ) dbt_internal_test