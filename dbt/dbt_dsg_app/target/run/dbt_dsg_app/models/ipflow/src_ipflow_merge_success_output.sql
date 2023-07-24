
  
    

        create or replace transient table DEV_QA.DBT_POC.src_ipflow_merge_success_output
         as
        (

select user_ip, min(last_response_code) as last_response_code,
       max(last_query_date) as last_query_date, any_value(api_response) as api_response 
       from DEV_QA.DBT_POC.stg_ipflow_success_data 
      group by user_ip
        );
      
  