{{ config(
    materialized = 'table'
) }}

select * from DEV_IP_FLOW.RAW_DATA.IP_FLOW_API_OUTPUT_DATA 
where last_response_code = 200
