
  
    

        create or replace transient table DEV_QA.DBT_POC.stg_ipflow_input_data
         as
        (

with mapped_ips as (
select  DISTINCT BD.USER_IP 
 
FROM DEV_BIDSTREAM.ACTIVITY.USER_ACTIVITY AS BD 
INNER JOIN FIVE_BY_FIVE_DEMO.PRODUCTS.IP_COMPANY_2_10_0 AS FBF 
ON IP_ADDRESS =  USER_IP

UNION 

select  DISTINCT BD.USER_IP 
FROM DEV_BIDSTREAM.ACTIVITY.USER_ACTIVITY AS BD 
INNER JOIN DEV_IP_FLOW.STAGING.IP_FLOW_API_OUTPUT_DATA AS IPF 
ON BD.USER_IP =  IPF.USER_IP
WHERE IPF.LAST_RESPONSE_CODE = 200
)
, unmapped_ips as (
select DISTINCT BD.USER_IP 
FROM DEV_BIDSTREAM.ACTIVITY.USER_ACTIVITY AS BD 
where BD.USER_IP not in (select user_ip from mapped_ips)
) ,
unmapped_ips_frequency as (
select a.USER_IP, count(b.PAGE_URL) as frequency from unmapped_ips as a 
left join DEV_BIDSTREAM.ACTIVITY.USER_ACTIVITY b 
on a.USER_IP = b.USER_IP
group by a.USER_IP
),
unmapped_ips_ranked as (
select a.*, row_number() over (order by frequency desc) as row_number_ranked
  from unmapped_ips_frequency as a
),
unmapped_ips_zero_deprioritized as (
select USER_IP from unmapped_ips_ranked where SPLIT_PART(USER_IP, '.', 4) <> 0
union
select USER_IP from unmapped_ips_ranked where SPLIT_PART(USER_IP, '.', 4) = 0 
)
select USER_IP from unmapped_ips_zero_deprioritized a
where not exists  
( select null from DEV_IP_FLOW.STAGING.IP_FLOW_API_OUTPUT_DATA as b  
where a.USER_IP = b.USER_IP   
and ( b.LAST_QUERY_DATE >= DATE( DATEADD(day, -30, GETDATE()) ) 
and b.LAST_RESPONSE_CODE = 204 )  )
limit 1
        );
      
  