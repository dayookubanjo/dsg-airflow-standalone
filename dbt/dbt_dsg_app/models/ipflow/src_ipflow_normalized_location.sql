{{ config(
    materialized = 'table'
) }}

with normalized_country as (

  select 
  a.user_ip, 
  a.last_query_date, 
  b.isocode_3 as country_isocode_3,
  PARSE_JSON(a.API_RESPONSE):location:region::string as region_name, 
  PARSE_JSON(a.API_RESPONSE):location:town::string as city_name, 
  
  ---THE IP_TO_LOCATION table only allows numeric zipcodes which is only in the US
  CASE WHEN PARSE_JSON(a.API_RESPONSE):location:countryCode::string = 'US' THEN 
  PARSE_JSON(a.API_RESPONSE):location:postalCode::string END as postal_code 
  
  from {{ ref('stg_ipflow_merge_success_output') }} as a 
  left join DEV_IP_FLOW.raw_data.country_iso_codes as b
  on PARSE_JSON(a.API_RESPONSE):location:countryCode::string = b.isocode_2
), 

normalized_region as (
select a.*, b.region_isocode_2
  from normalized_country as a 
  left join DEV_IP_FLOW.raw_data.usa_region as b
  on a.region_name = b.region_name
)

select distinct
USER_IP as IP,
LAST_QUERY_DATE as DATE_UPDATED,
COUNTRY_ISOCODE_3 as NORMALIZED_COUNTRY_CODE,
REGION_ISOCODE_2 as NORMALIZED_REGION_CODE,
CITY_NAME as NORMALIZED_CITY_NAME,
POSTAL_CODE as NORMALIZED_ZIP
from normalized_region
