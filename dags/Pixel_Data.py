import logging
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----SNS Failure notification----

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Pixel_data_processing"
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Pixel_data_processing"
        ,subject="DAG Success"
        ,message=f"Pixel Data Processing has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

dag = DAG('Pixel_Data_Processing', start_date = datetime(2022, 10, 29), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'on_failure_callback': on_failure_callback,'on_success_callback': None})

#-----Snowflake queries----

#-----Copy Raw data from S3----
copy_query = ["copy into dev_pixel.raw_data.raw_pixel_data from @dev_pixel.raw_data.raw_pixel_data purge=True;"]

#-----User activity----
merge_insert_user_activity = ["""
merge into dev_pixel.activity.user_activity t
using (
with test_pixel as
(select
    pixel_record:"userId"::varchar as client_id,
    pixel_record:"ip"::varchar as ip,
    split_part (ip,',',-1) as user_ip,
    date(dev_pixel.public.date_normalizer(pixel_record:"date"),'dd-mm-yyyy') as date,
    pixel_record:"userAgent"::varchar as user_agent,
    pixel_record:"thirdPartyId"::varchar as site_url,
    count(*) as pageviews 
from DEV_PIXEL.RAW_DATA.RAW_PIXEL_DATA
group by 1,2,3,4,5,6
UNION  
select
    pixel_record:"userId"::varchar as client_id,
    pixel_record:"ip"::varchar as ip,
    split_part (ip,',',1) as user_ip,
    date(dev_pixel.public.date_normalizer(pixel_record:"date"),'dd-mm-yyyy') as date,
    pixel_record:"userAgent"::varchar as user_agent,
    pixel_record:"thirdPartyId"::varchar as site_url,
    count(*) as pageviews
from DEV_PIXEL.RAW_DATA.RAW_PIXEL_DATA 
group by 1,2,3,4,5,6)
select client_id,user_ip,date,user_agent,site_url,pageviews from test_pixel) s
on t.user_ip = s.user_ip 
and t.date = s.date
and t.user_agent = s.user_agent
and t.site_url = s.site_url
and t.client_id = s.client_id
when matched then update set
pageviews = s.pageviews + t.pageviews
when not matched then insert
(client_id, user_ip, date, user_agent, site_url, pageviews)
values
(s.client_id, s.user_ip, s.date, s.user_agent, s.site_url, s.pageviews);
"""]

#-----Delete raw data-----
clear_raw_data_cache = ["""
truncate table dev_pixel.raw_data.raw_pixel_data;
"""]

#-----Unique url's-----
merge_insert_unique_urls = ["""
merge into "DEV_PIXEL"."CONTENT"."UNIQUE_URLS" t
using (select Site_url as page_url,
       hash(site_url) as page_url_hash,
       dev_datamart.public.domain_normalizer(ifnull(Parse_url(SITE_URL,1):host::varchar,'')) as PUBLISHER_DOMAIN_NORMALIZED,
       min(date) as first_seen,
       max(date) as last_seen,
       count(*) as frequency
       from"DEV_PIXEL"."ACTIVITY"."USER_ACTIVITY" where len(page_url)>1 group by 1) s
on s.Page_url = t.page_url
when matched then update set
LAST_SEEN=s.LAST_SEEN,
frequency=s.frequency
when not matched then insert (page_url,page_url_hash,PUBLISHER_DOMAIN_NORMALIZED,first_seen,last_seen,frequency)
values (s.page_url,s.page_url_hash,s.PUBLISHER_DOMAIN_NORMALIZED,s.first_seen,s.last_seen,s.frequency);
"""]

#-----Merge into Webscraper input-----
merge_insert_WebScraper_input_cache = ["""
merge into "DEV_AIML"."WEB_SCRAPER"."INPUT_CACHE" t
using (select distinct a.page_url,a.last_seen from "DEV_PIXEL"."CONTENT"."UNIQUE_URLS" a
       left join dev_aiml.web_scraper.results b
       on a.page_url = b.page_url where last_scraped is null
       and publisher_domain_normalized not in (select distinct publisher_domain_normalized from DEV_DATAMART.FILTERS_SUPPRESSIONS.HIGH_VOLUME_BAD_PUBS_SUPPRESS))s
on t.page_url = s.page_url 
when not matched then insert
(page_url, date_inserted)
values
(s.page_url, current_date)
when matched then update set
date_inserted = current_date;
"""]

#-----Ip Mappings-----
merge_insert_Digital_element_observation = ["""merge into dev_datamart.entity_mappings.ip_to_company_domain_observations t
using (
   select distinct
        pw.user_ip as ip,
        de.normalized_company_domain,
        de.date_updated
    from "DEV_PIXEL"."ACTIVITY"."USER_ACTIVITY" pw
    join dev_digital_element.mappings.ip_range_mappings_filtered de
    on dev_digital_element.public.ip_to_number(pw.user_ip) between de.ip_range_start_numeric and de.ip_range_end_numeric
    where ip is not null and len(ip) > 0 and ip not ilike '%:%'
    and normalized_company_domain is not null and len(normalized_company_domain)>1) s
on s.ip = t.ip
and s.normalized_company_domain = t.normalized_company_domain
and t.source = 'DIGITAL ELEMENT'
and t.date = s.date_updated
when not matched then insert
(ip, normalized_company_domain, source, date)
values
(s.ip, s.normalized_company_domain, 'DIGITAL ELEMENT', s.date_updated);
"""]

#-----Company Activity-----
merge_insert_company_activity_cache = ["""
set user_activity_watermark = (select ifnull(max(date), '2022-10-13') from dev_pixel.activity.company_activity);
""",
"""
set user_activity_max_date = (select dateadd(day,1, $user_activity_watermark));
""",
"""
merge into "DEV_PIXEL"."ACTIVITY"."COMPANY_ACTIVITY_CACHE" t using 
(select a.site_url as page_url,
       dev_datamart.public.domain_normalizer(ifnull(Parse_url(a.SITE_URL,1):host::varchar,'')) as PUBLISHER_DOMAIN_NORMALIZED,
       b.normalized_company_domain,
       a.date,
       c.normalized_country_code, 
       c.normalized_region_code, 
       c.normalized_city_name,
       c.normalized_zip,
       sum(pageviews) as pageviews,
       sum(pageviews*score) as Weighted_pageviews,
       count(distinct USER_AGENT) as unique_devices,
       count(distinct user_ip) as unique_ips 
       from "DEV_PIXEL"."ACTIVITY"."USER_ACTIVITY" a
       join dev_datamart.entity_mappings.ip_to_company_domain b
       on a.user_ip = b.ip
       join dev_datamart.entity_mappings.ip_to_location c
       on a.user_ip = c.ip
       where a.date >= $user_activity_watermark and a.date <= $user_activity_max_date
       group by 1,2,3,4,5,6,7,8)s
       on t.page_url=s.page_url
       and t.date=s.date
       and t.NORMALIZED_COMPANY_DOMAIN=s.NORMALIZED_COMPANY_DOMAIN
       and t.NORMALIZED_COUNTRY_CODE=s.NORMALIZED_COUNTRY_CODE
       and equal_null(t.normalized_region_code,s.normalized_region_code)=true
       and equal_null(t.normalized_city_name,s.normalized_city_name)=true
       and equal_null(t.normalized_zip,s.normalized_zip)=true
       when matched then update set
       PAGEVIEWS=s.PAGEVIEWS,
       WEIGHTED_PAGEVIEWS=s.WEIGHTED_PAGEVIEWS,
       UNIQUE_DEVICES=s.UNIQUE_DEVICES,
       UNIQUE_IPS=s.UNIQUE_IPS
       when not matched then insert
       (PAGE_URL,PUBLISHER_DOMAIN_NORMALIZED,NORMALIZED_COMPANY_DOMAIN,DATE,NORMALIZED_COUNTRY_CODE,NORMALIZED_REGION_CODE,NORMALIZED_CITY_NAME,NORMALIZED_ZIP,PAGEVIEWS,WEIGHTED_PAGEVIEWS,UNIQUE_DEVICES,UNIQUE_IPS)
       values(S.PAGE_URL,S.PUBLISHER_DOMAIN_NORMALIZED,S.NORMALIZED_COMPANY_DOMAIN,S.DATE,S.NORMALIZED_COUNTRY_CODE,S.NORMALIZED_REGION_CODE,S.NORMALIZED_CITY_NAME,S.NORMALIZED_ZIP,S.PAGEVIEWS,S.WEIGHTED_PAGEVIEWS,S.UNIQUE_DEVICES,S.UNIQUE_IPS);
       """]

merge_insert_company_activity = ["""
merge into dev_pixel.activity.company_activity t using
(select * from dev_pixel.activity.company_activity_cache)s
on t.page_url=s.page_url
and t.date=s.date
and t.NORMALIZED_COMPANY_DOMAIN=s.NORMALIZED_COMPANY_DOMAIN
and t.NORMALIZED_COUNTRY_CODE=s.NORMALIZED_COUNTRY_CODE
and equal_null(t.normalized_region_code,s.normalized_region_code)=true
and equal_null(t.normalized_city_name,s.normalized_city_name)=true
and equal_null(t.normalized_zip,s.normalized_zip)=true
when matched then update set
PAGEVIEWS=s.PAGEVIEWS,
WEIGHTED_PAGEVIEWS=s.WEIGHTED_PAGEVIEWS,
UNIQUE_DEVICES=s.UNIQUE_DEVICES,
UNIQUE_IPS=s.UNIQUE_IPS
when not matched then insert
(PAGE_URL,PUBLISHER_DOMAIN_NORMALIZED,NORMALIZED_COMPANY_DOMAIN,DATE,NORMALIZED_COUNTRY_CODE,NORMALIZED_REGION_CODE,NORMALIZED_CITY_NAME,NORMALIZED_ZIP,PAGEVIEWS,WEIGHTED_PAGEVIEWS,UNIQUE_DEVICES,UNIQUE_IPS)
values(S.PAGE_URL,S.PUBLISHER_DOMAIN_NORMALIZED,S.NORMALIZED_COMPANY_DOMAIN,S.DATE,S.NORMALIZED_COUNTRY_CODE,S.NORMALIZED_REGION_CODE,S.NORMALIZED_CITY_NAME,S.NORMALIZED_ZIP,S.PAGEVIEWS,S.WEIGHTED_PAGEVIEWS,S.UNIQUE_DEVICES,S.UNIQUE_IPS);
"""]
#-----Pre-scoring-----
merge_insert_prescoring_cache=["""merge into dev_pixel.activity.prescoring_cache t
using (
select a.*,
       b.intent_topics,
       c.context as context_output,
       d.topics as title_output
    from "DEV_PIXEL"."ACTIVITY"."COMPANY_ACTIVITY_CACHE" a
    join "DEV_AIML"."TAXONOMY_CLASSIFIER"."OUTPUT" b
    on a.page_url = b.page_url
    join "DEV_AIML"."CONTEXT_CLASSIFIER"."OUTPUT" c
    on a.page_url = c.page_url
    join "DEV_AIML"."TITLE_CLASSIFIER"."OUTPUT" d
    on a.page_url = d.page_url)s
on t.page_url=s.page_url
and t.normalized_company_domain=s.normalized_company_domain
and t.date=s.date
and t.normalized_country_code=s.normalized_country_code
and equal_null(t.normalized_region_code,s.normalized_region_code)=true
and equal_null(t.normalized_city_name,s.normalized_city_name)=true
and equal_null(t.normalized_zip,s.normalized_zip)=true
when matched then update set
pageviews = s.pageviews,
weighted_pageviews=s.weighted_pageviews,
publisher_domain_normalized=s.publisher_domain_normalized,
unique_devices = s.unique_devices,
unique_ips = s.unique_ips,
intent_topics = s.intent_topics,
context_output = s.context_output,
title_output = s.title_output
when not matched then insert
(page_url,publisher_domain_normalized,normalized_company_domain,date,normalized_country_code,normalized_region_code,normalized_city_name,normalized_zip,pageviews,weighted_pageviews,unique_devices,unique_ips,intent_topics,context_output,title_output)
 values
(s.page_url,s.publisher_domain_normalized,s.normalized_company_domain,s.date,s.normalized_country_code,s.normalized_region_code,s.normalized_city_name,s.normalized_zip,s.pageviews,s.weighted_pageviews,s.unique_devices,s.unique_ips,s.intent_topics,s.context_output,s.title_output)
;"""]

#-----Delete from Company_activity_cache-----
delete_from_company_activity_cache=["""delete from "DEV_PIXEL"."ACTIVITY"."COMPANY_ACTIVITY_CACHE" a
using "DEV_PIXEL"."ACTIVITY"."PRESCORING_CACHE" b
where a.page_url=b.page_url
and a.NORMALIZED_COMPANY_DOMAIN=b.NORMALIZED_COMPANY_DOMAIN
and a.date=b.date
and a.NORMALIZED_COUNTRY_CODE=b.NORMALIZED_COUNTRY_CODE
and equal_null(a.normalized_region_code,b.normalized_region_code)=true
and equal_null(a.normalized_city_name,b.normalized_city_name)=true
and equal_null(a.normalized_zip,b.normalized_zip)= true;"""]
 
with dag:
  copy_query_exec = SnowflakeOperator(
    task_id= "load_from_s3",
    sql= copy_query,
    snowflake_conn_id= "Airflow-Dev_load-connection",
    )
  
  merge_into_user_activity_exec = SnowflakeOperator(
    task_id= "merge_into_user_activity",
    sql= merge_insert_user_activity,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )

  clear_raw_data_cache_exec = SnowflakeOperator(
    task_id= "clear_raw_data_cache",
    sql= clear_raw_data_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )

  merge_into_unique_urls_exec = SnowflakeOperator(
    task_id= "merge_into_unique_urls",
    sql= merge_insert_unique_urls,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  
  merge_into_WebScraper_input_cache_exec = SnowflakeOperator(
    task_id= "merge_into_WebScraper_input_cache",
    sql= merge_insert_WebScraper_input_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  
  merge_into_Digital_element_observation_exec = SnowflakeOperator(
    task_id= "merge_into_Digital_element_observation",
    sql= merge_insert_Digital_element_observation,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
    
  merge_into_company_activity_cache_exec = SnowflakeOperator(
    task_id= "merge_into_company_activity_cache",
    sql= merge_insert_company_activity_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  
  merge_into_company_activity_exec = SnowflakeOperator(
    task_id= "merge_into_company_activity",
    sql= merge_insert_company_activity,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  
  merge_into_prescoring_cache_exec = SnowflakeOperator(
    task_id= "merge_into_prescoring_cache",
    sql= merge_insert_prescoring_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  
  delete_from_company_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_company_activity_cache",
    on_success_callback=on_success_callback,
    sql= delete_from_company_activity_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )
  copy_query_exec >> merge_into_user_activity_exec  >> clear_raw_data_cache_exec >> merge_into_unique_urls_exec >> merge_into_WebScraper_input_cache_exec >> merge_into_Digital_element_observation_exec >> merge_into_company_activity_cache_exec >> merge_into_company_activity_exec >> merge_into_prescoring_cache_exec >> delete_from_company_activity_cache_exec
