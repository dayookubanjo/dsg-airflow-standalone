import logging
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----Importing Variables----
SNS_ARN=Variable.get("SNS_ARN")
PIXEL_DATABASE = Variable.get("PIXEL_DATABASE")
DATAMART_DATABASE = Variable.get("DATAMART_DATABASE")
AIML_DATABASE = Variable.get("AIML_DATABASE")
DE_DATABASE = Variable.get("DE_DATABASE")
LOAD_CONNECTION = Variable.get("LOAD_CONNECTION")
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")

#-----SNS Failure notification----

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="PIXEL DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="PIXEL DAG SUCCESS"
        ,message=f"Pixel Data Processing has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

def end_success():
  logger.info("DAG Ended Successfully.")

dag = DAG('Pixel_Data_Processing', start_date = datetime(2022, 10, 29), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'on_failure_callback': on_failure_callback,'on_success_callback': None})


#-----Snowflake queries----

#-----Copy Raw data from S3----
copy_query = [f"""copy into {PIXEL_DATABASE}.raw_data.raw_pixel_data from @{PIXEL_DATABASE}.raw_data.raw_pixel_data purge=True;"""]

#-----User activity----
merge_insert_user_activity = [f"""
merge into {PIXEL_DATABASE}.activity.user_activity t
using (
with test_pixel as
(select
    pixel_record:"userId"::varchar as client_id,
    pixel_record:"ip"::varchar as ip,
    split_part (ip,',',-1) as user_ip,
    date({PIXEL_DATABASE}.public.date_normalizer(pixel_record:"date"),'dd-mm-yyyy') as date,
    pixel_record:"userAgent"::varchar as user_agent,
    pixel_record:"thirdPartyId"::varchar as site_url,
    count(*) as pageviews 
from {PIXEL_DATABASE}.raw_data.raw_pixel_data
group by 1,2,3,4,5,6
UNION  
select
    pixel_record:"userId"::varchar as client_id,
    pixel_record:"ip"::varchar as ip,
    split_part (ip,',',1) as user_ip,
    date({PIXEL_DATABASE}.public.date_normalizer(pixel_record:"date"),'dd-mm-yyyy') as date,
    pixel_record:"userAgent"::varchar as user_agent,
    pixel_record:"thirdPartyId"::varchar as site_url,
    count(*) as pageviews
from {PIXEL_DATABASE}.raw_data.raw_pixel_data 
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
clear_raw_data_cache = [f"""
truncate table {PIXEL_DATABASE}.raw_data.raw_pixel_data;
"""]

#-----Unique url's-----
merge_insert_unique_urls = [f"""
merge into {PIXEL_DATABASE}.content.unique_urls t
using (select Site_url as page_url,
       hash(site_url) as page_url_hash,
       {DATAMART_DATABASE}.public.domain_normalizer(ifnull(Parse_url(SITE_URL,1):host::varchar,'')) as PUBLISHER_DOMAIN_NORMALIZED,
       min(date) as first_seen,
       max(date) as last_seen,
       count(*) as frequency
       from {PIXEL_DATABASE}.activity.user_activity where len(page_url)>1 group by 1) s
on s.Page_url = t.page_url
when matched then update set
LAST_SEEN=s.LAST_SEEN,
frequency=s.frequency
when not matched then insert (page_url,page_url_hash,PUBLISHER_DOMAIN_NORMALIZED,first_seen,last_seen,frequency)
values (s.page_url,s.page_url_hash,s.PUBLISHER_DOMAIN_NORMALIZED,s.first_seen,s.last_seen,s.frequency);
"""]

#-----Merge into Webscraper input-----
merge_insert_WebScraper_input_cache = [f"""
merge into {AIML_DATABASE}.web_scraper.input_cache t
using (select distinct a.page_url,a.last_seen from {PIXEL_DATABASE}.content.unique_urls a
       left join {AIML_DATABASE}.web_scraper.results b
       on a.page_url = b.page_url where last_scraped is null
       and publisher_domain_normalized not in (select distinct publisher_domain_normalized from {DATAMART_DATABASE}.filters_suppressions.high_volume_bad_pubs_suppress))s
on t.page_url = s.page_url 
when not matched then insert
(page_url, date_inserted)
values
(s.page_url, current_date)
when matched then update set
date_inserted = current_date;
"""]

#-----Ip Mappings-----
merge_insert_Digital_element_observation = [f"""merge into {DATAMART_DATABASE}.entity_mappings.ip_to_company_domain_observations t
using (
   select distinct
        pw.user_ip as ip,
        de.normalized_company_domain,
        de.date_updated
    from {PIXEL_DATABASE}.activity.user_activity pw
    join {DE_DATABASE}.mappings.ip_range_mappings_filtered de
    on {DE_DATABASE}.public.ip_to_number(pw.user_ip) between de.ip_range_start_numeric and de.ip_range_end_numeric
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
merge_insert_company_activity_cache = [f"""
set user_activity_watermark = (select ifnull(max(date), '2022-10-13') from {PIXEL_DATABASE}.activity.company_activity);
""",
"""
set user_activity_max_date = (select dateadd(day,15, $user_activity_watermark));
""",
f"""
merge into {PIXEL_DATABASE}.activity.company_activity_cache t using 
(select a.site_url as page_url,
       {DATAMART_DATABASE}.public.domain_normalizer(ifnull(Parse_url(a.SITE_URL,1):host::varchar,'')) as PUBLISHER_DOMAIN_NORMALIZED,
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
       from {PIXEL_DATABASE}.activity.user_activity a
       join {DATAMART_DATABASE}.entity_mappings.ip_to_company_domain b
       on a.user_ip = b.ip
       join {DATAMART_DATABASE}.entity_mappings.ip_to_location c
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

merge_insert_company_activity = [f"""
merge into {PIXEL_DATABASE}.activity.company_activity t using
(select * from {PIXEL_DATABASE}.activity.company_activity_cache)s
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
enriched_company_activity_cache = [f"""
-- create table that will be used to merge into the prescoring cache and delete from the company activity cache
create or replace transient table {PIXEL_DATABASE}.activity.enriched_company_activity_cache as
with flattened_topics as (
select distinct
  t.page_url,
  f.value:"parentCategory"::varchar as parent_category,
  f.value:"category"::varchar as category,
  f.value:"topic"::varchar as topic,
  f.value:"probability"::varchar as probability
from {AIML_DATABASE}."TAXONOMY_CLASSIFIER"."OUTPUT" t,
lateral flatten(input=>intent_topics) f
),
flattened_brands as (
select distinct
  t.page_url,
  f.value:"parentCategory"::varchar as parent_category,
  f.value:"category"::varchar as category,
  f.value:"topic"::varchar as topic,
  f.value:"probability"::varchar as probability
from {AIML_DATABASE}."BRANDS_IDENTIFICATION"."OUTPUT" t,
lateral flatten(input=>brand_topics) f
)
select activity.*,
        parent_category,
        category,
        topic,
        probability,
        context.context as context_output,
        title.topics as title_output,
        title.url_type,
        title.activity_type,
        title.information_type
    from {PIXEL_DATABASE}.activity.company_activity_cache activity
    join flattened_topics taxo
    on activity.page_url = taxo.page_url
    join {AIML_DATABASE}."CONTEXT_CLASSIFIER"."OUTPUT" context
    on activity.page_url = context.page_url
    join {AIML_DATABASE}."TITLE_CLASSIFIER"."OUTPUT" title
    on activity.page_url = title.page_url
union 
select activity.*,
        parent_category,
        category,
        topic,
        probability,
        context.context as context_output,
        title.topics as title_output,
        title.url_type,
        title.activity_type,
        title.information_type
    from {PIXEL_DATABASE}.activity.company_activity_cache activity
    join flattened_brands brands
    on activity.page_url = brands.page_url
    join {AIML_DATABASE}."CONTEXT_CLASSIFIER"."OUTPUT" context
    on activity.page_url = context.page_url
    join {AIML_DATABASE}."TITLE_CLASSIFIER"."OUTPUT" title
    on activity.page_url = title.page_url;
"""]

merge_insert_prescoring_cache=[f"""merge into {PIXEL_DATABASE}.activity.prescoring_cache t
using (
  -- flatten enriched_company_activity_cache
  select
        -- rollup cols
        normalized_company_domain,
        parent_category,
        category,
        topic,
        normalized_country_code,
        normalized_region_code,
        normalized_city_name,
        normalized_zip,
        date,
        -- feature cols
        sum(pageviews) as pageviews,
        sum(probability*weighted_pageviews*greatest(title_output:"BUSINESS",title_output:"BUSINESS NEWS",title_output:"SCIENCE TECH NEWS")) as weighted_pageviews,
        1.0 as avg_page_relevance,
        100 as activity_type_score,
        100 as information_type_score,
        sum(context_output:"review/comparison") as review_pageviews,
        count(distinct page_url) as unique_pages,
        count(distinct publisher_domain_normalized) as unique_pubs,
        sum(unique_devices) as unique_devices,
        sum(unique_ips) as unique_ips
        -- table definition
        from {PIXEL_DATABASE}.activity.enriched_company_activity_cache
        group by 1,2,3,4,5,6,7,8,9
) s
on t.parent_category = s.category
and t.category = s.category
and t.topic = s.category
and t.normalized_company_domain=s.normalized_company_domain
and t.date=s.date
and equal_null(t.normalized_country_code,s.normalized_country_code)=true
and equal_null(t.normalized_region_code,s.normalized_region_code)=true
and equal_null(t.normalized_city_name,s.normalized_city_name)=true
and equal_null(t.normalized_zip,s.normalized_zip)=true
when matched then update set
pageviews = s.pageviews + t.pageviews,
weighted_pageviews = s.weighted_pageviews + t.weighted_pageviews,
avg_page_relevance = 1.0,
activity_type_score = 100,
information_type_score = 100,
review_pageviews = s.review_pageviews + t.review_pageviews,
unique_pages = s.unique_pages + t.unique_pages,
unique_pubs = s.unique_pubs + t.unique_pubs,
unique_devices = s.unique_devices + t.unique_devices,
unique_ips = s.unique_ips + t.unique_ips
when not matched then insert
(normalized_company_domain,
parent_category,
category,
topic,
normalized_country_code,
normalized_region_code,
normalized_city_name,
normalized_zip,
date,
pageviews,
weighted_pageviews,
avg_page_relevance,
activity_type_score,
information_type_score,
review_pageviews,
unique_pages,
unique_pubs,
unique_devices,
unique_ips
)
 values
(s.normalized_company_domain,
s.parent_category,
s.category,
s.topic,
s.normalized_country_code,
s.normalized_region_code,
s.normalized_city_name,
s.normalized_zip,
s.date,
s.pageviews,
s.weighted_pageviews,
s.avg_page_relevance,
s.activity_type_score,
s.information_type_score,
s.review_pageviews,
s.unique_pages,
s.unique_pubs,
s.unique_devices,
s.unique_ips
)
;"""]

#-----Delete from caches-----
delete_from_company_activity_cache=[f"""delete from {PIXEL_DATABASE}.activity.company_activity_cache a
using {PIXEL_DATABASE}.activity.enriched_company_activity_cache b
where a.page_url=b.page_url
and a.NORMALIZED_COMPANY_DOMAIN=b.NORMALIZED_COMPANY_DOMAIN
and a.date=b.date
and equal_null(a.NORMALIZED_COUNTRY_CODE,b.NORMALIZED_COUNTRY_CODE)=true
and equal_null(a.normalized_region_code,b.normalized_region_code)=true
and equal_null(a.normalized_city_name,b.normalized_city_name)=true
and equal_null(a.normalized_zip,b.normalized_zip)= true;"""]

clear_enriched_company_activity_cache = [f"""
truncate table {PIXEL_DATABASE}."ACTIVITY"."ENRICHED_COMPANY_ACTIVITY_CACHE";
"""]
 
with dag:
  copy_query_exec = SnowflakeOperator(
    task_id= "load_from_s3",
    sql= copy_query,
    snowflake_conn_id= LOAD_CONNECTION ,
    )
  
  merge_into_user_activity_exec = SnowflakeOperator(
    task_id= "merge_into_user_activity",
    sql= merge_insert_user_activity,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

  clear_raw_data_cache_exec = SnowflakeOperator(
    task_id= "clear_raw_data_cache",
    sql= clear_raw_data_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

  merge_into_unique_urls_exec = SnowflakeOperator(
    task_id= "merge_into_unique_urls",
    sql= merge_insert_unique_urls,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  merge_into_WebScraper_input_cache_exec = SnowflakeOperator(
    task_id= "merge_into_WebScraper_input_cache",
    sql= merge_insert_WebScraper_input_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  merge_into_Digital_element_observation_exec = SnowflakeOperator(
    task_id= "merge_into_Digital_element_observation",
    sql= merge_insert_Digital_element_observation,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    
  merge_into_company_activity_cache_exec = SnowflakeOperator(
    task_id= "merge_into_company_activity_cache",
    sql= merge_insert_company_activity_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  merge_into_company_activity_exec = SnowflakeOperator(
    task_id= "merge_into_company_activity",
    sql= merge_insert_company_activity,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

  enriched_company_activity_cache_exec = SnowflakeOperator(
    task_id= "enriched_company_activity_cache",
    sql= enriched_company_activity_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  merge_into_prescoring_cache_exec = SnowflakeOperator(
    task_id= "merge_into_prescoring_cache",
    sql= merge_insert_prescoring_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  delete_from_company_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_company_activity_cache",
    sql= delete_from_company_activity_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  clear_enriched_company_activity_cache_exec = SnowflakeOperator(
    task_id= "clear_enriched_company_activity_cache",
    sql= clear_enriched_company_activity_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )
  

  copy_query_exec >> merge_into_user_activity_exec  >> clear_raw_data_cache_exec >> merge_into_unique_urls_exec >> merge_into_WebScraper_input_cache_exec
  merge_into_WebScraper_input_cache_exec >> merge_into_Digital_element_observation_exec >> merge_into_company_activity_cache_exec >> merge_into_company_activity_exec
  merge_into_company_activity_exec >> enriched_company_activity_cache_exec >>  merge_into_prescoring_cache_exec
  merge_into_prescoring_cache_exec >> delete_from_company_activity_cache_exec >> clear_enriched_company_activity_cache_exec >> end_success_exec
