import logging
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# ---Variable definition--- #

dt = datetime.now()
date_time = dt.strftime("%m%d%Y%H:%M:%S")

# --- Function Definitions --- #
def end_success():
  logger.info("DAG Ended Successfully.")

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Beeswax_Data_Processing"
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Beeswax_Data_Processing"
        ,subject="DAG Success"
        ,message=f"Beeswax Data Processing has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)
    
SNOWFLAKE_CONN = "Airflow-Dev_Transform-connection"

WBI_SIZE_TRANSFORMS = '30'
WBI_SIZE_SCRAPER = '3'
WBI_SIZE_ML = '3'


dag = DAG('Beeswax_Channel', start_date = datetime(2022, 12, 7), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'depends_on_past' : False,'retries' : 0,'on_failure_callback': on_failure_callback,'on_success_callback': None})

# ---- USER ACTIVITY ---- #
user_activity_query = [
"""
set raw_bidstream_watermark = (select ifnull(max(date), date('2022-10-13')) from dev_bidstream.activity.user_activity);
""",

"""
set raw_bidstream_max_date = 
(select 
least(
    (select max(date(bid_time)) from "BEESWAX_EXTERNAL_SHARE"."ANTENNA"."SHARED_DEMANDSCIENCE_AUCTIONS_VIEW"),
    dateadd(day, {}, $raw_bidstream_watermark)));
""".format(WBI_SIZE_TRANSFORMS),

"""
delete from dev_bidstream.activity.user_activity_cache
where date > $raw_bidstream_watermark
and date < $raw_bidstream_max_date;
""",
"""
insert into dev_bidstream.activity.user_activity_cache
select
    trim(page_url) as page_url,
    dev_datamart.public.domain_normalizer(ifnull(domain,'')) as publisher_domain_normalized,
    trim(user_id) as user_id,
    trim(ip_address) as user_ip,
    date(bid_time) as date,
    trim(upper(geo_country)) as bw_country_normalized,
    trim(upper(split_part(geo_region,'/',-1))) as bw_region_normalized,
    '' as bw_city_normalized, -- TODO: replace with join between city code and city name,
    try_to_number(geo_zip) as bw_zip_normalized,
    count(*) as pageviews
from "BEESWAX_EXTERNAL_SHARE"."ANTENNA"."SHARED_DEMANDSCIENCE_AUCTIONS_VIEW"
where date > $raw_bidstream_watermark
    and date < $raw_bidstream_max_date
    and page_url is not null 
    and len(page_url) > 0
    and not ((user_ip is null or endswith(user_ip, '.0')) and user_id is null)
    and date is not null
group by 1,2,3,4,5,6,7,8,9;
"""]

user_activity_cache_to_cumulative_query = ["""
delete from dev_bidstream.activity.user_activity
where date in (select distinct date from dev_bidstream.activity.user_activity_cache);
""",
"""
insert into dev_bidstream.activity.user_activity
select * from dev_bidstream.activity.user_activity_cache;
"""]

# ---- IP MAPPINGS ---- #
bidstream_ip_mappings_query = ["""
create or replace table dev_bidstream.entity_relationships.ip_to_location_cache as
select distinct
    user_ip as ip,
    first_value(date) over (partition by user_ip order by date desc) as date_updated,
    first_value(normalized_country_code) over (partition by user_ip order by date desc, location_key desc) as normalized_country_code,
    first_value(normalized_region_code) over (partition by user_ip order by date desc, location_key desc) as normalized_region_code,
    first_value(normalized_city_name) over (partition by user_ip order by date desc, location_key desc) as normalized_city_name, 
    first_value(normalized_zip) over (partition by user_ip order by date desc, location_key desc) as normalized_zip
    from (
        select distinct
            user_ip,
            date,
            bw_country_normalized as normalized_country_code,
            bw_region_normalized as normalized_region_code,
            bw_city_normalized as normalized_city_name, --TODO: add a join to get the actual city name
            bw_zip_normalized as normalized_zip,
            normalized_country_code || normalized_region_code || normalized_city_name || normalized_zip as location_key
        from "DEV_BIDSTREAM"."ACTIVITY"."USER_ACTIVITY_CACHE")
     where user_ip is not null
     and len(user_ip)>0
     and not endswith(user_ip, '.0');
"""]

bidstream_ip_mappings_cache_to_cumulative_query = ["""
merge into dev_bidstream.entity_relationships.ip_to_location t
using (dev_bidstream.entity_relationships.ip_to_location_cache) s
on t.ip = s.ip
when not matched then insert
(ip, date_updated, normalized_country_code, normalized_region_code, normalized_city_name, normalized_zip)
values
(s.ip, s.date_updated, s.normalized_country_code, s.normalized_region_code, s.normalized_city_name, s.normalized_zip)
when matched then update set
date_updated = s.date_updated,
normalized_country_code = s.normalized_country_code,
normalized_region_code = s.normalized_region_code,
normalized_city_name = s.normalized_city_name,
normalized_zip = s.normalized_zip;
"""]

datamart_ip_location_mappings_update_query = ["""
set de_confidence_threshold = 0; -- eventually change this to something higher
""",
"""
merge into dev_datamart.entity_mappings.ip_to_location t
using (
    select 
        bw.ip,
        iff(country_confidence >= $de_confidence_threshold
           and region_confidence >= $de_confidence_threshold
           and city_confidence >= $de_confidence_threshold,
        'DE','BW') as loc_source,
        iff(loc_source = 'DE',de.date_updated,bw.date_updated) as date_updated,
        iff(loc_source = 'DE',de.normalized_country_code,bw.normalized_country_code) as normalized_country_code,
        iff(loc_source = 'DE',de.normalized_region_code,bw.normalized_region_code) as normalized_region_code,
        iff(loc_source = 'DE',de.normalized_city_name,bw.normalized_city_name) as normalized_city_name,
        iff(loc_source = 'DE',de.normalized_zip,bw.normalized_zip) as normalized_zip
    from dev_bidstream.entity_relationships.ip_to_location_cache bw
    join dev_digital_element.mappings.ip_range_mappings_filtered de
    on dev_digital_element.public.ip_to_number(bw.ip) between de.ip_range_start_numeric and de.ip_range_end_numeric) s
on s.ip = t.ip
when matched then update set
date_updated = s.date_updated,
normalized_country_code = s.normalized_country_code,
normalized_region_code = s.normalized_region_code,
normalized_city_name = s.normalized_city_name,
normalized_zip = s.normalized_zip
when not matched then insert
(ip, date_updated, normalized_country_code, normalized_region_code, normalized_city_name, normalized_zip)
values
(s.ip, s.date_updated, s.normalized_country_code, s.normalized_region_code, s.normalized_city_name, s.normalized_zip);
"""]

datamart_ip_domain_mappings_update_query = ["""
merge into dev_datamart.entity_mappings.ip_to_company_domain t
using (
    select
        bw.ip,
        de.date_updated,
        de.normalized_company_domain
    from dev_bidstream.entity_relationships.ip_to_location_cache bw
    join dev_digital_element.mappings.ip_range_mappings_filtered de
    on dev_digital_element.public.ip_to_number(bw.ip) between de.ip_range_start_numeric and de.ip_range_end_numeric) s
on s.ip = t.ip
when not matched then insert
(ip, date_updated, normalized_company_domain)
values
(s.ip, s.date_updated, s.normalized_company_domain)
when matched then update set
date_updated = s.date_updated,
normalized_company_domain = s.normalized_company_domain;
"""]

# ---- COMPANY ACTIVITY ---- #
company_activity_query = ["""
set user_activity_watermark = (select ifnull(max(date), '2022-10-13') from dev_bidstream.activity.company_activity);
""",
"""
set user_activity_max_date = (select dateadd(day, {}, $user_activity_watermark));
""".format(WBI_SIZE_TRANSFORMS),
"""
delete from dev_bidstream.activity.company_activity_cache
where date > $user_activity_watermark and date <= $user_activity_max_date;
""",
"""
insert into dev_bidstream.activity.company_activity_cache
select
    a.page_url,
    a.publisher_domain_normalized,
    d.normalized_company_domain,
    a.date,
    l.normalized_country_code, 
    l.normalized_region_code, 
    l.normalized_city_name,
    l.normalized_zip,
    sum(a.pageviews) as pageviews,
    count(distinct user_id) as unique_devices,
    count(distinct user_ip) as unique_ips
from dev_bidstream.activity.user_activity_cache a
join dev_datamart.entity_mappings.ip_to_company_domain d
on a.user_ip = d.ip
join dev_datamart.entity_mappings.ip_to_location l
on a.user_ip = l.ip
where a.date > $user_activity_watermark and a.date <= $user_activity_max_date
group by 1,2,3,4,5,6,7,8;
"""]

company_activity_cache_to_cumulative_query = ["""
delete from dev_bidstream.activity.company_activity
where date in (select distinct date from dev_bidstream.activity.company_activity_cache);
""",
"""
insert into dev_bidstream.activity.company_activity
select * from dev_bidstream.activity.company_activity_cache;
"""]

delete_from_user_activity_cache_query = ["""
delete from dev_bidstream.activity.user_activity_cache
where date in (select distinct date from dev_bidstream.activity.company_activity_cache);
"""]

# ---- UNIQUE URLS ---- #
unique_urls_query = ["""
set company_activity_watermark = (select ifnull(max(last_seen), '2022-10-13') from dev_bidstream.content.unique_urls);
""",
"""
set company_activity_max_date = (select dateadd(day, {}, $company_activity_watermark));
""".format(WBI_SIZE_TRANSFORMS),
"""
merge into dev_bidstream.content.unique_urls_cache t
using (
select
    page_url,
    hash(page_url) as page_url_hash,
    any_value(publisher_domain_normalized) as publisher_domain_normalized,
    min(date) as first_seen,
    max(date) as last_seen,
    count(*) as frequency
from dev_bidstream.activity.company_activity_cache
where date > $company_activity_watermark and date <= $company_activity_max_date
group by 1,2) s
on t.page_url = s.page_url
when not matched then insert
(page_url, page_url_hash, publisher_domain_normalized, first_seen, last_seen, frequency)
values
(s.page_url, s.page_url_hash, s.publisher_domain_normalized, s.first_seen, s.last_seen, s.frequency)
when matched then update set
publisher_domain_normalized = s.publisher_domain_normalized,
last_seen = s.last_seen,
frequency = iff(s.last_seen = t.last_seen, t.frequency, s.frequency + t.frequency);
"""]

unique_urls_cache_to_cumulative_query = ["""
merge into dev_bidstream.content.unique_urls t
using dev_bidstream.content.unique_urls_cache s
on t.page_url = s.page_url
when not matched then insert
(page_url, page_url_hash, publisher_domain_normalized, first_seen, last_seen, frequency)
values
(s.page_url, s.page_url_hash, s.publisher_domain_normalized, s.first_seen, s.last_seen, s.frequency)
when matched then update set
publisher_domain_normalized = s.publisher_domain_normalized,
last_seen = s.last_seen,
frequency = iff(s.last_seen = t.last_seen, t.frequency, s.frequency + t.frequency);
"""]

delete_from_unique_urls_cache_query = ["""
Delete from "DEV_BIDSTREAM"."CONTENT"."UNIQUE_URLS_CACHE" 
where page_url in (select distinct page_url from dev_aiml.web_scraper.input_cache);
"""]

# ---- WEB Scraper Data load ---- #

web_scraper_input_cache_query = [
"""merge into "DEV_AIML"."WEB_SCRAPER"."INPUT_CACHE" t
using (
select distinct
    a.page_url,
  a.last_seen
from "DEV_BIDSTREAM"."CONTENT"."UNIQUE_URLS_CACHE" a
left join dev_aiml.web_scraper.results b
on a.page_url = b.page_url
where last_scraped is null
and publisher_domain_normalized not in 
  (select distinct publisher_domain_normalized from DEV_DATAMART.FILTERS_SUPPRESSIONS.HIGH_VOLUME_BAD_PUBS_SUPPRESS)
order by a.last_seen desc limit 2000000) s
on t.page_url = s.page_url
when not matched then insert
(page_url, date_inserted)
values
(s.page_url, current_date)
when matched then update set
date_inserted = current_date;
;
"""]



with dag:
    # --- USER ACTIVITY --- #
    user_activity_exec = SnowflakeOperator(
    task_id= "raw_bidstream_to_user_activity_cache",
    sql= user_activity_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    user_activity_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "user_activity_cache_to_cumulative",
    sql= user_activity_cache_to_cumulative_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    # --- IP MAPPINGS --- #
    bidstream_ip_mappings_exec = SnowflakeOperator(
    task_id= "bidstream_ip_mappings_cache",
    sql= bidstream_ip_mappings_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    bidstream_ip_mappings_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "bidstream_ip_mappings_cache_to_cumulative",
    sql= bidstream_ip_mappings_cache_to_cumulative_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    datamart_ip_loc_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_loc_update",
    sql= datamart_ip_location_mappings_update_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    datamart_ip_domain_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_domain_update",
    sql= datamart_ip_domain_mappings_update_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    # --- COMPANY ACTIVITY --- #
    company_activity_exec = SnowflakeOperator(
    task_id= "company_activity_cache",
    sql= company_activity_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    company_activity_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "company_activity_cache_to_cumulative",
    sql= company_activity_cache_to_cumulative_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    delete_from_user_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_user_activity_cache",
    sql= delete_from_user_activity_cache_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    # --- UNIQUE URLS --- #
    unique_urls_exec = SnowflakeOperator(
    task_id= "unique_urls_cache",
    sql= unique_urls_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    unique_urls_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "unique_urls_cache_to_cumulative",
    sql= unique_urls_cache_to_cumulative_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )



    # --- WEB SCRAPER --- #
    web_scraper_input_cache_exec = SnowflakeOperator(
    task_id= "web_scraper_input__cache",
    sql= web_scraper_input_cache_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )
    
    delete_from_unique_urls_cache_exec = SnowflakeOperator(
    task_id= "delete_from_unique_urls_cache",
    sql= delete_from_unique_urls_cache_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    #----OTHER STEPS----
    end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )
    
    #main logic
    user_activity_exec >> bidstream_ip_mappings_exec >> datamart_ip_loc_mappings_exec >> datamart_ip_domain_mappings_exec >> company_activity_exec >> unique_urls_exec >> web_scraper_input_cache_exec
    
    #cache -> cumulative map
    user_activity_exec >> user_activity_cache_to_cumulative_exec
    bidstream_ip_mappings_exec >> bidstream_ip_mappings_cache_to_cumulative_exec
    company_activity_exec >> company_activity_cache_to_cumulative_exec
    unique_urls_exec >> unique_urls_cache_to_cumulative_exec

    #deletion logic
    [user_activity_cache_to_cumulative_exec, company_activity_exec] >> delete_from_user_activity_cache_exec

    [unique_urls_cache_to_cumulative_exec, web_scraper_input_cache_exec] >> delete_from_unique_urls_cache_exec

    [company_activity_cache_to_cumulative_exec, bidstream_ip_mappings_cache_to_cumulative_exec, 
    delete_from_user_activity_cache_exec, delete_from_unique_urls_cache_exec] >> end_success_exec