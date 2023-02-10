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
    any_value(dev_datamart.public.domain_normalizer(ifnull(domain,''))) as publisher_domain_normalized,
    trim(user_id) as user_id,
    trim(ip_address) as user_ip,
    date(bid_time) as date,
    trim(upper(geo_country)) as bw_country_normalized,
    trim(upper(split_part(geo_region,'/',-1))) as bw_region_normalized,
    '' as bw_city_normalized, -- TODO: replace with join between city code and city name,
    geo_zip as bw_zip_normalized,
    count(*) as pageviews
from "BEESWAX_EXTERNAL_SHARE"."ANTENNA"."SHARED_DEMANDSCIENCE_AUCTIONS_VIEW"
where date > $raw_bidstream_watermark
    and date < $raw_bidstream_max_date
    and page_url is not null 
    and len(page_url) > 0
    and not (user_ip is null and user_id is null)
    and date is not null
group by 1,3,4,5,6,7,8,9;
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
#IP2LOC
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
     and len(user_ip)>0;
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

#IP2DOMAIN
de_ip_domain_obs_query = ["""
merge into dev_datamart.entity_mappings.ip_to_company_domain_observations t
using (
    select distinct
        bw.user_ip as ip,
        de.normalized_company_domain,
        de.date_updated
    from dev_bidstream.activity.user_activity_cache bw
    join dev_digital_element.mappings.ip_range_mappings_filtered de
    on dev_digital_element.public.ip_to_number(bw.user_ip) between de.ip_range_start_numeric and de.ip_range_end_numeric
    where ip is not null and len(ip) > 0
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

datamart_ip_domain_mappings_update_query = ["""
create or replace table "DEV_DATAMART"."ENTITY_MAPPINGS"."IP_TO_COMPANY_DOMAIN" as
with most_recent as(
    select distinct
    ip,
    first_value(normalized_company_domain) over (partition by ip, source order by date desc) as latest_domain,
    source,
    source_confidence
from "DEV_DATAMART"."ENTITY_MAPPINGS"."IP_TO_COMPANY_DOMAIN_OBSERVATIONS"
),
rolled_obs as (
    select
    ip,
    latest_domain,
    array_agg(distinct source) as sources,
    iff(array_size(sources)>1, 1, sum((case source
        when 'DIGITAL ELEMENT' then 0.3
        when 'IP FLOW' then 0.5
        when 'FIVE BY FIVE' then 0.9
    else 0.0 end)*ifnull(source_confidence, 1))) as score
  from most_recent
  group by 1,2
)
select distinct
    ip,
    first_value(latest_domain) over (partition by ip order by score desc) as normalized_company_domain,
    first_value(score) over (partition by ip order by score desc) as score
from rolled_obs;
"""]

#--- DIG Updates ----- #
bitoid_to_ip_cache_query = ["""
merge into dev_bidstream.entity_relationships.bitoid_to_ip_observations_cache t
using (
    select distinct
    user_id,
    user_ip,
    date
from "DEV_BIDSTREAM"."ACTIVITY"."USER_ACTIVITY_CACHE"
where (len(user_ip) > 1) and (user_ip is not null) and (not endswith(user_ip,'.0'))
and (len(user_id) > 1) and (user_id is not null)  
    ) s
on t.ip = s.user_ip
and t.bitoid = s.user_id
and t.date = s.date
when not matched then insert
(bitoid, ip, date)
values
(s.user_id, s.user_ip, s.date);

"""]

bitoid_to_ip_cache_to_obs_query = ["""
merge into "DEV_DATAMART"."ENTITY_MAPPINGS"."BITOID_TO_IP_OBSERVATIONS" t
using dev_bidstream.entity_relationships.bitoid_to_ip_observations_cache s
on t.ip = s.ip
and t.bitoid = s.bitoid
and t.date = s.date
and t.source = 'BEESWAX'
when not matched then insert
(bitoid, ip, source, date)
values
(s.bitoid, s.ip, 'BEESWAX', s.date);

"""]

bitoid_to_domain_obs_query = ["""
merge into "DEV_DATAMART"."ENTITY_MAPPINGS"."BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS" t
using (
    select
    bitoid,
    normalized_company_domain,
    date,
    any_value('BEESWAX-VIA-IP') as source,
    any_value(score) as source_confidence -- for now just take the ip -> domain mapping as the confidence
from dev_bidstream.entity_relationships.bitoid_to_ip_observations_cache a
join "DEV_DATAMART"."ENTITY_MAPPINGS"."IP_TO_COMPANY_DOMAIN" b
on a.ip = b.ip
where len(normalized_company_domain) > 1 and normalized_company_domain is not null
group by 1,2,3
) s
on s.bitoid = t.bitoid
and s.normalized_company_domain = t.normalized_company_domain
and s.source = t.source
and s.date = t.date
when not matched then insert
(bitoid, normalized_company_domain, source, source_confidence, date)
values
(s.bitoid, s.normalized_company_domain, s.source, s.source_confidence, s.date);
"""]

bitoid_to_domain_mappings_query = ["""
create or replace table "DEV_DATAMART"."ENTITY_MAPPINGS"."BITOID_TO_COMPANY_DOMAIN" as
with bitoid_totals as (
    select
    bitoid,
    count(*) as bitoid_total
from DEV_DATAMART.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS
where not (normalized_company_domain in (select distinct domain from "DEV_DATAMART"."FILTERS_SUPPRESSIONS"."KNOWN_ISP_DOMAINS"))
group by 1
),
rolled_obs as (
    select
    a.bitoid,
    normalized_company_domain,
    array_agg(distinct source) as sources,
    iff(array_size(sources)>1, 1, max(source_confidence)) as source_score,
    count(*) as frequency,
    any_value(bitoid_total) as total,
    source_score*(frequency/total) as score
  from DEV_DATAMART.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS a
  join bitoid_totals b
  on a.bitoid = b.bitoid
  where normalized_company_domain != 'Shared'
  and not (normalized_company_domain in (select distinct domain from "DEV_DATAMART"."FILTERS_SUPPRESSIONS"."KNOWN_ISP_DOMAINS"))
  group by 1,2
)
select distinct
    bitoid,
    first_value(normalized_company_domain) over (partition by bitoid order by score desc) as normalized_company_domain,
    first_value(score) over (partition by bitoid order by score desc) as score
from rolled_obs;

"""]

clear_bitoid_to_ip_cache_query = ["""
truncate table dev_bidstream.entity_relationships.bitoid_to_ip_observations_cache;
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
-- company activity cache leveraging DIG
-- assumption: user_ip or user_id might be null, but not both. user_ip might end in .0
-- first join everything together
with joined_activity as (
select
    page_url,
    publisher_domain_normalized,
    d.normalized_company_domain as ip_domain,-- might be a known ISP
    d.score as ip_domain_conf,
    dig.normalized_company_domain as dig_domain, -- will never be a known ISP (we remove them in DIG)
    dig.score as dig_domain_conf,
    iff(dig_domain in (select distinct domain from "DEV_DATAMART"."FILTERS_SUPPRESSIONS"."KNOWN_ISP_DOMAINS"), true, false) as ip_domain_is_isp,
    a.date,
    a.bw_country_normalized as bw_country,
    a.bw_region_normalized as bw_region,
    g.normalized_city_name as bw_city, -- change this as soon as we can get the direct city from BW
    a.bw_zip_normalized::varchar as bw_zip,
    iff((bw_country is not null) and (bw_region is not null) and (bw_city is not null) and (bw_zip is not null), true, false) as bw_loc_not_null,
    l.normalized_country_code as ip_country, 
    l.normalized_region_code as ip_region, 
    l.normalized_city_name as ip_city,
    l.normalized_zip::varchar as ip_zip,
    iff((ip_country is not null) and (ip_region is not null) and (ip_city is not null) and (ip_zip is not null), true, false) as ip_loc_not_null,
    a.pageviews,
    a.user_id,
    a.user_ip
from dev_bidstream.activity.user_activity_cache a -- CHANGE BACK TO CACHE
left join dev_datamart.entity_mappings.ip_to_company_domain d
on a.user_ip = d.ip
left join dev_datamart.entity_mappings.bitoid_to_company_domain dig
on a.user_id = dig.bitoid
left join dev_datamart.entity_mappings.ip_to_location l
on a.user_ip = l.ip
left join dev_datamart.geographics.unique_geos g
on bw_country = g.normalized_country_code and bw_region = g.normalized_region_code and bw_zip = g.normalized_zip
where a.date > $user_activity_watermark and a.date <= $user_activity_max_date
),
-- now decide what to use
selected_values as (
select
    page_url,
    publisher_domain_normalized,
    -- deal with domain
    case
        when user_ip is null or len(user_ip) < 2 then dig_domain
        when (endswith(user_ip, '.0') or ip_domain_is_isp or ip_domain = 'Shared' or len(ip_domain) < 2) and not (dig_domain is null) then dig_domain
        else ip_domain
    end as chosen_domain,
    case
        when chosen_domain = dig_domain then dig_domain_conf
        when chosen_domain = ip_domain then ip_domain_conf
    else 0.0 end as domain_conf,
    date,
    -- deal with location
    iff(ip_loc_not_null, ip_country, bw_country) as chosen_country,
    iff(ip_loc_not_null, ip_region, bw_region) as chosen_region,
    iff(ip_loc_not_null, ip_city, bw_city) as chosen_city,
    iff(ip_loc_not_null, ip_zip, bw_zip) as chosen_zip,
    -- remaining fields
    pageviews,
    user_ip,
    user_id
from joined_activity
 )   
   select
   page_url,
   any_value(publisher_domain_normalized) as publisher_domain_normalized,
   chosen_domain as normalized_company_domain,
   date,
   chosen_country as normalized_country_code,
   chosen_region as normalized_region_code,
   chosen_city as normalized_city_name,
   iff(is_integer(try_to_number(chosen_zip)) and normalized_country_code = 'USA' and len(chosen_zip) = 4, '0'||chosen_zip,chosen_zip) as normalized_zip,
   sum(pageviews) as pageviews,
   sum(domain_conf*pageviews) as weighted_pageviews,
   count(distinct user_id) as unique_devices,
   count(distinct user_ip) as unique_ips
   from selected_values
   where normalized_company_domain is not null and normalized_company_domain != 'Shared' and len(normalized_company_domain) > 1
   and normalized_country_code is not null
   and normalized_region_code is not null
   and normalized_city_name is not null
   and normalized_zip is not null
   group by 1,3,4,5,6,7,8
;
"""]

company_activity_cache_to_cumulative_query = [
"""
merge into dev_bidstream.activity.company_activity t
using dev_bidstream.activity.company_activity_cache s
on  s.page_url = t.page_url
and s.normalized_company_domain = t.normalized_company_domain
and s.date = t.date
and s.normalized_country_code = t.normalized_country_code 
and s.normalized_region_code = t.normalized_region_code 
and s.normalized_city_name = t.normalized_city_name
and s.normalized_zip = t.normalized_zip
when not matched then insert
(page_url,
publisher_domain_normalized,
normalized_company_domain,
date,
normalized_country_code, 
normalized_region_code, 
normalized_city_name,
normalized_zip,
pageviews,
weighted_pageviews,
unique_devices,
unique_ips)
values
(s.page_url,
s.publisher_domain_normalized,
s.normalized_company_domain,
s.date,
s.normalized_country_code, 
s.normalized_region_code, 
s.normalized_city_name,
s.normalized_zip,
s.pageviews,
s.weighted_pageviews,
s.unique_devices,
s.unique_ips)
;
"""]

delete_from_user_activity_cache_query = ["""
delete from dev_bidstream.activity.user_activity_cache
where date in (select distinct date from dev_bidstream.activity.company_activity_cache);
"""]

# --- PRESCORING ---- #
prescoring_cache_query = ["""
merge into dev_bidstream.activity.prescoring_cache t
using (
    select
        activity.*,
        taxo.intent_topics,
        context.context as context_output,
        title.topics as title_output
    from "DEV_BIDSTREAM"."ACTIVITY"."COMPANY_ACTIVITY_CACHE" activity
    join "DEV_AIML"."TAXONOMY_CLASSIFIER"."OUTPUT" taxo
    on activity.page_url = taxo.page_url
    join "DEV_AIML"."CONTEXT_CLASSIFIER"."OUTPUT" context
    on activity.page_url = context.page_url
    join "DEV_AIML"."TITLE_CLASSIFIER"."OUTPUT" title
    on activity.page_url = title.page_url) s
on s.page_url = t.page_url
and s.normalized_company_domain = t.normalized_company_domain
and s.date = t.date
and s.normalized_country_code = t.normalized_country_code
and s.normalized_region_code = t.normalized_region_code
and s.normalized_city_name = t.normalized_city_name
and s.normalized_zip = t.normalized_zip
when not matched then insert
(page_url, 
 publisher_domain_normalized,
 normalized_company_domain,
 date,
 normalized_country_code,
 normalized_region_code,
 normalized_city_name,
 normalized_zip,
 pageviews,
 weighted_pageviews,
 unique_devices,
 unique_ips,
 intent_topics,
 context_output,
 title_output)
 values
 (s.page_url, 
 s.publisher_domain_normalized,
 s.normalized_company_domain,
 s.date,
 s.normalized_country_code,
 s.normalized_region_code,
 s.normalized_city_name,
 s.normalized_zip,
 s.pageviews,
 s.weighted_pageviews,
 s.unique_devices,
 s.unique_ips,
 s.intent_topics,
 s.context_output,
 s.title_output)
 when matched then update set
 publisher_domain_normalized = s.publisher_domain_normalized,
 pageviews = s.pageviews,
 unique_devices = s.unique_devices,
 unique_ips = s.unique_ips,
 intent_topics = s.intent_topics,
 context_output = s.context_output,
 title_output = s.title_output;
"""]



delete_from_company_activity_cache_query = ["""
delete from "DEV_BIDSTREAM"."ACTIVITY"."COMPANY_ACTIVITY_CACHE" a
 using "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING_CACHE" b
 where a.page_url = b.page_url
 and a.normalized_company_domain = b.normalized_company_domain
 and a.date = b.date
 and a.normalized_country_code = b.normalized_country_code
 and a.normalized_region_code = b.normalized_region_code
 and a.normalized_city_name = b.normalized_city_name
 and a.normalized_zip = b.normalized_zip;
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
""",
"""
Delete from "DEV_BIDSTREAM"."CONTENT"."UNIQUE_URLS_CACHE" 
where page_url in (select distinct page_url from dev_aiml.web_scraper.results);
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
and not (publisher_domain_normalized in 
  (select distinct publisher_domain_normalized from DEV_DATAMART.FILTERS_SUPPRESSIONS.HIGH_VOLUME_BAD_PUBS_SUPPRESS))
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

    de_ip_domain_obs_exec = SnowflakeOperator(
    task_id= "de_ip_domain_obs_update",
    sql= de_ip_domain_obs_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    datamart_ip_domain_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_domain_update",
    sql= datamart_ip_domain_mappings_update_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    #-----DIG LOGIC ----#
    bitoid_to_ip_cache_exec = SnowflakeOperator(
    task_id= "bitoid_to_ip_cache",
    sql= bitoid_to_ip_cache_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    bitoid_to_domain_obs_exec = SnowflakeOperator(
    task_id= "bitoid_to_domain_obs",
    sql= bitoid_to_domain_obs_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    bitoid_to_domain_map_exec = SnowflakeOperator(
    task_id= "bitoid_to_domain_map",
    sql= bitoid_to_domain_mappings_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    bitoid_to_ip_cache_to_obs_exec = SnowflakeOperator(
    task_id= "bitoid_to_ip_cache_to_obs",
    sql= bitoid_to_ip_cache_to_obs_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    clear_bitoid_to_ip_cache_exec = SnowflakeOperator(
    task_id= "clear_bitoid_to_ip_cache",
    sql= clear_bitoid_to_ip_cache_query,
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

    # --- PRESCORING --- #
    prescoring_cache_exec = SnowflakeOperator(
    task_id= "prescoring_cache",
    sql= prescoring_cache_query,
    snowflake_conn_id= SNOWFLAKE_CONN,
    )

    

    delete_from_company_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_company_activity_cache",
    sql= delete_from_company_activity_cache_query,
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
    user_activity_exec >> bidstream_ip_mappings_exec >> datamart_ip_loc_mappings_exec >> de_ip_domain_obs_exec >> datamart_ip_domain_mappings_exec
    datamart_ip_domain_mappings_exec >> bitoid_to_ip_cache_exec >> bitoid_to_domain_obs_exec >> bitoid_to_domain_map_exec >> company_activity_exec
    company_activity_exec >> unique_urls_exec >> web_scraper_input_cache_exec
    company_activity_exec >> prescoring_cache_exec
    
    #cache -> cumulative map
    user_activity_exec >> user_activity_cache_to_cumulative_exec
    bidstream_ip_mappings_exec >> bidstream_ip_mappings_cache_to_cumulative_exec
    bitoid_to_ip_cache_exec >> bitoid_to_ip_cache_to_obs_exec
    company_activity_exec >> company_activity_cache_to_cumulative_exec
    unique_urls_exec >> unique_urls_cache_to_cumulative_exec

    #deletion logic
    [user_activity_cache_to_cumulative_exec, company_activity_exec] >> delete_from_user_activity_cache_exec
    [bitoid_to_ip_cache_to_obs_exec, bitoid_to_domain_obs_exec] >> clear_bitoid_to_ip_cache_exec
    [unique_urls_cache_to_cumulative_exec, web_scraper_input_cache_exec] >> delete_from_unique_urls_cache_exec
    [company_activity_cache_to_cumulative_exec, prescoring_cache_exec, unique_urls_exec] >> delete_from_company_activity_cache_exec

    #end success logic

    [delete_from_company_activity_cache_exec, bidstream_ip_mappings_cache_to_cumulative_exec, 
    delete_from_user_activity_cache_exec, delete_from_unique_urls_cache_exec, clear_bitoid_to_ip_cache_exec] >> end_success_exec