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
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
BIDSTREAM_DATABASE = Variable.get("BIDSTREAM_DATABASE")
DATAMART_DATABASE = Variable.get("DATAMART_DATABASE")
DE_DATABASE = Variable.get("DE_DATABASE")
AIML_DATABASE = Variable.get("AIML_DATABASE")

# ---Variable definition--- #
WBI_SIZE_TRANSFORMS = '180'
dt = datetime.now()
date_time = dt.strftime("%m%d%Y%H:%M:%S")

# --- Function Definitions --- #
def end_success():
  logger.info("DAG Ended Successfully.")

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="BEESWAX DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="BEESWAX DAG SUCCESS"
        ,message=f"Beeswax Data Processing has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)


dag = DAG('Beeswax_Channel', start_date = datetime(2022, 12, 7), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'depends_on_past' : False,'retries' : 0,'on_failure_callback': on_failure_callback,'on_success_callback': None})


# ---- USER ACTIVITY ---- #
user_activity_query = [
"""
set raw_bidstream_watermark = (SELECT CONVERT_TIMEZONE('America/Atikokan', CURRENT_TIMESTAMP()));
""",
f"""
insert into {BIDSTREAM_DATABASE}.activity.user_activity_cache
select
    trim(page_url) as page_url,
    any_value({DATAMART_DATABASE}.public.domain_normalizer(ifnull(domain,''))) as publisher_domain_normalized,
    trim(user_id) as user_id,
    trim(ip_address) as user_ip,
    date(bid_time) as date,
    trim(upper(geo_country)) as bw_country_normalized,
    trim(upper(split_part(geo_region,'/',-1))) as bw_region_normalized,
    '' as bw_city_normalized, -- TODO: replace with join between city code and city name,
    geo_zip as bw_zip_normalized,
    count(*) as pageviews
from {BIDSTREAM_DATABASE}.RAW_DATA.AUCTION_LOGS
where bid_time <= $raw_bidstream_watermark
    and page_url is not null 
    and len(page_url) > 0
    and not (user_ip is null and user_id is null)
    and date is not null
group by 1,3,4,5,6,7,8,9;
""",
f"""delete from {BIDSTREAM_DATABASE}.RAW_DATA.AUCTION_LOGS where bid_time <= $raw_bidstream_watermark;""" ]

user_activity_cache_to_cumulative_query = [f"""
merge into {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY t
using {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY_CACHE s
on t.PAGE_URL=s.PAGE_URL
and t.DATE=s.DATE
and t.PUBLISHER_DOMAIN_NORMALIZED=s.PUBLISHER_DOMAIN_NORMALIZED
and t.PAGEVIEWS=s.PAGEVIEWS
and equal_null(t.USER_ID,s.USER_ID)=true
and equal_null(t.USER_IP,s.USER_IP)=true
when not matched then insert
(PAGE_URL,PUBLISHER_DOMAIN_NORMALIZED,USER_ID,USER_IP,DATE,BW_COUNTRY_NORMALIZED,BW_REGION_NORMALIZED,BW_CITY_NORMALIZED,BW_ZIP_NORMALIZED,PAGEVIEWS)
values
(s.PAGE_URL,s.PUBLISHER_DOMAIN_NORMALIZED,s.USER_ID,s.USER_IP,s.DATE,s.BW_COUNTRY_NORMALIZED,s.BW_REGION_NORMALIZED,s.BW_CITY_NORMALIZED,s.BW_ZIP_NORMALIZED,s.PAGEVIEWS);
"""]

# ---- IP MAPPINGS ---- #
#IP2LOC
bidstream_ip_mappings_query = [f"""
create or replace table {BIDSTREAM_DATABASE}.entity_relationships.ip_to_location_cache as
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
        from {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY_CACHE)
     where user_ip is not null
     and len(user_ip)>0;
"""]

bidstream_ip_mappings_cache_to_cumulative_query = [f"""
merge into {BIDSTREAM_DATABASE}.entity_relationships.ip_to_location t
using ({BIDSTREAM_DATABASE}.entity_relationships.ip_to_location_cache) s
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

datamart_ip_location_mappings_update_query = [f"""
set de_confidence_threshold = 0; -- eventually change this to something higher
""",
f"""
merge into {DATAMART_DATABASE}.entity_mappings.ip_to_location t
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
    from {BIDSTREAM_DATABASE}.entity_relationships.ip_to_location_cache bw
    join {DE_DATABASE}.mappings.ip_range_mappings_filtered de
    on {DE_DATABASE}.public.ip_to_number(bw.ip) between de.ip_range_start_numeric and de.ip_range_end_numeric) s
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
de_ip_domain_obs_query = [f"""
merge into {DATAMART_DATABASE}.entity_mappings.ip_to_company_domain_observations t
using (
    select distinct
        bw.user_ip as ip,
        de.normalized_company_domain,
        de.date_updated
    from {BIDSTREAM_DATABASE}.activity.user_activity_cache bw
    join {DE_DATABASE}.mappings.ip_range_mappings_filtered de
    on {DE_DATABASE}.public.ip_to_number(bw.user_ip) between de.ip_range_start_numeric and de.ip_range_end_numeric
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

datamart_ip_domain_mappings_update_query = [f"""
create or replace table {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN as
with scores as(
    select distinct
    ip,
    normalized_company_domain,
    source,
    source_confidence,
    case source
        when 'DIGITAL ELEMENT' then 0.3
        when 'IP FLOW' then 0.5
        when 'FIVE BY FIVE' then 0.9
        when 'LASTBOUNCE' then 0.7
    else 0.5 end as source_score,
    greatest(0.01,1.0-(0.01*(current_date - date))) as recency_score,
    ifnull(source_confidence, 1)*recency_score*source_score as score
from {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN_OBSERVATIONS
where normalized_company_domain != 'Shared'
and normalized_company_domain is not null
and len(normalized_company_domain)>1
),
map_scores as (
    select
    ip,
    normalized_company_domain,
    array_agg(distinct source) as sources,
    sum(score) as score
  from scores
  group by 1,2
),
score_totals as (
    select
    ip,
    sum(score) as score_total
    from map_scores
  group by 1
)
select distinct
    a.ip,
    normalized_company_domain,
    greatest(0.1,score/score_total) as score
from map_scores a
join score_totals b
on a.ip = b.ip;
"""]

datamart_ip_trio_domain_mappings_update_query = [f"""
create or replace table {DATAMART_DATABASE}."ENTITY_MAPPINGS"."IP_TRIO_MAPPINGS" as
with trio_domain_scores as (
    select
        SPLIT_PART(ip, '.', 1) || '.' || SPLIT_PART(ip, '.', 2) || '.' || SPLIT_PART(ip, '.', 3) as ip_trio,
        normalized_company_domain,
        sum(score) as score
  from {DATAMART_DATABASE}."ENTITY_MAPPINGS"."IP_TO_COMPANY_DOMAIN"
  where not (normalized_company_domain in (select distinct domain from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.KNOWN_ISP_DOMAINS))
  group by 1,2
),
trio_totals as (
    select
        ip_trio,
        sum(score) as score_total
  from trio_domain_scores
  group by 1
)
select distinct
a.ip_trio,
normalized_company_domain,
greatest(0.01,score/score_total) as score
from trio_domain_scores a
join trio_totals b
on a.ip_trio = b.ip_trio;
"""]

#--- DIG Updates ----- #
bitoid_to_ip_cache_query = [f"""
merge into {BIDSTREAM_DATABASE}.entity_relationships.bitoid_to_ip_observations_cache t
using (
    select distinct
    user_id,
    user_ip,
    date
from {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY_CACHE
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

bitoid_to_ip_cache_to_obs_query = [f"""
merge into {DATAMART_DATABASE}.ENTITY_MAPPINGS.BITOID_TO_IP_OBSERVATIONS t
using {BIDSTREAM_DATABASE}.entity_relationships.bitoid_to_ip_observations_cache s
on t.ip = s.ip
and t.bitoid = s.bitoid
and t.date = s.date
and t.source = 'BEESWAX'
when not matched then insert
(bitoid, ip, source, date)
values
(s.bitoid, s.ip, 'BEESWAX', s.date);

"""]

bitoid_to_domain_obs_query = [f"""
merge into {DATAMART_DATABASE}.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS t
using (
    select
    bitoid,
    normalized_company_domain,
    date,
    any_value('BEESWAX-VIA-IP') as source,
    any_value(score) as source_confidence -- for now just take the ip -> domain mapping as the confidence
from {BIDSTREAM_DATABASE}.entity_relationships.bitoid_to_ip_observations_cache a
join {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN b
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

bitoid_to_domain_mappings_query = [f"""
create or replace table {DATAMART_DATABASE}.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN as
with bitoid_totals as (
    select
    bitoid,
    count(*) as bitoid_total
from {DATAMART_DATABASE}.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS
where not (normalized_company_domain in (select distinct domain from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.KNOWN_ISP_DOMAINS))
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
  from {DATAMART_DATABASE}.ENTITY_MAPPINGS.BITOID_TO_COMPANY_DOMAIN_OBSERVATIONS a
  join bitoid_totals b
  on a.bitoid = b.bitoid
  where normalized_company_domain != 'Shared'
  and not (normalized_company_domain in (select distinct domain from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.KNOWN_ISP_DOMAINS))
  group by 1,2
)
select distinct
    bitoid,
    first_value(normalized_company_domain) over (partition by bitoid order by score desc) as normalized_company_domain,
    first_value(score) over (partition by bitoid order by score desc) as score
from rolled_obs;

"""]

clear_bitoid_to_ip_cache_query = [f"""
truncate table {BIDSTREAM_DATABASE}.entity_relationships.bitoid_to_ip_observations_cache;
"""]

# ---- COMPANY ACTIVITY ---- #
company_activity_query = [f"""
set user_activity_watermark = (select ifnull(max(date), '2022-10-13') from {BIDSTREAM_DATABASE}.activity.company_activity);
""",
"""
set user_activity_max_date = (select dateadd(day, {}, $user_activity_watermark));
""".format(WBI_SIZE_TRANSFORMS),
f"""
-- 0. figure out what domains can be resolved via IP
merge into {BIDSTREAM_DATABASE}."ENTITY_RELATIONSHIPS"."IP_RESOLVABLE_DOMAINS" t
using (
    select distinct
  normalized_company_domain,
  date
  from {BIDSTREAM_DATABASE}.activity.user_activity_cache a
  join {DATAMART_DATABASE}.entity_mappings.ip_to_company_domain b
  on a.user_ip = b.ip
  where a.date > $user_activity_watermark and a.date <= $user_activity_max_date
  and (not endswith(ip,'.0')) 
  and (not (normalized_company_domain in (select distinct domain from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.KNOWN_ISP_DOMAINS)))
  and normalized_company_domain is not null
  and len(normalized_company_domain)>1 and normalized_company_domain!='shared'
) s
on s.normalized_company_domain = t.normalized_company_domain
and s.date = t.date
when not matched then insert
(normalized_company_domain, date)
values
(s.normalized_company_domain, s.date);
""",
f"""
delete from {BIDSTREAM_DATABASE}.activity.company_activity_cache
where date > $user_activity_watermark and date <= $user_activity_max_date;
""",
f"""
insert into {BIDSTREAM_DATABASE}.activity.company_activity_cache
with filtered_ip_mappings as (
select distinct
  ip,
  normalized_company_domain,
  score
from {DATAMART_DATABASE}.entity_mappings.ip_to_company_domain
where (not endswith(ip,'.0')) and (not (normalized_company_domain in (select distinct domain from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.KNOWN_ISP_DOMAINS)))
  and len(normalized_company_domain)>1 and normalized_company_domain!='shared'
),
-- 1. left join user activity to ip->domain with ISPs and .0s taken out
ip_joined as (
select
    a.*,
    normalized_company_domain,
    score
from {BIDSTREAM_DATABASE}.activity.user_activity_cache a
left join filtered_ip_mappings d
on a.user_ip = d.ip
where a.date > $user_activity_watermark and a.date <= $user_activity_max_date
),
-- 2. left join 1* to DIG
dig_joined as (
select 
    a.* exclude (normalized_company_domain, score),
    dig.normalized_company_domain,
    dig.score
from (select * from ip_joined where normalized_company_domain is null) a
left join {DATAMART_DATABASE}.entity_mappings.bitoid_to_company_domain dig
on a.user_id = dig.bitoid
where len(dig.normalized_company_domain)>1 and dig.normalized_company_domain!='shared'
),
-- 3. inner join 1* to ip trio -> domain table
filtered_trio_mappings as (
select 
  ip_trio,
  normalized_company_domain,
  score
from 
{DATAMART_DATABASE}."ENTITY_MAPPINGS"."IP_TRIO_MAPPINGS"
  where (not (normalized_company_domain in (select distinct normalized_company_domain from {BIDSTREAM_DATABASE}.entity_relationships.ip_resolvable_domains where date between current_date - 180 and current_date)))
  and len(normalized_company_domain)>1 and normalized_company_domain!='shared'
),
trio_joined as (
select
    a.* exclude (normalized_company_domain, score),
    b.normalized_company_domain,
    b.score
from (select * from ip_joined where normalized_company_domain is null) a
join filtered_trio_mappings b
on (SPLIT_PART(a.user_ip, '.', 1) || '.' || SPLIT_PART(a.user_ip, '.', 2) || '.' || SPLIT_PART(a.user_ip, '.', 3)) = b.ip_trio
),
-- 4. join results 1+2+3 to location
combined_w_location as (
  select 
    a.* exclude bw_city_normalized,
    iff((bw_country_normalized is not null) and (bw_region_normalized is not null) and (g.normalized_city_name is not null) and (bw_zip_normalized is not null), true, false) as bw_loc_not_null,
    l.normalized_country_code as ip_country, 
    l.normalized_region_code as ip_region, 
    l.normalized_city_name as ip_city,
    l.normalized_zip::varchar as ip_zip,
    iff((ip_country is not null) and (ip_region is not null) and (ip_city is not null) and (ip_zip is not null), true, false) as ip_loc_not_null,
    iff(ip_loc_not_null, ip_country, bw_country_normalized) as norm_country_code, --naming this way for this part to avoid name collisions
    iff(ip_loc_not_null, ip_region, bw_region_normalized) as norm_region_code, --naming this way for this part to avoid name collisions
    iff(ip_loc_not_null, ip_city, g.normalized_city_name) as norm_city_name, --naming this way for this part to avoid name collisions
    iff(ip_loc_not_null, ip_zip, bw_zip_normalized) as chosen_zip,
    iff(is_integer(try_to_number(chosen_zip)) 
        and norm_country_code = 'USA' 
        and len(chosen_zip) = 4, '0'||chosen_zip,chosen_zip) as norm_zip --naming this way for this part to avoid name collisions
  from (
(select * from ip_joined where normalized_company_domain is not null)
union all
(select * from dig_joined where normalized_company_domain is not null)
union all
(select * from trio_joined where normalized_company_domain is not null)
) a
left join {DATAMART_DATABASE}.entity_mappings.ip_to_location l
on a.user_ip = l.ip
left join {DATAMART_DATABASE}.geographics.unique_geos g
on bw_country_normalized = g.normalized_country_code and bw_region_normalized = g.normalized_region_code and bw_zip_normalized = g.normalized_zip
)
--5. filter and aggregate
   select
   page_url,
   any_value(publisher_domain_normalized) as publisher_domain_normalized,
   normalized_company_domain,
   date,
   norm_country_code as normalized_country_code,
   norm_region_code as normalized_region_code,
   norm_city_name as normalized_city_name,
   norm_zip as normalized_zip,
   sum(pageviews) as pageviews,
   sum(score*pageviews) as weighted_pageviews,
   count(distinct user_id) as unique_devices,
   count(distinct user_ip) as unique_ips
   from combined_w_location
   where normalized_country_code is not null
   and normalized_region_code is not null
   and normalized_city_name is not null
   and normalized_zip is not null
   group by 1,3,4,5,6,7,8
;
"""]

company_activity_cache_to_cumulative_query = [
f"""
merge into {BIDSTREAM_DATABASE}.activity.company_activity t
using {BIDSTREAM_DATABASE}.activity.company_activity_cache s
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

delete_from_user_activity_cache_query = [f"""
delete from {BIDSTREAM_DATABASE}.activity.user_activity_cache
where date in (select distinct date from {BIDSTREAM_DATABASE}.activity.company_activity_cache);
"""]

# --- PRESCORING ---- #
enriched_company_activity_cache_query = [f"""
create or replace transient table {BIDSTREAM_DATABASE}.activity.enriched_company_activity_cache as
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
    from {BIDSTREAM_DATABASE}.activity.company_activity_cache activity
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
    from {BIDSTREAM_DATABASE}.activity.company_activity_cache activity
    join flattened_brands brands
    on activity.page_url = brands.page_url
    join {AIML_DATABASE}."CONTEXT_CLASSIFIER"."OUTPUT" context
    on activity.page_url = context.page_url
    join {AIML_DATABASE}."TITLE_CLASSIFIER"."OUTPUT" title
    on activity.page_url = title.page_url;
"""]

prescoring_cache_query = [f"""
merge into {BIDSTREAM_DATABASE}.activity.prescoring_cache t
using (
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
        sum(probability*weighted_pageviews*greatest(title_output:"BUSINESS", title_output:"BUSINESS NEWS", title_output:"SCIENCE TECH NEWS")) as weighted_pageviews,
        avg(greatest(title_output:"BUSINESS", title_output:"BUSINESS NEWS", title_output:"SCIENCE TECH NEWS")) as avg_page_relevance,
        avg(case activity_type
              when 'downloading' then 100
              when 'reading' then 85
              when 'visiting' then 50
              when 'executing' then 10
              when 'contacting' then 10
              else 5 end) as activity_type_score,
        avg(case information_type
              when 'informational' then 100
              when 'transactional' then 75
              when 'navigational' then 25
              else 5 end) as information_type_score,
        sum(context_output:"review/comparison") as review_pageviews,
        count(distinct page_url) as unique_pages,
        count(distinct publisher_domain_normalized) as unique_pubs,
        sum(unique_devices) as unique_devices,
        sum(unique_ips) as unique_ips
        -- table definition
        from {BIDSTREAM_DATABASE}.activity.enriched_company_activity_cache
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
avg_page_relevance = (s.avg_page_relevance*s.pageviews + t.avg_page_relevance*t.pageviews)/(s.pageviews + t.pageviews),
activity_type_score = (s.activity_type_score*s.pageviews + t.activity_type_score*t.pageviews)/(s.pageviews + t.pageviews),
information_type_score = (s.information_type_score*s.pageviews + t.information_type_score*t.pageviews)/(s.pageviews + t.pageviews),
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
;
"""]

delete_from_company_activity_cache_query = [f"""
delete from {BIDSTREAM_DATABASE}.activity.company_activity_cache a
using (select distinct 
        page_url,
        normalized_company_domain,
        date,
        normalized_country_code,
        normalized_region_code,
        normalized_city_name,
        normalized_zip 
        from {BIDSTREAM_DATABASE}.activity.enriched_company_activity_cache) b
where a.page_url=b.page_url
and a.NORMALIZED_COMPANY_DOMAIN=b.NORMALIZED_COMPANY_DOMAIN
and a.date=b.date
and equal_null(a.NORMALIZED_COUNTRY_CODE,b.NORMALIZED_COUNTRY_CODE)=true
and equal_null(a.normalized_region_code,b.normalized_region_code)=true
and equal_null(a.normalized_city_name,b.normalized_city_name)=true
and equal_null(a.normalized_zip,b.normalized_zip)= true;
"""]

clear_enriched_company_activity_cache_query = [f"""
truncate table {BIDSTREAM_DATABASE}."ACTIVITY"."ENRICHED_COMPANY_ACTIVITY_CACHE";
"""]

# ---- UNIQUE URLS ---- #
unique_urls_query = [f"""
set company_activity_watermark = (select ifnull(max(last_seen), '2022-10-13') from {BIDSTREAM_DATABASE}.content.unique_urls);
""",
"""
set company_activity_max_date = (select dateadd(day, {}, $company_activity_watermark));
""".format(WBI_SIZE_TRANSFORMS),
f"""
merge into {BIDSTREAM_DATABASE}.content.unique_urls_cache t
using (
select
    page_url,
    hash(page_url) as page_url_hash,
    any_value(publisher_domain_normalized) as publisher_domain_normalized,
    min(date) as first_seen,
    max(date) as last_seen,
    count(*) as frequency
from {BIDSTREAM_DATABASE}.activity.company_activity_cache
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

unique_urls_cache_to_cumulative_query = [f"""
merge into {BIDSTREAM_DATABASE}.content.unique_urls t
using {BIDSTREAM_DATABASE}.content.unique_urls_cache s
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

delete_from_unique_urls_cache_query = [f"""
Delete from {BIDSTREAM_DATABASE}.CONTENT.UNIQUE_URLS_CACHE 
where page_url in (select distinct page_url from {AIML_DATABASE}.web_scraper.input_cache);
""",
f"""
Delete from {BIDSTREAM_DATABASE}.CONTENT.UNIQUE_URLS_CACHE 
where page_url in (select distinct page_url from {AIML_DATABASE}.web_scraper.results);
"""]

# ---- WEB Scraper Data load ---- #

web_scraper_input_cache_query = [
f"""merge into {AIML_DATABASE}.WEB_SCRAPER.INPUT_CACHE t
using (
select distinct
    a.page_url,
  a.last_seen
from {BIDSTREAM_DATABASE}.CONTENT.UNIQUE_URLS_CACHE a
left join {AIML_DATABASE}.web_scraper.results b
on a.page_url = b.page_url
where last_scraped is null
and not (publisher_domain_normalized in 
  (select distinct publisher_domain_normalized from {DATAMART_DATABASE}.FILTERS_SUPPRESSIONS.HIGH_VOLUME_BAD_PUBS_SUPPRESS))
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
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    user_activity_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "user_activity_cache_to_cumulative",
    sql= user_activity_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    # --- IP MAPPINGS --- #
    bidstream_ip_mappings_exec = SnowflakeOperator(
    task_id= "bidstream_ip_mappings_cache",
    sql= bidstream_ip_mappings_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    bidstream_ip_mappings_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "bidstream_ip_mappings_cache_to_cumulative",
    sql= bidstream_ip_mappings_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    datamart_ip_loc_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_loc_update",
    sql= datamart_ip_location_mappings_update_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    de_ip_domain_obs_exec = SnowflakeOperator(
    task_id= "de_ip_domain_obs_update",
    sql= de_ip_domain_obs_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    datamart_ip_domain_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_domain_update",
    sql= datamart_ip_domain_mappings_update_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    datamart_ip_trio_domain_mappings_exec = SnowflakeOperator(
    task_id= "datamart_ip_trio_domain_update",
    sql= datamart_ip_trio_domain_mappings_update_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    #-----DIG LOGIC ----#
    bitoid_to_ip_cache_exec = SnowflakeOperator(
    task_id= "bitoid_to_ip_cache",
    sql= bitoid_to_ip_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    bitoid_to_domain_obs_exec = SnowflakeOperator(
    task_id= "bitoid_to_domain_obs",
    sql= bitoid_to_domain_obs_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    bitoid_to_domain_map_exec = SnowflakeOperator(
    task_id= "bitoid_to_domain_map",
    sql= bitoid_to_domain_mappings_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    bitoid_to_ip_cache_to_obs_exec = SnowflakeOperator(
    task_id= "bitoid_to_ip_cache_to_obs",
    sql= bitoid_to_ip_cache_to_obs_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    clear_bitoid_to_ip_cache_exec = SnowflakeOperator(
    task_id= "clear_bitoid_to_ip_cache",
    sql= clear_bitoid_to_ip_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    # --- COMPANY ACTIVITY --- #
    company_activity_exec = SnowflakeOperator(
    task_id= "company_activity_cache",
    sql= company_activity_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    company_activity_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "company_activity_cache_to_cumulative",
    sql= company_activity_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    delete_from_user_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_user_activity_cache",
    sql= delete_from_user_activity_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    # --- PRESCORING --- #
    enriched_company_activity_cache_exec = SnowflakeOperator(
    task_id= "enriched_company_activity_cache",
    sql= enriched_company_activity_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    prescoring_cache_exec = SnowflakeOperator(
    task_id= "prescoring_cache",
    sql= prescoring_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    delete_from_company_activity_cache_exec = SnowflakeOperator(
    task_id= "delete_from_company_activity_cache",
    sql= delete_from_company_activity_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    clear_enriched_company_activity_cache_exec = SnowflakeOperator(
    task_id= "clear_enriched_company_activity_cache",
    sql= clear_enriched_company_activity_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )


    # --- UNIQUE URLS --- #
    unique_urls_exec = SnowflakeOperator(
    task_id= "unique_urls_cache",
    sql= unique_urls_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    unique_urls_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "unique_urls_cache_to_cumulative",
    sql= unique_urls_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )



    # --- WEB SCRAPER --- #
    web_scraper_input_cache_exec = SnowflakeOperator(
    task_id= "web_scraper_input__cache",
    sql= web_scraper_input_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    
    delete_from_unique_urls_cache_exec = SnowflakeOperator(
    task_id= "delete_from_unique_urls_cache",
    sql= delete_from_unique_urls_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    #----OTHER STEPS----
    end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )
    
    #main logic
    user_activity_exec >> bidstream_ip_mappings_exec >> datamart_ip_loc_mappings_exec >> de_ip_domain_obs_exec >> datamart_ip_domain_mappings_exec >> datamart_ip_trio_domain_mappings_exec
    datamart_ip_trio_domain_mappings_exec >> bitoid_to_ip_cache_exec >> bitoid_to_domain_obs_exec >> bitoid_to_domain_map_exec >> company_activity_exec
    company_activity_exec >> unique_urls_exec >> web_scraper_input_cache_exec
    company_activity_exec >> enriched_company_activity_cache_exec >> prescoring_cache_exec
    
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
    prescoring_cache_exec >> clear_enriched_company_activity_cache_exec

    #end success logic

    [delete_from_company_activity_cache_exec, bidstream_ip_mappings_cache_to_cumulative_exec, 
    delete_from_user_activity_cache_exec, delete_from_unique_urls_cache_exec, clear_bitoid_to_ip_cache_exec
    ] >> clear_enriched_company_activity_cache_exec >> end_success_exec
