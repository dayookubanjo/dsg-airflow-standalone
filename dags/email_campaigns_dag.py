import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----Importing Variables----
SNS_ARN=Variable.get("SNS_ARN")
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
EMAIL_CAMPAIGNS_DATABASE = Variable.get("EMAIL_CAMPAIGNS_DATABASE")
DATAMART_DATABASE = Variable.get("DATAMART_DATABASE")
AIML_DATABASE = Variable.get("AIML_DATABASE")

# ---Variable definition--- #
DAG_NAME = 'Email_Campaigns'
dt = datetime.now()
date_time = dt.strftime("%m%d%Y%H:%M:%S")
DOW = dt.weekday()

# --- Function Definitions --- #
def end_success():
  logger.info("DAG Ended Successfully.")

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject= "EMAIL_CAMPAIGNS DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="EMAIL_CAMPAIGNS DAG SUCCESS"
        ,message=f"{DAG_NAME} has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

dag = DAG(DAG_NAME, start_date = datetime(2022, 12, 7), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'depends_on_past' : False,'retries' : 0,'on_failure_callback': on_failure_callback,'on_success_callback': None})

#---- SNOWFLAKE QUERIES ----#
load_clicks_query = [f"""
copy into {EMAIL_CAMPAIGNS_DATABASE}."RAW_DATA"."CLICKS"
from @{EMAIL_CAMPAIGNS_DATABASE}."RAW_DATA"."CLICKS";
"""]

load_opens_query = [f"""
copy into {EMAIL_CAMPAIGNS_DATABASE}."RAW_DATA"."OPENS"
from @{EMAIL_CAMPAIGNS_DATABASE}."RAW_DATA"."OPENS";
"""]

company_clicks_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.activity.company_clicks as
select
    cam_id,
    {DATAMART_DATABASE}.public.domain_normalizer(domain) as normalized_company_domain,
    date(time) as date,
    split_part(link,'?email',1) as page_url, --index is 1-based
    count(*) as frequency
from  (select distinct * from {EMAIL_CAMPAIGNS_DATABASE}.RAW_DATA.CLICKS)
where cam_id is not null
and normalized_company_domain is not null
and date is not null
and page_url is not null
group by 1,2,3,4;
"""]

company_opens_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.activity.company_opens as
select
    cam_id,
    {DATAMART_DATABASE}.public.domain_normalizer(domain) as normalized_company_domain,
    date(time) as date,
    count(*) as frequency
from  (select distinct * from {EMAIL_CAMPAIGNS_DATABASE}.RAW_DATA.OPENS)
where cam_id is not null
and normalized_company_domain is not null
and date is not null
group by 1,2,3;
"""]

unique_urls_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.content.unique_urls as
select 
       split_part(link,'?email',1) as page_url, --index is 1-based
       any_value(hash(page_url)) as page_url_hash,
       any_value({DATAMART_DATABASE}.public.domain_normalizer(split_part(replace(replace(page_url, 'http://'), 'https://'),'/',1))) as publisher_domain_normalized,
       min(date(time)) as first_seen,
       max(date(time)) as last_seen,
       count(*) as frequency
from {EMAIL_CAMPAIGNS_DATABASE}.RAW_DATA.CLICKS
group by 1;
"""]

campaign_content_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.content.campaign_content as
select distinct
    cam_id,
    split_part(link,'?email',1) as page_url --index is 1-based
from {EMAIL_CAMPAIGNS_DATABASE}.RAW_DATA.CLICKS;
"""]

campaign_topics_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.content.campaign_topics as
with joined_topics as (
select
    t.cam_id,
    f.value:parentCategory::varchar as parent_category,
    f.value:category::varchar as category,
    f.value:topic::varchar as topic,
    sum(f.value:probability::number(5,4)) as summed_topic_probabilities
from (select a.cam_id, a.page_url, b.intent_topics from {EMAIL_CAMPAIGNS_DATABASE}.CONTENT.CAMPAIGN_CONTENT a
join {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT b
on a.page_url = b.page_url) t,
lateral flatten(input => t.intent_topics) f
group by 1,2,3,4
),
campaign_totals as (
select
    cam_id,
    sum(summed_topic_probabilities) as total
from joined_topics
group by 1)
select
    a.cam_id,
    parent_category,
    category,
    topic,
    summed_topic_probabilities/total as proportion
from joined_topics a
join campaign_totals b
on a.cam_id = b.cam_id;
"""]

web_scraper_input_query = [f"""
merge into {AIML_DATABASE}.WEB_SCRAPER.INPUT_CACHE t
using (
    select distinct a.page_url
    from {EMAIL_CAMPAIGNS_DATABASE}.CONTENT.UNIQUE_URLS a
    left join {AIML_DATABASE}.WEB_SCRAPER.RESULTS b
    on a.page_url = b.page_url
    where last_scraped is null) s
on t.page_url = s.page_url
when not matched then insert
(page_url, date_inserted)
values (s.page_url, current_date)
when matched then update set
date_inserted = current_date;
"""]

prescoring_query = [f"""
create or replace table {EMAIL_CAMPAIGNS_DATABASE}.activity.prescoring as
with joined_clicks as (
select
    t.normalized_company_domain,
    t.date,
    f.value:parentCategory::varchar as parent_category,
    f.value:category::varchar as category,
    f.value:topic::varchar as topic,
    sum(t.frequency * f.value:probability::number(5,4)) as weighted_clicks
from (select a.normalized_company_domain, a.date, a.frequency, b.intent_topics 
      from {EMAIL_CAMPAIGNS_DATABASE}.ACTIVITY.COMPANY_CLICKS a
     join {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT b
     on a.page_url = b.page_url) t,
lateral flatten(input => t.intent_topics) f
group by 1,2,3,4,5),

joined_opens as (
select
    a.normalized_company_domain,
    a.date,
    b.parent_category,
    b.category,
    b.topic,
    sum(a.frequency * b.proportion) as weighted_opens
from {EMAIL_CAMPAIGNS_DATABASE}.ACTIVITY.COMPANY_OPENS a
join {EMAIL_CAMPAIGNS_DATABASE}.CONTENT.CAMPAIGN_TOPICS b
on a.cam_id = b.cam_id
group by 1,2,3,4,5)

select
    coalesce(c.normalized_company_domain, o.normalized_company_domain) as normalized_company_domain,
    coalesce(c.date, o.date) as date,
    coalesce(c.parent_category, o.parent_category) as parent_category,
    coalesce(c.category, o.category) as category,
    coalesce(c.topic, o.topic) as topic,
    ifnull(weighted_opens, 0) as weighted_opens,
    ifnull(weighted_clicks, 0) as weighted_clicks
from joined_clicks c
full outer join joined_opens o
on c.normalized_company_domain = o.normalized_company_domain
and c.date = o.date
and c.parent_category = o.parent_category
and c.category = o.category
and c.topic = o.topic;
"""]

with dag:
    # --- load steps --- #
    clicks_load_exec = SnowflakeOperator(
    task_id= "load_clicks",
    sql=load_clicks_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    opens_load_exec = SnowflakeOperator(
    task_id= "load_opens",
    sql=load_opens_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    # --- activity steps --- #
    company_clicks_exec = SnowflakeOperator(
    task_id= "company_clicks",
    sql=company_clicks_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    company_opens_exec = SnowflakeOperator(
    task_id= "company_opens",
    sql=company_opens_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    #--- content steps ---- #
    unique_urls_exec = SnowflakeOperator(
    task_id= "unique_urls",
    sql=unique_urls_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    campaign_content_exec = SnowflakeOperator(
    task_id= "campaign_content",
    sql=campaign_content_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    campaign_topics_exec = SnowflakeOperator(
    task_id= "campaign_topics",
    sql=campaign_topics_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    web_scraper_input_exec = SnowflakeOperator(
    task_id= "web_scraper_input",
    sql=web_scraper_input_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    #---- prescoring step ---- #
    prescoring_exec = SnowflakeOperator(
    task_id= "prescoring",
    sql=prescoring_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    # --- End Success --- #
    end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )

    #--- DAG FLOW ----#
    clicks_load_exec >> [company_clicks_exec, campaign_content_exec, unique_urls_exec]
    opens_load_exec >> company_opens_exec
    [company_clicks_exec, company_opens_exec, campaign_topics_exec] >> prescoring_exec
    campaign_content_exec >> campaign_topics_exec
    unique_urls_exec >> web_scraper_input_exec
    [prescoring_exec, web_scraper_input_exec] >> end_success_exec
