import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# ---Variable definition--- #
SNS_ARN = ... #TODO: change to scoring SNS arn
DAG_NAME = 'Intent_Scoring'
TRANSFORM_CONNECTION = "Airflow-Dev_Transform-connection"
SCORE_DATE_GLOBAL_MIN = '2023-02-01'

dt = datetime.now()
date_time = dt.strftime("%m%d%Y%H:%M:%S")
DOW = dt.weekday()

# --- Function Definitions --- #
def end_success():
  logger.info("DAG Ended Successfully.")

def on_failure_callback(context):
    """
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)
    """
    pass

    
def on_success_callback(context):
    pass
    """
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="DAG Success"
        ,message=f"{DAG_NAME} has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)
    """
    
TRANSFORM_CONN = "Airflow-Dev_Transform-connection"

dag = DAG(DAG_NAME, start_date = datetime(2022, 12, 7), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'depends_on_past' : False,'retries' : 0,'on_failure_callback': on_failure_callback,'on_success_callback': None})

# -- PYTHON FUNCTIONS ---- #
def quote_wrap(s):
    return "'" + s + "'"

def get_min_score_date(snow_hook):
    date_format = '%Y-%m-%d'
    global_min = datetime.strptime(SCORE_DATE_GLOBAL_MIN, date_format).date()
    min_cache_result = snow_hook.get_first("select min(date) from DEV_BIDSTREAM.ACTIVITY.PRESCORING_CACHE")
    min_cache_date = min_cache_result['MIN(DATE)']
    if min_cache_date is None:
        return global_min
    min_score_date = max([global_min, min_cache_date])
    return min_score_date

def scoring_input_cache():
    date_format = '%Y-%m-%d'
    snow_hook = SnowflakeHook(snowflake_conn_id=TRANSFORM_CONNECTION)
    #get min date from caches
    min_score_date = get_min_score_date(snow_hook)
    #set lookback date
    lookback_date = min_score_date - timedelta(days = 120)
    lookback_date_str = lookback_date.strftime(date_format)
    #create input cache
    if DOW == 6:
        snow_hook.run(scoring_input_cache_without_join(quote_wrap(lookback_date_str)))
    else:
        snow_hook.run(scoring_input_cache_with_join(quote_wrap(lookback_date_str)))

def intent_scoring_backfill():
    date_format = '%Y-%m-%d'
    snow_hook = SnowflakeHook(snowflake_conn_id=TRANSFORM_CONNECTION)
    min_score_date = get_min_score_date(snow_hook)
    print(f"Min date to begin scoring: {min_score_date}")
    max_date = datetime.today().date()
    print(f"Max date to score to: {max_date}")
    #create list of dates to re-score from min_cache_date to current date
    date_diff = max_date - min_score_date
    date_list = [max_date - timedelta(days = x) for x in range(date_diff.days)]
    print(f"dates to score: {date_list}")
    #for each date in date list, score and merge into output cache
    for score_date in date_list:
        score_date_str = score_date.strftime(date_format)
        print(f"scoring for {score_date_str}")
        snow_hook.run(scoring_query(quote_wrap(score_date_str)))

# ---- SNOWFLAKE QUERIES ----
def scoring_input_cache_without_join(lookback_date):
    query = f"""
    create or replace table dev_aiml.intent_scoring.input_cache as (
  with subset as (
  select * from "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING"
  where (title_output:"BUSINESS">=0.8 or title_output:"BUSINESS NEWS">=0.4 or title_output:"SCIENCE TECH NEWS">=0.55)
  and date >= date({lookback_date})
  and date < current_date),
    
    bidstream as (
      select
        -- rollup cols
        t.normalized_company_domain,
        f.value:"parentCategory"::varchar as parent_category,
        f.value:"category"::varchar as category,
        f.value:"topic"::varchar as topic,
        t.normalized_country_code,
        t.normalized_region_code,
        t.normalized_city_name,
        t.normalized_zip,
        t.date,
        -- feature cols
        sum(t.pageviews) as pageviews,
        sum(f.value:"probability"*t.weighted_pageviews*greatest(t.title_output:"BUSINESS", t.title_output:"BUSINESS NEWS", t.title_output:"SCIENCE TECH NEWS")) as weighted_pageviews,
        avg(greatest(t.title_output:"BUSINESS", t.title_output:"BUSINESS NEWS", t.title_output:"SCIENCE TECH NEWS")) as avg_page_relevance,
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
        sum(t.context_output:"review/comparison") as review_pageviews,
        count(distinct t.page_url) as unique_pages,
        count(distinct t.publisher_domain_normalized) as unique_pubs,
        sum(t.unique_devices) as unique_devices,
        sum(t.unique_ips) as unique_ips
        -- table definition
        from subset t,
        lateral flatten(input=>t.intent_topics) f
        group by 1,2,3,4,5,6,7,8,9 )

    -- other sources would go here

    -- main select statement
    select
      *,
      avg(weighted_pageviews) over (partition by normalized_company_domain,
                                                   parent_category,
                                                    category,
                                                    topic,
                                                    normalized_country_code,
                                                    normalized_region_code,
                                                    normalized_city_name,
                                                    normalized_zip
                                   order by date asc
                                   rows between unbounded preceding and current row) as moving_avg_weighted_pageviews,
      stddev(weighted_pageviews) over (partition by normalized_company_domain,
                                                   parent_category,
                                                    category,
                                                    topic,
                                                    normalized_country_code,
                                                    normalized_region_code,
                                                    normalized_city_name,
                                                    normalized_zip
                                   order by date asc
                                   rows between unbounded preceding and current row) as moving_stddev_weighted_pageviews
    from bidstream);

    """
    return query

def scoring_input_cache_with_join(lookback_date):
    query = f"""
    create or replace table dev_aiml.intent_scoring.input_cache as (
  with subset as (
  select * from "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING" t
  join (select distinct 
                normalized_company_domain,
                normalized_country_code,
                normalized_region_code,
                normalized_city_name,
                normalized_zip
              from "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING_CACHE" ) a
        on t.normalized_company_domain = a.normalized_company_domain
        and t.normalized_country_code = a.normalized_country_code
        and t.normalized_region_code = a.normalized_region_code
        and t.normalized_city_name = a.normalized_city_name
        and t.normalized_zip = a.normalized_zip
  where (title_output:"BUSINESS">=0.8 or title_output:"BUSINESS NEWS">=0.4 or title_output:"SCIENCE TECH NEWS">=0.55)
  and date >= date({lookback_date})
  and date < current_date),
    bidstream as (
      select
        -- rollup cols
        t.normalized_company_domain,
        f.value:"parentCategory"::varchar as parent_category,
        f.value:"category"::varchar as category,
        f.value:"topic"::varchar as topic,
        t.normalized_country_code,
        t.normalized_region_code,
        t.normalized_city_name,
        t.normalized_zip,
        t.date,
        -- feature cols
        sum(t.pageviews) as pageviews,
        sum(f.value:"probability"*t.weighted_pageviews*greatest(t.title_output:"BUSINESS", t.title_output:"BUSINESS NEWS", t.title_output:"SCIENCE TECH NEWS")) as weighted_pageviews,
        avg(greatest(t.title_output:"BUSINESS", t.title_output:"BUSINESS NEWS", t.title_output:"SCIENCE TECH NEWS")) as avg_page_relevance,
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
        sum(t.context_output:"review/comparison") as review_pageviews,
        count(distinct t.page_url) as unique_pages,
        count(distinct t.publisher_domain_normalized) as unique_pubs,
        sum(t.unique_devices) as unique_devices,
        sum(t.unique_ips) as unique_ips
        -- table definition
        from subset t,
        lateral flatten(input=>t.intent_topics) f
        group by 1,2,3,4,5,6,7,8,9 )

    -- other sources would go here

    -- main select statement
    select
      *,
      avg(weighted_pageviews) over (partition by normalized_company_domain,
                                                   parent_category,
                                                    category,
                                                    topic,
                                                    normalized_country_code,
                                                    normalized_region_code,
                                                    normalized_city_name,
                                                    normalized_zip
                                   order by date asc
                                   rows between unbounded preceding and current row) as moving_avg_weighted_pageviews,
      stddev(weighted_pageviews) over (partition by normalized_company_domain,
                                                   parent_category,
                                                    category,
                                                    topic,
                                                    normalized_country_code,
                                                    normalized_region_code,
                                                    normalized_city_name,
                                                    normalized_zip
                                   order by date asc
                                   rows between unbounded preceding and current row) as moving_stddev_weighted_pageviews
    from bidstream);

    """
    return query

def scoring_query(score_date):
    query = f"""
    merge into "DEV_AIML"."INTENT_SCORING"."OUTPUT_CACHE" t
   using (
      with lookback_input_cache as (
      select * from "DEV_AIML"."INTENT_SCORING"."INPUT_CACHE"
      where date >= dateadd(day, -91, date({score_date}))
      and date < date({score_date})),
    
      weekly_volume as (
      select
        normalized_company_domain,
        parent_category,
        category,
        topic,
        normalized_country_code,
        normalized_region_code,
        normalized_city_name,
        normalized_zip,
        floor((date({score_date}) - date)/7) as weekno,
        sum(weighted_pageviews) as weekly_weighted_pageviews
      from lookback_input_cache
      group by 1,2,3,4,5,6,7,8,9),
      this_week_activity  as (
      select * from weekly_volume where weekno = 0),
      last_week_activity as (
      select * from weekly_volume where weekno = 1),
      --monthly volume
      monthly_volume as (
      select
        normalized_company_domain,
        parent_category,
        category,
        topic,
        normalized_country_code,
        normalized_region_code,
        normalized_city_name,
        normalized_zip,
        floor((date({score_date}) - date)/30) as monthno,
        sum(weighted_pageviews) as weekly_weighted_pageviews
      from lookback_input_cache
      group by 1,2,3,4,5,6,7,8,9),
      this_month_activity  as (
      select * from monthly_volume where monthno = 0),
      last_month_activity as (
      select * from monthly_volume where monthno = 1)
      select
      -- identifier fields
        date({score_date}) as score_date,
        a.normalized_company_domain,
        a.parent_category,
        a.category,
        a.topic,
        a.normalized_country_code,
        a.normalized_region_code,
        a.normalized_city_name,
        a.normalized_zip,
      -- score factors
      -- VOLUME
        least(100, sum(iff(weighted_pageviews > moving_avg_weighted_pageviews + moving_stddev_weighted_pageviews, 30*avg_page_relevance, 10*avg_page_relevance))) as volume,
      -- RECENCY
        (sum(weighted_pageviews*(91-(date({score_date}) - date)))/(90*sum(weighted_pageviews)))*100 as recency,
      -- VARIETY
        50*(sum(unique_pages)/sum(pageviews)) + 50*(sum(unique_pubs)/sum(unique_pages)) as variety,
      -- CONTEXT
        case sum(review_pageviews)
            when 0 then 0
            when 1 then 10
            when 2 then 20
            when 3 then 30
            when 4 then 40
            when 5 then 50
            when 6 then 60
            when 7 then 70
            when 8 then 80
            when 9 then 90
            else 100 end as review_score,
        avg(activity_type_score*avg_page_relevance) as activity_score,
        avg(information_type_score*avg_page_relevance) as information_score,
        (0.3*review_score + 0.5*activity_score + 0.2*information_score) as context,
      -- TREND
      any_value(ifnull(tw.weekly_weighted_pageviews, 0)) as tw_pageviews,
      any_value(ifnull(lw.weekly_weighted_pageviews, 0)) as lw_pageviews,
      case
        when tw_pageviews = 0 and lw_pageviews = 0 then 0
        when tw_pageviews > 0 and lw_pageviews = 0 then 1
        else (tw_pageviews - lw_pageviews)/lw_pageviews
      end as week_over_week_trend,
      any_value(ifnull(tm.weekly_weighted_pageviews, 0)) as tm_pageviews,
      any_value(ifnull(lm.weekly_weighted_pageviews, 0)) as lm_pageviews,
      case
        when tm_pageviews = 0 and lm_pageviews = 0 then 0
        when tm_pageviews > 0 and lm_pageviews = 0 then 1
        else (tm_pageviews - lm_pageviews)/lm_pageviews
      end as month_over_month_trend,
      -- FINAL SCORE
      least(100,greatest(0, (0.3*volume + 0.3*recency + 0.3*context + 0.1*variety)+least(10,greatest(0, 10*week_over_week_trend))+least(20,greatest(-10, 20*month_over_month_trend)))) as intent_score
      from lookback_input_cache a
      --join to this week's activity
      left join this_week_activity tw
      on a.normalized_company_domain = tw.normalized_company_domain
      and a.parent_category = tw.parent_category
      and a.category = tw.category
      and a.topic = tw.topic
      and a.normalized_country_code = tw.normalized_country_code
      and a.normalized_region_code = tw.normalized_region_code
      and a.normalized_city_name = tw.normalized_city_name
      and a.normalized_zip = tw.normalized_zip
      --join to last week's activity
      left join last_week_activity lw
      on a.normalized_company_domain = lw.normalized_company_domain
      and a.parent_category = lw.parent_category
      and a.category = lw.category
      and a.topic = lw.topic
      and a.normalized_country_code = lw.normalized_country_code
      and a.normalized_region_code = lw.normalized_region_code
      and a.normalized_city_name = lw.normalized_city_name
      and a.normalized_zip = lw.normalized_zip
      --join to this month's activity
      left join this_month_activity tm
      on a.normalized_company_domain = tm.normalized_company_domain
      and a.parent_category = tm.parent_category
      and a.category = tm.category
      and a.topic = tm.topic
      and a.normalized_country_code = tm.normalized_country_code
      and a.normalized_region_code = tm.normalized_region_code
      and a.normalized_city_name = tm.normalized_city_name
      and a.normalized_zip = tm.normalized_zip
      --join to last month's activity
      left join last_month_activity lm
      on a.normalized_company_domain = lm.normalized_company_domain
      and a.parent_category = lm.parent_category
      and a.category = lm.category
      and a.topic = lm.topic
      and a.normalized_country_code = lm.normalized_country_code
      and a.normalized_region_code = lm.normalized_region_code
      and a.normalized_city_name = lm.normalized_city_name
      and a.normalized_zip = lm.normalized_zip
      group by 1,2,3,4,5,6,7,8,9) s
      -- merge insert logic continued
      on t.normalized_company_domain = s.normalized_company_domain
      and t.parent_category = s.parent_category
      and t.category = s.category
      and t.topic = s.topic
      and t.normalized_country_code = s.normalized_country_code
      and t.normalized_region_code = s.normalized_region_code
      and t.normalized_city_name = s.normalized_city_name
      and t.normalized_zip = s.normalized_zip
      and t.score_date = s.score_date
      when not matched then insert
      (score_date, normalized_company_domain, parent_category, category, topic, 
       normalized_country_code, normalized_region_code, normalized_city_name, normalized_zip,
       volume, recency, variety, context, week_over_week_trend, month_over_month_trend, intent_score
      )
      values
      (s.score_date, s.normalized_company_domain, s.parent_category, s.category, s.topic, 
       s.normalized_country_code, s.normalized_region_code, s.normalized_city_name, s.normalized_zip,
       s.volume, s.recency, s.variety, s.context, s.week_over_week_trend, s.month_over_month_trend, s.intent_score
      )
      when matched then update set
      volume = s.volume,
      recency = s.recency,
      variety = s.variety,
      context = s.context,
      week_over_week_trend = s.week_over_week_trend,
      month_over_month_trend = s.month_over_month_trend,
      intent_score = s.intent_score;

    """
    return query

historical_merge_query = ["""
merge into dev_aiml.intent_scoring.intent_scores_historical t
using dev_aiml.intent_scoring.output_cache s
  on t.normalized_company_domain = s.normalized_company_domain
  and t.parent_category = s.parent_category
  and t.category = s.category
  and t.topic = s.topic
  and t.normalized_country_code = s.normalized_country_code
  and t.normalized_region_code = s.normalized_region_code
  and t.normalized_city_name = s.normalized_city_name
  and t.normalized_zip = s.normalized_zip
  and t.score_date = s.score_date
  when not matched then insert
  (score_date, normalized_company_domain, parent_category, category, topic, 
   normalized_country_code, normalized_region_code, normalized_city_name, normalized_zip,
   volume, recency, variety, context, trend, intent_score
  )
  values
  (s.score_date, s.normalized_company_domain, s.parent_category, s.category, s.topic, 
   s.normalized_country_code, s.normalized_region_code, s.normalized_city_name, s.normalized_zip,
   s.volume, s.recency, s.variety, s.context, s.trend, s.intent_score
  )
  when matched then update set
  volume = s.volume,
  recency = s.recency,
  variety = s.variety,
  context = s.context,
  trend = s.trend,
  intent_score = s.intent_score;
"""]

clear_bidstream_prescoring_cache_query = ["""
truncate table dev_bidstream.activity.prescoring_cache;
"""]

bidstream_prescoring_cache_to_cumulative_query = ["""
merge into "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING" t
 using "DEV_BIDSTREAM"."ACTIVITY"."PRESCORING_CACHE" s
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
 title_output,
 url_type,
 activity_type,
 information_type)
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
 s.title_output,
 s.url_type,
 s.activity_type,
 s.information_type)
 when matched then update set
 publisher_domain_normalized = s.publisher_domain_normalized,
 pageviews = s.pageviews,
 unique_devices = s.unique_devices,
 unique_ips = s.unique_ips,
 intent_topics = s.intent_topics,
 context_output = s.context_output,
 title_output = s.title_output,
 url_type = s.url_type,
 activity_type = s.activity_type,
 information_type = s.information_type;
"""]

with dag:
    # --- merge prescoring input caches into cumulative tables --- #
    bidstream_prescoring_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "bidstream_prescoring_cache_to_cumulative",
    sql= bidstream_prescoring_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    # --- Gather prescoring tables/Populate scoring input cache --- #
    scoring_input_cache_exec = PythonOperator(
    task_id= "create_scoring_input_cache",
    python_callable = scoring_input_cache
    )

    # --- Execute Scoring --- #
    intent_scoring_exec = PythonOperator(
    task_id= "intent_scoring_with_backfill",
    python_callable = intent_scoring_backfill
    )

    # --- Merge into historical scores table --- #
    historical_merge_exec = SnowflakeOperator(
    task_id= "merge_into_historical_scores",
    sql= historical_merge_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    
    # --- Clear prescoring caches --- #
    clear_bidstream_prescoring_cache_exec = SnowflakeOperator(
    task_id= "clear_bidstream_prescoring_cache",
    sql= clear_bidstream_prescoring_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    # --- End Success --- #
    end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )

    # --- DAG FLOW ---#
    bidstream_prescoring_cache_to_cumulative_exec >> scoring_input_cache_exec >> intent_scoring_exec >> historical_merge_exec >> clear_bidstream_prescoring_cache_exec >> end_success_exec