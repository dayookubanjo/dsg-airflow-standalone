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
BIDSTREAM_DATABASE = Variable.get("BIDSTREAM_DATABASE")
AIML_DATABASE = Variable.get("AIML_DATABASE")
PIXEL_DATABASE = Variable.get("PIXEL_DATABASE")
EMAIL_CAMPAIGNS_DATABASE = Variable.get("EMAIL_CAMPAIGNS_DATABASE")
LEADSIFT_DATABASE = Variable.get("LEADSIFT_DATABASE")

# ---DAG Variable definitions--- #
DAG_NAME = 'Intent_Scoring'
SCORE_DATE_GLOBAL_MIN = '2023-02-01'
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
        ,subject="INTENT_SCORING DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="INTENT_SCORING DAG SUCCESs"
        ,message=f"{DAG_NAME} has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)
    

dag = DAG(DAG_NAME, start_date = datetime(2022, 12, 7), schedule_interval = '@weekly', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'depends_on_past' : False,'retries' : 0,'on_failure_callback': on_failure_callback,'on_success_callback': None})

# -- PYTHON FUNCTIONS ---- #
def quote_wrap(s):
    return "'" + s + "'"

def get_min_score_date(snow_hook):
    date_format = '%Y-%m-%d'
    global_min = datetime.strptime(SCORE_DATE_GLOBAL_MIN, date_format).date()
    min_cache_result = snow_hook.get_first(f"select min(date) from {BIDSTREAM_DATABASE}.ACTIVITY.PRESCORING_CACHE")
    print("query result: ",min_cache_result)
    min_cache_date = min_cache_result[0]
    if min_cache_date is None:
        return global_min
    min_score_date = max([global_min, min_cache_date])
    return min_score_date

def scoring_input_cache():
    date_format = '%Y-%m-%d'
    snow_hook = SnowflakeHook(snowflake_conn_id=TRANSFORM_CONNECTION)
    #get min date from caches
    #min_score_date = get_min_score_date(snow_hook) #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    min_score_date = datetime.today().date() #COMMENT THIS OUT WHEN SWITCHING BACK TO DAILY UPDATES
    #set lookback date
    lookback_date = min_score_date - timedelta(days = 120)
    lookback_date_str = lookback_date.strftime(date_format)
    #create input cache
    join_clause = ""
    #if DOW == 6: #UNCOMMENT THIS AND THE ELSE BLOCK WHEN SWITCHING BACK TO DAILY UPDATES
      #join_clause = """join (select distinct 
      #          normalized_company_domain,
      #          normalized_country_code,
      #          normalized_region_code,
      #          normalized_city_name,
      #          normalized_zip,
      #          parent_category,
      #          category,
      #          topic
      #        from {BIDSTREAM_DATABASE}.ACTIVITY.PRESCORING_CACHE ) a
      #  on t.normalized_company_domain = a.normalized_company_domain
      #  and t.parent_category = a.parent_category
      #  and t.category = a.category
      #  and t.topic = a.topic
      #  and equal_null(t.normalized_country_code,a.normalized_country_code)
      #  and equal_null(t.normalized_region_code,a.normalized_region_code)
      #  and equal_null(t.normalized_city_name,a.normalized_city_name)
      #  and equal_null(t.normalized_zip,a.normalized_zip) """
    snow_hook.run(create_scoring_input_cache(quote_wrap(lookback_date_str), join_clause))

def intent_scoring_backfill():
    date_format = '%Y-%m-%d'
    snow_hook = SnowflakeHook(snowflake_conn_id=TRANSFORM_CONNECTION)
    #min_score_date = get_min_score_date(snow_hook) #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    #print(f"Min date to begin scoring: {min_score_date}") #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    max_date = datetime.today().date()
    #print(f"Max date to score to: {max_date}") #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    #create list of dates to re-score from min_cache_date to current date
    #date_diff = max_date - min_score_date #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    #date_list = [max_date - timedelta(days = x) for x in range(date_diff.days)] #UNCOMMENT WHEN SWITCHING BACK TO DAILY UPDATES
    date_list = [max_date] #COMMENT THIS OUT WHEN SWITCHING BACK TO DAILY UPDATES
    print(f"dates to score: {date_list}")
    #for each date in date list, score and merge into output cache
    for score_date in date_list:
        score_date_str = score_date.strftime(date_format)
        print(f"scoring for {score_date_str}")
        snow_hook.run(scoring_query(quote_wrap(score_date_str)))

# ---- SNOWFLAKE QUERIES ----
def create_scoring_input_cache(lookback_date, join_clause):
    query = f"""
    create or replace table {AIML_DATABASE}.intent_scoring.input_cache as
      with bidstream as (
        select * from {BIDSTREAM_DATABASE}.ACTIVITY.PRESCORING t
        {join_clause}
        where date >= date({lookback_date}) 
        and date < current_date
      ), 
        
      pixel as (
        select * from {PIXEL_DATABASE}.ACTIVITY.PRESCORING t
        where date >= date({lookback_date}) 
        and date < current_date
      ),
      
      email_campaigns as (
        select * from {EMAIL_CAMPAIGNS_DATABASE}.ACTIVITY.PRESCORING
        where date >= date({lookback_date})
        and date < current_date
      ),
      
      leadsift as (
        select * from {LEADSIFT_DATABASE}.ACTIVITY.PRESCORING
        where date >= date({lookback_date})
        and date < current_date
      ),
      
      combined_sources as (
        select
          coalesce(b.normalized_company_domain, p.normalized_company_domain, l.normalized_company_domain, e.normalized_company_domain) as normalized_company_domain,
          coalesce(b.parent_category, p.parent_category, l.parent_category, e.parent_category) as parent_category,
          coalesce(b.category, p.category, l.category, e.category) as category,
          coalesce(b.topic, p.topic, l.topic, e.topic) as topic,
          coalesce(b.normalized_country_code, p.normalized_country_code) as normalized_country_code,
          coalesce(b.normalized_region_code, p.normalized_region_code) as normalized_region_code,
          coalesce(b.normalized_city_name, p.normalized_city_name) as normalized_city_name,
          coalesce(b.normalized_zip, p.normalized_zip) as normalized_zip,
          coalesce(b.date, p.date, l.date, e.date) as date,
          ifnull(b.pageviews,0) + ifnull(p.pageviews,0) + ifnull(ceil(iff(l.leadsift_score=0,50,l.leadsift_score)/100),0) + ifnull(ceil(e.weighted_clicks),0) + ifnull(ceil(e.weighted_opens),0) as pageviews,
          1000*(ifnull(b.weighted_pageviews,0) + 10*ifnull(p.weighted_pageviews,0) + ifnull(iff(l.leadsift_score=0,50,l.leadsift_score)/100,0) + ifnull(e.weighted_clicks,0)*10 + 5*ifnull(e.weighted_opens,0)) as weighted_pageviews,
          ifnull(b.avg_page_relevance,1.0) as avg_page_relevance,
          ifnull(b.activity_type_score,100) as activity_type_score,
          ifnull(b.information_type_score,100) as information_type_score,
          ifnull(b.review_pageviews,0) as review_pageviews,
          ifnull(b.unique_pages,0) + ifnull(p.unique_pages,0) + ifnull(ceil(iff(l.leadsift_score=0,50,l.leadsift_score)/100),0) + ifnull(ceil(e.weighted_clicks),0) + ifnull(ceil(e.weighted_opens),0) as unique_pages,
          ifnull(b.unique_pubs,0) + ifnull(p.unique_pubs,0) + ifnull(ceil(iff(l.leadsift_score=0,50,l.leadsift_score)/100),0) + ifnull(ceil(e.weighted_clicks),0) + ifnull(ceil(e.weighted_opens),0) as unique_pubs
        from bidstream b
        --join to email
        full outer join email_campaigns e
        on b.normalized_company_domain = e.normalized_company_domain
        and b.parent_category = e.parent_category
        and b.category = e.category
        and b.topic = e.topic
        and b.date = e.date
        --join to leadisft
        full outer join leadsift l
        on b.normalized_company_domain = l.normalized_company_domain
        and b.parent_category = l.parent_category
        and b.category = l.category
        and b.topic = l.topic
        and b.date = l.date
        --join to pixel
        full outer join pixel p
        on b.normalized_company_domain = p.normalized_company_domain
        and b.normalized_country_code = p.normalized_country_code
        and b.normalized_region_code = p.normalized_region_code
        and b.normalized_city_name = p.normalized_city_name
        and b.normalized_zip = p.normalized_zip
        and b.parent_category = p.parent_category
        and b.category = p.category
        and b.topic = p.topic
        and b.date = p.date
    
      )
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
      ifnull(stddev(weighted_pageviews) over (partition by normalized_company_domain,
                                                   parent_category,
                                                    category,
                                                    topic,
                                                    normalized_country_code,
                                                    normalized_region_code,
                                                    normalized_city_name,
                                                    normalized_zip
                                   order by date asc
                                   rows between unbounded preceding and current row),0) as moving_stddev_weighted_pageviews
    from combined_sources
    where weighted_pageviews>0;

    """
    return query

def scoring_query(score_date):
    query = f"""
    merge into {AIML_DATABASE}.INTENT_SCORING.OUTPUT_CACHE t
   using (
      with lookback_input_cache as (
      select * from {AIML_DATABASE}.INTENT_SCORING.INPUT_CACHE
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
      least(100,greatest(0, (0.25*volume + 0.2*recency + 0.5*context + 0.05*variety)+least(10,greatest(0, 10*week_over_week_trend))+least(20,greatest(-10, 20*month_over_month_trend)))) as intent_score
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

historical_merge_query = [f"""
merge into {AIML_DATABASE}.intent_scoring.intent_scores_historical t
using {AIML_DATABASE}.intent_scoring.output_cache s
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
"""]

clear_bidstream_prescoring_cache_query = [f"""
truncate table {BIDSTREAM_DATABASE}.activity.prescoring_cache;
"""]

clear_pixel_prescoring_cache_query = [f"""
truncate table {PIXEL_DATABASE}.activity.prescoring_cache;
"""]

clear_scoring_output_cache_query = [f"""
truncate table {AIML_DATABASE}.INTENT_SCORING.OUTPUT_CACHE;
"""]

bidstream_prescoring_cache_to_cumulative_query = [f"""
merge into {BIDSTREAM_DATABASE}.activity.prescoring t
using {BIDSTREAM_DATABASE}.activity.prescoring_cache s
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

pixel_prescoring_cache_to_cumulative_query = [f"""
merge into {PIXEL_DATABASE}.activity.prescoring t
using {PIXEL_DATABASE}.activity.prescoring_cache s
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
"""]

with dag:
    # --- merge prescoring input caches into cumulative tables --- #
    bidstream_prescoring_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "bidstream_prescoring_cache_to_cumulative",
    sql= bidstream_prescoring_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    pixel_prescoring_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "pixel_prescoring_cache_to_cumulative",
    sql= pixel_prescoring_cache_to_cumulative_query,
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

    clear_pixel_prescoring_cache_exec = SnowflakeOperator(
    task_id= "clear_pixel_prescoring_cache",
    sql= clear_pixel_prescoring_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    # --- Clear scoring output cache --- #
    clear_scoring_output_cache_exec = SnowflakeOperator(
    task_id= "clear_scoring_output_cache",
    sql= clear_scoring_output_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    # --- End Success --- #
    end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )

    # --- DAG FLOW ---#
    [pixel_prescoring_cache_to_cumulative_exec,bidstream_prescoring_cache_to_cumulative_exec] >> scoring_input_cache_exec >> intent_scoring_exec >> historical_merge_exec >> [clear_bidstream_prescoring_cache_exec,clear_pixel_prescoring_cache_exec] >> clear_scoring_output_cache_exec >> end_success_exec
