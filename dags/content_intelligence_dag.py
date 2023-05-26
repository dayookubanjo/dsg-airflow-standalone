import logging
from datetime import datetime
import boto3
import botocore
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----Importing Variables----
SNS_ARN=Variable.get("SNS_ARN")
AIML_BUCKET=Variable.get("AIML_BUCKET")
LOAD_CONNECTION = Variable.get("LOAD_CONNECTION")
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
TITLE_FLAG_IN_FILENAME = Variable.get("TITLE_FLAG_IN_FILENAME")
TITLE_FLAG_OUT_FILENAME = Variable.get("TITLE_FLAG_OUT_FILENAME")
TITLE_MODEL_INPUT_PATH = Variable.get("TITLE_MODEL_INPUT_PATH")
TITLE_MODEL_THRESHOLD = Variable.get("TITLE_MODEL_THRESHOLD")
TITLE_MODEL_STAGE_NAME = Variable.get("TITLE_MODEL_STAGE_NAME")
TITLE_MODEL_CACHE_TABLE_NAME = Variable.get("TITLE_MODEL_CACHE_TABLE_NAME")
TITLE_MODEL_SELECT_COLS = Variable.get("TITLE_MODEL_SELECT_COLS")
TITLE_MODEL_BATCH_LIMIT = Variable.get("TITLE_MODEL_BATCH_LIMIT")
TITLE_MODEL_MAX_FILE_SIZE = Variable.get("TITLE_MODEL_MAX_FILE_SIZE")
CONTENT_FLAG_IN_FILENAME = Variable.get("CONTENT_FLAG_IN_FILENAME")
CONTENT_FLAG_OUT_FILENAME = Variable.get("CONTENT_FLAG_OUT_FILENAME")
CONTENT_MODEL_INPUT_PATH = Variable.get("CONTENT_MODEL_INPUT_PATH")
CONTENT_MODEL_STAGE_NAME = Variable.get("CONTENT_MODEL_STAGE_NAME")
CONTENT_MODEL_CACHE_TABLE_NAME = Variable.get("CONTENT_MODEL_CACHE_TABLE_NAME")
CONTENT_MODEL_SELECT_COLS = Variable.get("CONTENT_MODEL_SELECT_COLS")
CONTENT_MODEL_BATCH_LIMIT = Variable.get("CONTENT_MODEL_BATCH_LIMIT")
CONTENT_MODEL_MAX_FILE_SIZE = Variable.get("CONTENT_MODEL_MAX_FILE_SIZE")
WEB_SCRAPPER_LAUNCH_TEMPLATE_ID = Variable.get("WEB_SCRAPPER_LAUNCH_TEMPLATE_ID")
AIML_DATABASE = Variable.get("AIML_DATABASE")

#------ Global Vars ------
dt = datetime.now()
date_time = dt.strftime("%m%d%Y%H:%M:%S")
DAG_NAME = 'Content_Intelligence'

#-----SNS Failure notification----

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="CONTENT_INTELLIGENCE DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="CONTENT_INTELLIGENCE DAG SUCCESS"
        ,message=f"{DAG_NAME} has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

dag = DAG(DAG_NAME, start_date = datetime(2022, 10, 29), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'on_failure_callback': on_failure_callback,'on_success_callback': None})

#-----Python Functions-----
def end_success():
  logger.info("DAG Ended Successfully.")

def s3_object_exists(bucket, key):
  s3 = boto3.resource('s3')

  try:
    s3.Object(bucket, key).load()
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        # The object does not exist.
        return False
    else:
        # Something else has gone wrong.
        raise
  else:
    # The object does exist.
    return True

def create_flag_file(name):
  with open(name, 'w') as f:
    f.write(name)
  return name

def delete_all_objects(bucket, prefix):
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
  if 'Contents' not in response:
    return
  for object in response['Contents']:
    s3.delete_object(Bucket=bucket, Key=object['Key'])

def s3_object_exists_prefix(bucket, prefix):
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
  if 'Contents' not in response:
    return False
  return True

def get_object_count(bucket, prefix):
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
  if 'Contents' in response:
    return len(response['Contents'])
  return 0

def conditional_model_start(flag_in, flag_out, input_data_path, stage_name, cache_table_name, select_cols, batch_limit, max_file_size):
  s3 = boto3.client('s3')
  if s3_object_exists_prefix(AIML_BUCKET, 'PRODUCTION/'+flag_out):
    #clear all objects from input s3 path
    delete_all_objects(AIML_BUCKET, input_data_path)
    #upload from snowflake cache into s3 input_data_path
    snow_hook = SnowflakeHook(snowflake_conn_id=LOAD_CONNECTION)
    snow_hook.run(f"copy into @{stage_name} from (select {select_cols} from {cache_table_name} limit {batch_limit}) max_file_size = {max_file_size} ;")
    #upload input flag file to s3
    if get_object_count(AIML_BUCKET, input_data_path) > 0:
      #delete flag out
      delete_all_objects(AIML_BUCKET, 'PRODUCTION/'+flag_out)
      #upload flag in
      s3.upload_file(create_flag_file(flag_in), AIML_BUCKET, 'PRODUCTION/'+flag_in)
      logger.info("Started a new batch process for model {}.".format(flag_in))
    else:
      logger.info("Not starting new batch process: no data to process after copy from Snowflake.")
  else:
    logger.info("No flagout detected for model {}; not starting new batch process.".format(flag_in))

#-----Snowflake queries----
#-----SCRAPER-------
scraper_out_query = [f"""
    copy into {AIML_DATABASE}.web_scraper.output_cache
    from @{AIML_DATABASE}.web_scraper.output
    pattern = '.*.json'
    purge = True;
"""]

scraper_results_cache_merge_query = [f"""
    merge into {AIML_DATABASE}.WEB_SCRAPER.RESULTS_CACHE t
    using (
        select
            result:url::varchar as page_url,
            any_value(result:statuscode::varchar) as outcome,
            any_value(result:title::varchar) as title,
            any_value(result:body::varchar) as content,
            any_value(result:contenttype::varchar) as content_type,
            any_value(result:image::varchar) as image,
            any_value(result:lang::varchar) as language,
            {AIML_DATABASE}.public.clean_string(any_value(result:body::varchar),1100) as CLEANED_CONTENT,
            {AIML_DATABASE}.public.clean_string(any_value(result:title::varchar),75) as CLEANED_TITLE
        from {AIML_DATABASE}.WEB_SCRAPER.OUTPUT_CACHE
        where result != 'null' and result is not null
        group by 1
    ) s
    on s.page_url = t.page_url
    when not matched then insert
    (page_url, result, title, content, last_scraped, content_type, image, language,CLEANED_CONTENT,CLEANED_TITLE)
    values
    (s.page_url, s.outcome, s.title, s.content, current_date, s.content_type, s.image, s.language,s.CLEANED_CONTENT,s.CLEANED_TITLE)
    when matched then update set
    title = s.title,
    result = s.outcome,
    content = s.content,
    last_scraped = current_date,
    content_type = s.content_type,
    image = s.image,
    language = s.language,
    CLEANED_CONTENT=s.CLEANED_CONTENT,
    CLEANED_TITLE=s.CLEANED_TITLE;
"""]

scraper_results_merge_query = [f"""
    merge into {AIML_DATABASE}.WEB_SCRAPER.RESULTS t
    using (
        select
            *
        from {AIML_DATABASE}.WEB_SCRAPER.RESULTS_CACHE
    ) s
    on s.page_url = t.page_url
    when not matched then insert
    (page_url, result, title, content, last_scraped, content_type, image, language,CLEANED_CONTENT,CLEANED_TITLE)
    values
    (s.page_url, s.result, s.title, s.content, s.last_scraped, s.content_type, s.image, s.language,s.CLEANED_CONTENT,CLEANED_TITLE)
    when matched then update set
    title = s.title,
    result = s.result,
    content = s.content,
    last_scraped = s.last_scraped,
    content_type = s.content_type,
    image = s.image,
    language = s.language,
    CLEANED_CONTENT=s.CLEANED_CONTENT,
    CLEANED_TITLE=s.CLEANED_TITLE;
"""]

prune_upload_scraper_input_cache_query = [f"""
 delete from {AIML_DATABASE}.web_scraper.input_cache
 where page_url in (select distinct result:url::varchar from {AIML_DATABASE}.WEB_SCRAPER.OUTPUT_CACHE)
 """,
 f"""
 create or replace transient table {AIML_DATABASE}.WEB_SCRAPER.UPLOAD_INCREMENT as
 select distinct page_url from (select * from {AIML_DATABASE}.WEB_SCRAPER.INPUT_CACHE
 where date_pushed is null
 order by date_inserted asc)
 limit 2000000;
 """,
 f"""
 copy into @{AIML_DATABASE}.WEB_SCRAPER.INPUT_CACHE/{date_time} from 
 {AIML_DATABASE}.WEB_SCRAPER.UPLOAD_INCREMENT;
 """,
 f"""
 merge into {AIML_DATABASE}.WEB_SCRAPER.INPUT_CACHE t
 using {AIML_DATABASE}.WEB_SCRAPER.UPLOAD_INCREMENT s
 on t.page_url = s.page_url
 when matched then update set
 t.date_pushed = current_date;
 """]

clear_scraper_cache_query = [f"""
  truncate table {AIML_DATABASE}.web_scraper.output_cache;
"""]

clear_scraper_results_cache = [f"""
DELETE FROM {AIML_DATABASE}.WEB_SCRAPER.RESULTS_CACHE
WHERE page_url IN (
    SELECT page_url FROM {AIML_DATABASE}.WEB_SCRAPER.RESULTS
);
"""]

#----TAGGING_BRANDS-----
generating_ngarms_query = [f"""merge into {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS a 
using(select distinct 
  page_url, 
  {AIML_DATABASE}.brands_identification.unique_ngrams(cleaned_title || ' ' || cleaned_content) as ngrams
from (select * from {AIML_DATABASE}.WEB_SCRAPER.RESULTS_CACHE where result != 'null' and result is not null  
    and result = '200'
    having len(cleaned_content)>1)) b
on a.page_url=b.page_url
when matched then update set
a.ngrams=b.ngrams
when not matched then
insert (page_url,ngrams)
values (b.page_url,b.ngrams)
""",
f"""create or replace table {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS_W_CATEGORIES as
select distinct 
  t.page_url, 
  t.ngrams,
  f.value:parentCategory::varchar as parent_category,
  f.value:category::varchar as category,
  f.value:topic::varchar as topic
from 
    (select a.page_url,
            ngrams,
            b.intent_topics
     from  {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS a
     join {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT b
     on a.page_url = b.page_url ) t,
lateral flatten(input=>t.intent_topics) f;
"""]

tagging_brands_ngrams_query = [f"""
create or replace table {AIML_DATABASE}.brands_identification.tagged_brands as
select distinct
    page_url,
    ngrams,
    a.parent_category,
    a.category,
    a.topic,
    {AIML_DATABASE}.brands_identification.arrays_intersect(brands, ngrams) as brand_intersection,
    {AIML_DATABASE}.brands_identification.arrays_intersect(domains, ngrams) as domain_intersection
from {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS_W_CATEGORIES a
join {AIML_DATABASE}.brands_identification.CATEGORY_TOPICS_BRANDS b
on a.parent_category = b.parent_category and a.category = b.category and a.topic=b.original_topic
where array_size(brand_intersection) > 0
or array_size(domain_intersection) > 0
""",
f"""
MERGE INTO {AIML_DATABASE}.BRANDS_IDENTIFICATION.OUTPUT AS target
using (
SELECT DISTINCT
  a.page_url,
  a.ngrams,
  ARRAY_AGG(DISTINCT e.VALUE) AS brand_topics
FROM (
  SELECT DISTINCT
    page_url,
    ngrams,
    parent_category,
    category,
    topic,
    IFF(array_size(BRAND_INTERSECTION) > 0, LOWER(REGEXP_REPLACE(f.VALUE, '^"|"$', '')), NULL) AS brand_intersection,
    IFF(array_size(DOMAIN_INTERSECTION) > 0, LOWER(REGEXP_REPLACE(t.VALUE, '^"|"$', '')), NULL) AS domain_intersection
  FROM
    {AIML_DATABASE}.BRANDS_IDENTIFICATION.TAGGED_BRANDS,
    LATERAL FLATTEN(input => BRAND_INTERSECTION, OUTER => TRUE) f,
    LATERAL FLATTEN(input => DOMAIN_INTERSECTION, OUTER => TRUE) t
) a
INNER JOIN {AIML_DATABASE}.BRANDS_IDENTIFICATION.REFERENCE_TOPICS b
  ON a.PARENT_CATEGORY = b.PARENT_CATEGORY
  AND a.CATEGORY = b.CATEGORY
  AND a.TOPIC = b.ORIGINAL_TOPIC
  AND LOWER(a.brand_intersection) = LOWER(b.BRAND)
  AND (
    a.brand_intersection IS NOT NULL
    OR
    a.domain_intersection = b.DOMAIN
  ),
  LATERAL FLATTEN(input => b.brand_topics) e
GROUP BY 1,2
) AS source
on target.page_url = source.page_url
WHEN MATCHED THEN
UPDATE SET target.brand_topics = ARRAY_DISTINCT(ARRAY_CAT(target.brand_topics, source.brand_topics)),
target.ngrams = source.ngrams,
target.date_classified=current_date
WHEN NOT MATCHED THEN
INSERT (page_url,ngrams,brand_topics,DATE_CLASSIFIED)
VALUES (source.page_url,source.ngrams,source.brand_topics,current_date)
""",
f"""
create or replace table {AIML_DATABASE}.BRANDS_IDENTIFICATION.CONTANUITY_TAGGED_CASE_INSENSITIVE as
WITH brands_and_domains AS (
SELECT distinct iff(len(brand)<4, null, brand) as brand, iff(len(domain)<4, null, domain) as  domain, brand_topics
FROM {AIML_DATABASE}.BRANDS_IDENTIFICATION.CONTANUITY_NOT_MATCHED
),
brands_array as 
(select 1 as indicator, array_agg(brand) as brands from brands_and_domains),
domains_array as 
(select 1 as indicator, array_agg(domain) as domains from brands_and_domains)
select distinct
    page_url,
    ngrams,
    {AIML_DATABASE}.brands_identification.arrays_intersect_case_insensitive(brands, ngrams) as brand_intersection,
    {AIML_DATABASE}.brands_identification.arrays_intersect_case_insensitive(domains, ngrams) as domain_intersection
from {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS
join brands_array b
on b.indicator = 1
join domains_array c
on c.indicator = 1
where array_size(brand_intersection) > 0
or array_size(domain_intersection) > 0
""",
f"""
MERGE INTO {AIML_DATABASE}.BRANDS_IDENTIFICATION.OUTPUT AS target
USING (
  SELECT DISTINCT
    a.page_url,
    a.ngrams,
    ARRAY_AGG(DISTINCT e.VALUE) AS brand_topics
  FROM (
    SELECT DISTINCT
      page_url,
      ngrams,
      IFF(array_size(BRAND_INTERSECTION) > 0, LOWER(REGEXP_REPLACE(f.VALUE, '^"|"$', '')), NULL) AS brand_intersection,
      IFF(array_size(DOMAIN_INTERSECTION) > 0, LOWER(REGEXP_REPLACE(t.VALUE, '^"|"$', '')), NULL) AS domain_intersection
    FROM
      {AIML_DATABASE}.BRANDS_IDENTIFICATION.CONTANUITY_TAGGED_CASE_INSENSITIVE,
      LATERAL FLATTEN(input => BRAND_INTERSECTION, OUTER => TRUE) f,
      LATERAL FLATTEN(input => DOMAIN_INTERSECTION, OUTER => TRUE) t
  ) a
  INNER JOIN {AIML_DATABASE}.BRANDS_IDENTIFICATION.REFERENCE_TOPICS b
    ON LOWER(a.brand_intersection) = LOWER(b.BRAND)
    AND (
      a.brand_intersection IS NOT NULL
      OR
      a.domain_intersection = b.DOMAIN
    ),
    LATERAL FLATTEN(input => b.brand_topics) e
  GROUP BY 1,2
) AS source
ON target.page_url = source.page_url
WHEN MATCHED THEN
UPDATE SET target.brand_topics = ARRAY_DISTINCT(ARRAY_CAT(target.brand_topics, source.brand_topics)),
target.ngrams = source.ngrams,
target.date_classified=current_date
WHEN NOT MATCHED THEN
INSERT (page_url,ngrams,brand_topics,DATE_CLASSIFIED)
VALUES (source.page_url,source.ngrams,source.brand_topics,current_date);
"""]

cleaning_brands_identification_query = [f"""
truncate table {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS_W_CATEGORIES
""",
f"""truncate table {AIML_DATABASE}.brands_identification.tagged_brands
""",
f"""truncate table {AIML_DATABASE}.BRANDS_IDENTIFICATION.CONTANUITY_TAGGED_CASE_INSENSITIVE
""",
f"""delete from  {AIML_DATABASE}.BRANDS_IDENTIFICATION.URL_NGRAMS
WHERE page_url IN (
    SELECT page_url FROM {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT
) or array_size(ngrams)=0;
"""]
                               
#----TITLE MODEL ------
title_model_input_cache_query = [f"""
  merge into {AIML_DATABASE}.TITLE_CLASSIFIER.INPUT_CACHE t
  using (
    select
      result:url::varchar as page_url,
      {AIML_DATABASE}.public.first_n_words_filtered(any_value(result:title::varchar),50) || ' ' || {AIML_DATABASE}.public.first_n_words_filtered(any_value(result:body::varchar),25) as title
    from {AIML_DATABASE}.WEB_SCRAPER.OUTPUT_CACHE a
    left join {AIML_DATABASE}.TITLE_CLASSIFIER.OUTPUT b
    on a.result:url::varchar = b.page_url
    where result != 'null' and result is not null
    and result:statuscode::varchar = '200'
    and date_classified is null
    group by 1
    having len(trim(title))>1
  ) s
  on s.page_url = t.page_url
  when not matched then insert
  (page_url, page_title, date_inserted)
  values
  (s.page_url, s.title, current_date)
  when matched then update set
  page_title = s.title,
  date_inserted = current_date;

"""]

title_out_query = [f"""
  copy into {AIML_DATABASE}."TITLE_CLASSIFIER"."OUTPUT_CACHE"
  from @dev_aiml.title_classifier.prod_output purge=True;
"""]

title_cache_to_cumulative_query = [f"""
  merge into {AIML_DATABASE}.TITLE_CLASSIFIER.OUTPUT t
  using 
  (select page_url,
          any_value(page_title) as page_title,
          any_value(payload) as payload
   from {AIML_DATABASE}.TITLE_CLASSIFIER.OUTPUT_CACHE
   where page_url is not null and len(page_url)>1 and not startswith(lower(page_title),'page not found')
   group by 1) s
  on t.page_url = s.page_url
  when matched then update set
  page_title = s.page_title,
  url_type = s.payload:url_type,
  activity_type = s.payload:activity_type,
  information_type = s.payload:information_type,
  topics = s.payload:topics,
  date_classified = current_date
  when not matched then insert
  (page_url, page_title, url_type, activity_type, information_type, topics, date_classified)
  values
  (s.page_url, 
   s.page_title, 
   s.payload:url_type,
   s.payload:activity_type,
   s.payload:information_type,
   s.payload:topics,
   current_date);
"""]

prune_title_model_input_cache_query = [f"""
delete from {AIML_DATABASE}.title_classifier.input_cache
where page_url in
(select distinct page_url from {AIML_DATABASE}.title_classifier.output_cache);
"""]

clear_title_model_output_cache_query = [f"""
truncate table {AIML_DATABASE}.title_classifier.output_cache;
"""]

#------ CONTENT/TAXONOMY MODEL ------
content_out_query = [f"""
copy into {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT_CACHE
from @dev_aiml.taxonomy_classifier.prod_output purge = True;
"""]

content_cache_to_cumulative_query = [f"""
merge into {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT t
using 
  (select
    page_url,
    any_value(content) as content,
    any_value(payload) as intent_topics
  from {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT_CACHE
  group by 1) s
on t.page_url = s.page_url
when not matched then insert
(page_url, content, intent_topics, date_classified)
values
(s.page_url, s.content, s.intent_topics, current_date)
when matched then update set
content = s.content,
intent_topics = s.intent_topics,
date_classified = current_date;
"""]

content_model_input_cache_query = [f"""
merge into {AIML_DATABASE}.taxonomy_classifier.input_cache t
using
  (select
      result:url::varchar as page_url,
      {AIML_DATABASE}.public.first_n_words_filtered(any_value(result:title::varchar),50) || ' ' || {AIML_DATABASE}.public.first_n_words_filtered(any_value(result:body::varchar),1000) as title_plus_content
    from {AIML_DATABASE}.WEB_SCRAPER.OUTPUT_CACHE a
    left join {AIML_DATABASE}.TAXONOMY_CLASSIFIER.OUTPUT b
    on a.result:url::varchar = b.page_url
    where result != 'null' and result is not null
    and result:statuscode::varchar = '200'
    and date_classified is null
    group by 1
    having len(trim(title_plus_content))>1) s
on t.page_url = s.page_url
when not matched then insert
(page_url, title_plus_content, date_inserted)
values
(s.page_url, s.title_plus_content, current_date)
when matched then update set
title_plus_content = s.title_plus_content,
date_inserted = current_date;
"""]

prune_content_model_input_cache_query = [f"""
delete from {AIML_DATABASE}.taxonomy_classifier.input_cache
where page_url in
(select distinct page_url from {AIML_DATABASE}.taxonomy_classifier.output_cache);
"""]

clear_content_model_output_cache_query = [f"""
truncate table {AIML_DATABASE}.taxonomy_classifier.output_cache;
"""]

#----CONTEXT INFERENCE QUERIES---
label_context_query = [f"""
merge into {AIML_DATABASE}.context_classifier.output t
using (
select 
    page_url,
    page_title,
    case when 
        page_title ilike any 
        ('%versus%','%vs%','%compared%','%review%','%comparison%') 
        then object_construct('review/comparison',1) 
        else object_construct('review/comparison',0) 
    end as context
    from (
    select
      result:url::varchar as page_url,
      any_value(result:title::varchar) || ' ' || {AIML_DATABASE}.public.first_n_words(any_value(result:body::varchar), 25) as page_title
    from {AIML_DATABASE}.WEB_SCRAPER.OUTPUT_CACHE
    where result != 'null' and result is not null
    and result:statuscode::varchar = '200'
    group by 1
    having len(page_title)>1)
    
  ) s
  on s.page_url = t.page_url
  when not matched then insert
  (page_url, page_title, context, date_classified)
  values
  (s.page_url, s.page_title, s.context, current_date)
  when matched then update set
  page_title = s.page_title,
  context = s.context,
  date_classified = current_date;
"""]

with dag:

  #------SCRAPER-------
  #load scraper output into output cache
  scraper_out_exec = SnowflakeOperator(
    task_id= "scraper_output_from_s3",
    sql= scraper_out_query,
    snowflake_conn_id= LOAD_CONNECTION, #replace with load connection
    retries=3
    )
  
  #merge output cache into cache results table
  scraper_results_cache_merge_exec = SnowflakeOperator(
    task_id= "merge_into_scraper_results_cache",
    sql= scraper_results_cache_merge_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )
    
  #merge results cache into results table
  scraper_results_merge_exec = SnowflakeOperator(
    task_id= "merge_into_scraper_results",
    sql= scraper_results_merge_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )
    
  #delete urls from scraper input cache present in scraper output cache
  prune_upload_scraper_input_cache_exec = SnowflakeOperator(
    task_id= "prune_upload_scraper_input_cache",
    sql= prune_upload_scraper_input_cache_query,
    snowflake_conn_id= LOAD_CONNECTION, #replace with load connection
    )

  scraper_start_exec = BashOperator(
    task_id = "launch-instance",
    depends_on_past=False,
    bash_command=f"aws ec2 run-instances --count 1 --launch-template LaunchTemplateId={WEB_SCRAPPER_LAUNCH_TEMPLATE_ID} --region us-east-2"
    ) 
  
  #clear the scraper output cache 
  clear_scraper_output_cache_exec = SnowflakeOperator(
    task_id= "clear_scraper_output_cache",
    sql= clear_scraper_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )
    
  #clear the scraper results cache 
  clear_scraper_results_cache_exec = SnowflakeOperator(
    task_id= "clear_scraper_results_cache",
    sql= clear_scraper_results_cache,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )
    
  #-----BRANDS IDENTIFICATION-----
  #generating ngrams from the scraper results_cache
  generating_ngarms_exec = SnowflakeOperator(
    task_id= "generating_ngarms_from_scraper_results_cache",
    sql= generating_ngarms_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    
  #tagging brands to ngrams and merging them to output
  tagging_brands_to_page_url_ngrams_exec = SnowflakeOperator(
    task_id= "tagging_brands_to_page_url_ngrams",
    sql= tagging_brands_ngrams_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
    
  #cleaning the brands_identification tables
  cleaning_brands_identification_exec = SnowflakeOperator(
    task_id= "cleaning_brands_identification",
    sql= cleaning_brands_identification_query,
    snowflake_conn_id= TRANSFORM_CONNECTION,
    )
  
  #-------TITLE MODEL -------
  #load from title model output cache
  title_out_exec = SnowflakeOperator(
    task_id= "title_model_output_from_s3",
    sql= title_out_query,
    snowflake_conn_id= LOAD_CONNECTION, #replace with load connection
    )

  #merge title model output cache into cumulative table
  title_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "title_model_output_cache_to_cumulative",
    sql= title_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with load connection
    )

  #merge into title model input cache
  title_model_input_cache_exec = SnowflakeOperator(
    task_id= "merge_into_title_model_input_cache",
    sql= title_model_input_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )

  #delete from title model input cache where url in title model output cache
  prune_title_model_input_cache_exec = SnowflakeOperator(
    task_id= "prune_title_model_input_cache",
    sql= prune_title_model_input_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )

  #title model conditional start
  title_model_conditional_start_exec = PythonOperator(
    task_id= "title_model_conditional_start",
    python_callable = conditional_model_start,
    op_args = [TITLE_FLAG_IN_FILENAME, 
              TITLE_FLAG_OUT_FILENAME, 
              TITLE_MODEL_INPUT_PATH, 
              TITLE_MODEL_STAGE_NAME, 
              TITLE_MODEL_CACHE_TABLE_NAME,
              TITLE_MODEL_SELECT_COLS, 
              TITLE_MODEL_BATCH_LIMIT, 
              TITLE_MODEL_MAX_FILE_SIZE]
    )

  #clear the title model output cache 
  clear_title_model_output_cache_exec = SnowflakeOperator(
    task_id= "clear_title_model_output_cache",
    sql= clear_title_model_output_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )

  #-------TAXONOMY/CONTENT MODEL ---------
  #pull content model results from s3
  content_out_exec = SnowflakeOperator(
    task_id= "content_model_output_from_s3",
    sql= content_out_query,
    snowflake_conn_id= LOAD_CONNECTION, #replace with transform connection
    )

  #merge content model output cache into cumulative table
  content_cache_to_cumulative_exec = SnowflakeOperator(
    task_id= "content_model_output_cache_to_cumulative",
    sql= content_cache_to_cumulative_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with load connection
    )
  
  #merge into content model input cache
  content_model_input_cache_exec = SnowflakeOperator(
    task_id= "merge_into_content_model_input_cache",
    sql= content_model_input_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )

  #delete from content model input cache where url in content model output cache
  prune_content_model_input_cache_exec = SnowflakeOperator(
    task_id= "prune_content_model_input_cache",
    sql= prune_content_model_input_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION, #replace with transform connection
    )

  #content model conditional start
  content_model_conditional_start_exec = PythonOperator(
    task_id= "content_model_conditional_start",
    python_callable = conditional_model_start,
    op_args = [CONTENT_FLAG_IN_FILENAME, 
              CONTENT_FLAG_OUT_FILENAME, 
              CONTENT_MODEL_INPUT_PATH,
              CONTENT_MODEL_STAGE_NAME, 
              CONTENT_MODEL_CACHE_TABLE_NAME,
              CONTENT_MODEL_SELECT_COLS, 
              CONTENT_MODEL_BATCH_LIMIT, 
              CONTENT_MODEL_MAX_FILE_SIZE]
    )

  #clear the content model output cache 
  clear_content_model_output_cache_exec = SnowflakeOperator(
    task_id= "clear_content_model_output_cache",
    sql= clear_content_model_output_cache_query,
    snowflake_conn_id= TRANSFORM_CONNECTION
    )

  #----CONTEXT LABELING STEPS ----
  label_context_exec = SnowflakeOperator(
    task_id= "label_url_context",
    sql= label_context_query,
    snowflake_conn_id= TRANSFORM_CONNECTION
    )
  #----OTHER STEPS----
  end_success_exec = PythonOperator(
    task_id= "end_success",
    python_callable = end_success,
    on_success_callback = on_success_callback
    )


  #--MAIN FLOW ---
  #scraper out to start
  scraper_out_exec >> prune_upload_scraper_input_cache_exec >> scraper_start_exec
  #brands identification chain 
  scraper_results_cache_merge_exec >> generating_ngarms_exec >> tagging_brands_to_page_url_ngrams_exec
  #title model chain
  scraper_out_exec >> title_out_exec >> title_model_input_cache_exec >> prune_title_model_input_cache_exec >> title_model_conditional_start_exec
  #content model chain
  scraper_out_exec >> content_out_exec >> content_model_input_cache_exec >> prune_content_model_input_cache_exec >> content_model_conditional_start_exec
  #context model chain
  scraper_out_exec >> label_context_exec
  #---Cache to cumulative---
  scraper_out_exec >> scraper_results_cache_merge_exec >> scraper_results_merge_exec
  title_out_exec >> title_cache_to_cumulative_exec
  content_out_exec >> content_cache_to_cumulative_exec

  #--Cache deletions---
  [scraper_results_cache_merge_exec,scraper_results_merge_exec,prune_upload_scraper_input_cache_exec, 
  title_model_input_cache_exec, content_model_input_cache_exec, label_context_exec] >> clear_scraper_output_cache_exec
  [clear_scraper_output_cache_exec, generating_ngarms_exec] >> clear_scraper_results_cache_exec
  [title_cache_to_cumulative_exec, prune_title_model_input_cache_exec] >> clear_title_model_output_cache_exec
  [content_cache_to_cumulative_exec, prune_content_model_input_cache_exec] >> clear_content_model_output_cache_exec
  [generating_ngarms_exec,tagging_brands_to_page_url_ngrams_exec] >> cleaning_brands_identification_exec

  #--Success Condition---
  [scraper_start_exec, title_model_conditional_start_exec, content_model_conditional_start_exec,
  clear_scraper_output_cache_exec, clear_title_model_output_cache_exec, clear_content_model_output_cache_exec,tagging_brands_to_page_url_ngrams_exec] >> end_success_exec

