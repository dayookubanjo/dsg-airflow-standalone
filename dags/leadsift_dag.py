#------ standard python imports ----
from datetime import datetime
import logging 

#--------- aws imports -------
import boto3

#--- airflow imports -------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----Importing Variables----
SNS_ARN=Variable.get("SNS_ARN")
LOAD_CONNECTION = Variable.get("LOAD_CONNECTION")
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
LEADSIFT_DATABASE = Variable.get("LEADSIFT_DATABASE")

# ---- error handling ----
def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="LEADSIFT DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="LEADSIFT DAG SUCCESS"
        ,message=f"LeadSift ingestion DAG has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

#---- Python definitions ------

def end_success():
  logger.info("DAG Ended Successfully.")

# Snowflake Queries

load_data_from_s3_query = [
f"""COPY INTO {LEADSIFT_DATABASE}.raw_data.leadsift_flat_files_cache FROM @{LEADSIFT_DATABASE}.raw_data.leadsift_flat_files_cache ON_ERROR = 'SKIP_FILE';"""
    ]

merge_cache_to_output_query = [
    f"""MERGE INTO {LEADSIFT_DATABASE}.raw_data.leadsift_flat_files as target_table
USING 
(
  SELECT FILE_DATE,COALESCE(CATEGORY, 'NULL VALUE') AS CATEGORY,COALESCE(DOMAIN, 'NULL VALUE') AS DOMAIN,COALESCE(COMPANY, 'NULL VALUE') AS COMPANY,COALESCE(ENTITY_TRIGGER, 'NULL VALUE') AS ENTITY_TRIGGER,COALESCE(SENIORITY, 'NULL VALUE') AS SENIORITY,COALESCE(ENTITY_FUNCTION, 'NULL VALUE') AS ENTITY_FUNCTION,
   COALESCE(TRIGGER_TYPE, 'NULL VALUE') AS TRIGGER_TYPE,COALESCE(INDUSTRY, 'NULL VALUE') AS INDUSTRY,COALESCE(COMPANY_SIZE, 'NULL VALUE') AS COMPANY_SIZE,COALESCE(CITY, 'NULL VALUE') AS CITY,COALESCE(STATE, 'NULL VALUE') AS STATE,COALESCE(COUNTRY, 'NULL VALUE') AS COUNTRY,  MAX( COALESCE(SCORE, 0) ) AS SCORE
  FROM {LEADSIFT_DATABASE}.raw_data.leadsift_flat_files_cache 
  GROUP BY FILE_DATE,COALESCE(CATEGORY, 'NULL VALUE')  ,COALESCE(DOMAIN, 'NULL VALUE')  ,COALESCE(COMPANY, 'NULL VALUE')  ,COALESCE(ENTITY_TRIGGER, 'NULL VALUE') ,COALESCE(SENIORITY, 'NULL VALUE') ,COALESCE(ENTITY_FUNCTION, 'NULL VALUE')  ,
   COALESCE(TRIGGER_TYPE, 'NULL VALUE')  ,COALESCE(INDUSTRY, 'NULL VALUE')  ,COALESCE(COMPANY_SIZE, 'NULL VALUE') ,COALESCE(CITY, 'NULL VALUE') ,COALESCE(STATE, 'NULL VALUE')  ,COALESCE(COUNTRY, 'NULL VALUE') 
  
)  as source_table
ON (
  source_table.FILE_DATE = target_table.FILE_DATE
AND COALESCE(source_table.CATEGORY, 'NULL VALUE') = COALESCE(target_table.CATEGORY, 'NULL VALUE')
AND COALESCE(source_table.DOMAIN, 'NULL VALUE') = COALESCE(target_table.DOMAIN, 'NULL VALUE')
AND COALESCE(source_table.COMPANY, 'NULL VALUE') = COALESCE(target_table.COMPANY, 'NULL VALUE')
AND COALESCE(source_table.ENTITY_TRIGGER, 'NULL VALUE') = COALESCE(target_table.ENTITY_TRIGGER, 'NULL VALUE')
AND COALESCE(source_table.SENIORITY, 'NULL VALUE') = COALESCE(target_table.SENIORITY, 'NULL VALUE')
AND COALESCE(source_table.ENTITY_FUNCTION, 'NULL VALUE') = COALESCE(target_table.ENTITY_FUNCTION, 'NULL VALUE')
AND COALESCE(source_table.TRIGGER_TYPE, 'NULL VALUE') = COALESCE(target_table.TRIGGER_TYPE, 'NULL VALUE')
AND COALESCE(source_table.INDUSTRY, 'NULL VALUE') = COALESCE(target_table.INDUSTRY, 'NULL VALUE')
AND COALESCE(source_table.COMPANY_SIZE, 'NULL VALUE') = COALESCE(target_table.COMPANY_SIZE, 'NULL VALUE')
AND COALESCE(source_table.CITY, 'NULL VALUE') = COALESCE(target_table.CITY, 'NULL VALUE')
AND COALESCE(source_table.STATE, 'NULL VALUE') = COALESCE(target_table.STATE, 'NULL VALUE')
AND COALESCE(source_table.COUNTRY, 'NULL VALUE') = COALESCE(target_table.COUNTRY, 'NULL VALUE')
AND  COALESCE(source_table.SCORE, 0) =  COALESCE(target_table.SCORE, 0) 
  )
    
WHEN NOT MATCHED THEN
    INSERT (FILE_DATE,CATEGORY,DOMAIN,COMPANY,ENTITY_TRIGGER,SENIORITY,ENTITY_FUNCTION,TRIGGER_TYPE,INDUSTRY,COMPANY_SIZE,CITY,STATE,COUNTRY,SCORE)
    
    VALUES(source_table.FILE_DATE,
            source_table.CATEGORY,
          source_table.DOMAIN,
          source_table.COMPANY,
           source_table.ENTITY_TRIGGER,
           source_table.SENIORITY,
           source_table.ENTITY_FUNCTION,
           source_table.TRIGGER_TYPE,
           source_table.INDUSTRY,
           source_table.COMPANY_SIZE,
           source_table.CITY,
           source_table.STATE,
           source_table.COUNTRY,
           source_table.SCORE);"""
    ]

cleanup_tables_query = [
    f"""TRUNCATE TABLE {LEADSIFT_DATABASE}.raw_data.leadsift_flat_files_cache;""" 
]

prescoring_query = [
    f"""
    create or replace table {LEADSIFT_DATABASE}.activity.prescoring as
    select
        file_date as date,
        ds_parent_category as parent_category,
        ds_category as category,
        ds_topic as topic,
        dev_datamart.public.domain_normalizer(domain) as normalized_company_domain,
        max(score) as leadsift_score
    from {LEADSIFT_DATABASE}.RAW_DATA.LEADSIFT_FLAT_FILES a
    join {LEADSIFT_DATABASE}.PUBLIC.TOPIC_MAPPINGS b
    on a.entity_trigger = b.leadsift_category
    group by 1,2,3,4,5;
    """
]

#----- begin DAG definition -------
with DAG(
    'LeadSift-Ingestion',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': None
    },
    description = 'Ingests LeadSift data from s3 into Snowflake',
    schedule_interval = '@weekly',
    start_date = datetime(2023, 2, 5),
    catchup=False,
    tags=['LeadSift', 'Intent'], 
    on_failure_callback=on_failure_callback, 
    on_success_callback=None,
    
) as dag:

    load_data_from_s3_exec = SnowflakeOperator(
        task_id= "load_data_from_s3",
        sql= load_data_from_s3_query,
        snowflake_conn_id= LOAD_CONNECTION,
    )

    merge_cache_to_output_exec = SnowflakeOperator(
        task_id= "merge_cache_to_output",
        sql= merge_cache_to_output_query,
        snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    cleanup_tables_exec = SnowflakeOperator(
        task_id= "cleanup_tables",
        sql= cleanup_tables_query,
        snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    prescoring_exec = SnowflakeOperator(
        task_id= "prescoring",
        sql= prescoring_query,
        snowflake_conn_id= TRANSFORM_CONNECTION,
    )    

    end_success_exec = PythonOperator(
        task_id= "end_success",
        python_callable = end_success,
        on_success_callback = on_success_callback
        ) 

    load_data_from_s3_exec >> merge_cache_to_output_exec >> cleanup_tables_exec >> prescoring_exec >> end_success_exec