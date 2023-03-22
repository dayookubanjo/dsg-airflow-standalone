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
TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
SNS_ARN=Variable.get("SNS_ARN")
DATAMART_DATABASE = Variable.get("DATAMART_DATABASE") 
FIVE_BY_FIVE_TABLE = Variable.get("FIVE_BY_FIVE_TABLE") 

# ---- error handling ----
def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="FIVE_BY_FIVE DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="FIVE_BY_FIVE DAG SUCCESS"
        ,message=f"Five by Five merge DAG has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

#---- Python definitions ------

def end_success():
  logger.info("DAG Ended Successfully.")

# Snowflake Queries

merge_devmart_domain_observations_query = [
   f"""MERGE INTO {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN_OBSERVATIONS as target_table
USING 

(SELECT DISTINCT A.*, 'FIVE BY FIVE' AS SOURCE FROM  {FIVE_BY_FIVE_TABLE} AS A) as source_table

ON (source_table.IP_ADDRESS = target_table.IP AND source_table.SOURCE = target_table.SOURCE)
WHEN MATCHED THEN
    UPDATE SET 
      
    target_table.DATE = current_date(),
    target_table.NORMALIZED_COMPANY_DOMAIN =   source_table.COMPANY_DOMAIN,
    target_table.SOURCE_CONFIDENCE =    source_table.DISTRIBUTION_PERCENTAGE   
WHEN NOT MATCHED THEN
    INSERT (IP, NORMALIZED_COMPANY_DOMAIN, SOURCE, SOURCE_CONFIDENCE, DATE )
    VALUES(source_table.IP_ADDRESS,
           source_table.COMPANY_DOMAIN,
           'FIVE BY FIVE',
            source_table.DISTRIBUTION_PERCENTAGE   ,
          current_date() 
          );"""
    ]

create_devmart_domain_query = [
    f"""create or replace table {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN as
with most_recent as(
    select distinct
    ip,
    first_value(normalized_company_domain) over (partition by ip, source order by date desc) as latest_domain,
    source,
    source_confidence
from {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN_OBSERVATIONS
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
from rolled_obs;"""
    ]

with DAG(
    'Five-By-Five-IP-to-Domain-Merge',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': None
    },
    description = 'Merges Five By Five data with Devmart IP to Domain table',
    schedule_interval = '@weekly', # '0 0 * * SAT'
    start_date = datetime(2023, 2, 5),
    catchup=False,
    tags=['Five By Five', 'Intent'], 
    on_failure_callback=on_failure_callback, 
    on_success_callback=None,
    
) as dag:

    merge_devmart_domain_observations_exec = SnowflakeOperator(
        task_id= "merge_devmart_domain_observations",
        sql= merge_devmart_domain_observations_query,
        snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    create_devmart_domain_exec = SnowflakeOperator(
        task_id= "create_devmart_domain",
        sql= create_devmart_domain_query,
        snowflake_conn_id= TRANSFORM_CONNECTION,
    )

    end_success_exec = PythonOperator(
        task_id= "end_success",
        python_callable = end_success,
        on_success_callback = on_success_callback
        ) 

    merge_devmart_domain_observations_exec >> create_devmart_domain_exec >> end_success_exec