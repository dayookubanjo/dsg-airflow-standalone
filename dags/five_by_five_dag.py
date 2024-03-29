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
(with top_domains as (
    select distinct
        ip_address as ip,
        dev_datamart.public.domain_normalizer(company_domain) as normalized_company_domain,
        distribution_percentage as source_confidence
    from {FIVE_BY_FIVE_TABLE}
    where normalized_company_domain is not null and normalized_company_domain!='Shared' and len(normalized_company_domain)>1),
 secondary_domains as (
 select distinct
 t.ip_address as ip,
 dev_datamart.public.domain_normalizer(f.value) as normalized_company_domain,
 t.distribution_percentage as source_confidence
 from {FIVE_BY_FIVE_TABLE} t,
 lateral split_to_table(t.company_domain_array,',') f
 where normalized_company_domain is not null and normalized_company_domain!='Shared' and len(normalized_company_domain)>1)
   
  SELECT distinct
    ip,
    normalized_company_domain,
    source_confidence
    from top_domains
    union 
    select * from
    secondary_domains) as source_table
ON (source_table.IP = target_table.IP 
    AND source_table.normalized_company_domain = target_table.normalized_company_domain 
    and target_table.SOURCE = 'FIVE BY FIVE'
    and target_table.date = current_date) 
WHEN NOT MATCHED THEN
    INSERT (IP, NORMALIZED_COMPANY_DOMAIN, SOURCE, SOURCE_CONFIDENCE, DATE )
    VALUES(source_table.IP,
           source_table.normalized_COMPANY_DOMAIN,
           'FIVE BY FIVE',
            source_table.source_confidence,
          current_date
          );"""
    ]

create_devmart_domain_query = [
    f"""create or replace table {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN as
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
    1.0-(0.01*(current_date - date)) as recency_score,
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
    greatest(0.1, (CASE WHEN score_total=0 THEN 0 ELSE score/score_total END) ) as score 
from map_scores a
join score_totals b
on a.ip = b.ip;"""
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
