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
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# ---- Global variables ----
SNOWFLAKE_TRANSFORM_CONNECTION = "Airflow-Dev_Transform-connection"  

# ---- error handling ----
def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:five-by-five-dag"
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:five-by-five-dag"
        ,subject="DAG Success"
        ,message=f"Five by Five merge DAG has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

#---- Python definitions ------

def end_success():
  logger.info("DAG Ended Successfully.")

# Snowflake Queries

merge_devmart_domain_observations_query = [
    """MERGE INTO DEV_DATAMART.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN_OBSERVATIONS as target_table
USING 

(SELECT DISTINCT A.*, 'FIVE BY FIVE' AS SOURCE FROM  FIVE_BY_FIVE_DEMO.PRODUCTS.IP_COMPANY_2_6_0 AS A) as source_table

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

# merge_devmart_domain_query = [
#     """MERGE INTO DEV_DATAMART.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN as target_table
# USING FIVE_BY_FIVE_DEMO.PRODUCTS.IP_COMPANY_2_6_0 as source_table
# ON (source_table.IP_ADDRESS = target_table.IP)
# WHEN MATCHED THEN
#     UPDATE SET 
      
#     target_table.DATE_UPDATED = current_date(),
#     target_table.NORMALIZED_COMPANY_DOMAIN =   source_table.COMPANY_DOMAIN
# WHEN NOT MATCHED THEN
#     INSERT (IP,DATE_UPDATED,NORMALIZED_COMPANY_DOMAIN)
#     VALUES(source_table.IP_ADDRESS,
#           current_date(),
#           source_table.COMPANY_DOMAIN);"""
#     ]

with DAG(
    'Five-By-Five-IP-to-Domain-Merge',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': None
    },
    description = 'Merges Five By Five data with Devmart IP to Domain table',
    schedule_interval = '0 0 * * SAT',
    start_date = datetime(2023, 2, 5),
    catchup=False,
    tags=['Five By Five', 'Intent'], 
    on_failure_callback=on_failure_callback, 
    on_success_callback=None,
    
) as dag:

    merge_devmart_domain_observations_exec = SnowflakeOperator(
        task_id= "merge_devmart_domain_observations",
        sql= merge_devmart_domain_observations_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )

    # merge_devmart_domain_exec = SnowflakeOperator(
    #     task_id= "merge_devmart_domain",
    #     sql= merge_devmart_domain_query,
    #     snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    # )

    end_success_exec = PythonOperator(
        task_id= "end_success",
        python_callable = end_success,
        on_success_callback = on_success_callback
        ) 

    merge_devmart_domain_observations_exec >> end_success_exec