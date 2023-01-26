#------ standard python imports ----
from datetime import datetime 
import logging
import time
from typing import List

#--------- aws imports -------
import boto3

#--- airflow imports -------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor 
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
    

# ---- Global variables ----
SNOWFLAKE_LOAD_CONNECTION = "Airflow-Dev_load-connection"
SNOWFLAKE_TRANSFORM_CONNECTION = "Airflow-Dev_Transform-connection" 
EC2_WAITTIME = 3*60
POKE_INTERVAL = 10
POKE_TIMEOUT = 25*60
FILE_SIZE_IN_BYTES = 2.5 * 1000 * 1048576

# ---- error handling ----
def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:EC2_Notification"
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

def on_timeout_callback(context):
    op = SnsPublishOperator(
        task_id="dag_timeout"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:EC2_Notification"
        ,subject="DAG TIMED OUT"
        ,message=f"Task has timed out as there's no data to process, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:EC2_Notification"
        ,subject="DAG Success"
        ,message=f"Digital Element ingestion DAG has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

#---- Python definitions ------

def wait_n_seconds(n):
    time.sleep(n)

def check_fn(files: List) -> bool:
    return any(f.get('Size', 0) >= FILE_SIZE_IN_BYTES  for f in files)

# ---- Query definitions ------
snowflake_ingestion_query = [
    """truncate table "DEV_DIGITAL_ELEMENT"."RAW_DATA"."DE_FLAT_FILE";""",
    """copy into "DEV_DIGITAL_ELEMENT"."RAW_DATA"."DE_FLAT_FILE" from @dev_digital_element.raw_data.dev_digital_element purge=TRUE;"""
    ]

data_cleaning_query = [
    """create or replace table dev_digital_element.mappings.ip_range_mappings_filtered as
select
    current_date as date_updated,
    ip_range_start,
    dev_digital_element.public.ip_to_number(ip_range_start) as ip_range_start_numeric,
    ip_range_end,
    dev_digital_element.public.ip_to_number(ip_range_end) as ip_range_end_numeric,
    trim(upper(pulseplus_country)) as normalized_country_code,
    trim(upper(pulseplus_region)) as normalized_region_code,
    dev_digital_element.public.normalize_city_de(pulseplus_city) as normalized_city_name,
    pulseplus_metro_code as metro_code,
    try_to_number(pulseplus_postal_code) as normalized_zip,
    pulseplus_country_conf as country_confidence,
    pulseplus_region_conf as region_confidence,
    pulseplus_city_conf as city_confidence,
    pulseplus_postal_conf as postal_confidence,
    isp_name,
    homebiz_type,
    company_name,
    dev_datamart.public.domain_normalizer(domain_name) as normalized_company_domain
from "DEV_DIGITAL_ELEMENT"."RAW_DATA"."DE_FLAT_FILE"
where not ((company_name is null or len(company_name)<2) and (domain_name is null or len(domain_name)<2))
and normalized_country_code in ('USA', 'CAN', 'GBR', 'DEU', 'SWE', 'NLD', 'NOR', 'FRA', 'ESP')
and not startswith(normalized_company_domain, 'ip-');"""
    ]

#----- begin DAG definition -------
with DAG(
    'Digital-Element-Ingestion',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': None
    },
    description = 'Starts EC2 instance from launch template that runs an ingestion script.',
    schedule_interval = '@daily',
    start_date = datetime(2022, 10, 1),
    catchup=False,
    tags=['Digital-Element', 'EC2', 'Intent'], 
    on_failure_callback=on_failure_callback, 
    on_success_callback=None,
    
) as dag:
    
    
    launch_instance_exec = BashOperator(
        task_id = "launch-instance",
        depends_on_past=False,
        bash_command="aws ec2 run-instances --count 1 --launch-template LaunchTemplateId=lt-016744221119eb24c "
    )
    
    launch_instance_exec.doc = "This task launches an EC2 instance according to the DE-related launch template. Uses the default version of the launch template."

    check_data_ingestion_status = S3KeySensor(
        task_id='check_data_ingestion_status',
        bucket_name='dev-intent-digital-element',
        bucket_key='flat-files/de_out.txt.gz',
        aws_conn_id='aws_default',
        check_fn=check_fn,
        mode='poke',
        poke_interval=POKE_INTERVAL,
        timeout=POKE_TIMEOUT,
        on_failure_callback=on_timeout_callback
    )

    wait_n_exec = PythonOperator(
        task_id = "wait_{}_seconds".format(EC2_WAITTIME),
        python_callable=wait_n_seconds, 
        op_args=[EC2_WAITTIME]
        
    )

    snowflake_ingestion_exec = SnowflakeOperator(
        task_id= "load_from_s3",
        sql= snowflake_ingestion_query,
        snowflake_conn_id= SNOWFLAKE_LOAD_CONNECTION,
    )

    data_cleaning_exec = SnowflakeOperator(
        task_id= "clean_ip_range_mappings",
        sql= data_cleaning_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
        on_success_callback=on_success_callback
    )

    
    launch_instance_exec >> check_data_ingestion_status >> wait_n_exec >> snowflake_ingestion_exec >> data_cleaning_exec