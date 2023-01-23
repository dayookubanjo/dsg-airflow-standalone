import logging
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

#-----SNS Failure notification----

def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Pixel_data_processing"
        ,subject="DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn="arn:aws:sns:us-east-2:698085094823:Pixel_data_processing"
        ,subject="DAG Success"
        ,message=f"Pixel Data Processing has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

dag = DAG('Pixel_Data_Processing', start_date = datetime(2022, 10, 29), schedule_interval = '@daily', catchup=False, on_failure_callback=on_failure_callback, on_success_callback=None,
        default_args={'on_failure_callback': on_failure_callback,'on_success_callback': None})

#-----Snowflake queries----

copy_query = ["copy into dev_pixel.raw_data.raw_pixel_data from @dev_pixel.raw_data.raw_pixel_data purge=True;"]

merge_insert = ["""
merge into dev_pixel.activity.user_activity t
using (
select
    pixel_record:"userId"::varchar as client_id,
    pixel_record:"ip"::varchar as user_ip,
    date(dev_pixel.public.date_normalizer(pixel_record:"date"),'dd-mm-yyyy') as date,
    pixel_record:"userAgent"::varchar as user_agent,
    pixel_record:"thirdPartyId"::varchar as site_url,
    count(*) as pageviews
from DEV_PIXEL.RAW_DATA.RAW_PIXEL_DATA
group by 1,2,3,4,5) s
on t.user_ip = s.user_ip 
and t.date = s.date
and t.user_agent = s.user_agent
and t.site_url = s.site_url
and t.client_id = s.client_id
when matched then update set
pageviews = s.pageviews + t.pageviews
when not matched then insert
(client_id, user_ip, date, user_agent, site_url, pageviews)
values
(s.client_id, s.user_ip, s.date, s.user_agent, s.site_url, s.pageviews);
"""]

clear_raw_data_cache = ["""
truncate table dev_pixel.raw_data.raw_pixel_data;
"""]


with dag:
  copy_query_exec = SnowflakeOperator(
    task_id= "load_from_s3",
    sql= copy_query,
    snowflake_conn_id= "Airflow-Dev_load-connection",
    )
  
  merge_insert_exec = SnowflakeOperator(
    task_id= "merge_into_user_activity",
    sql= merge_insert,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )

  clear_raw_data_cache_exec = SnowflakeOperator(
    task_id= "clear_raw_data_cache",
    on_success_callback=on_success_callback,
    sql= clear_raw_data_cache,
    snowflake_conn_id= "Airflow-Dev_Transform-connection",
    )

  copy_query_exec >> merge_insert_exec >> clear_raw_data_cache_exec
