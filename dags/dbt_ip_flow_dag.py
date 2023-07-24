#------ standard python imports ----
from datetime import date, datetime
import logging
import requests
from requests.exceptions import ConnectionError
import snowflake.connector
import time

#--------- aws imports -------
import boto3

#--- airflow imports -------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

PATH_TO_DBT_PROJECT = "/opt/airflow/dbt/dbt_dsg_app" 

# ---- Global variables ----
SNOWFLAKE_TRANSFORM_CONNECTION = Variable.get("TRANSFORM_CONNECTION")
SNS_ARN=Variable.get("SNS_ARN")
SNOWFLAKE_USER=Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD=Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT=Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE=Variable.get("SNOWFLAKE_WAREHOUSE")
IP_FLOW_DATABASE=Variable.get("IP_FLOW_DATABASE")
IP_FLOW_TOKEN=Variable.get("IP_FLOW_TOKEN")
BIDSTREAM_DATABASE=Variable.get("BIDSTREAM_DATABASE")
DATAMART_DATABASE=Variable.get("DATAMART_DATABASE")
FIVE_BY_FIVE_TABLE=Variable.get("FIVE_BY_FIVE_TABLE")
ENV=Variable.get("ENV")
VAR_DICT = {}
VAR_DICT['env']= ENV

"{'env': {{ENV}}}"
# today's date
today = date.today()


def ip_api_search():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=IP_FLOW_DATABASE,
        schema='RAW_DATA'
    )
    cs = ctx.cursor()

    # Retrieve the data from the table
    cs.execute(f"SELECT * FROM DEV_QA.DBT_POC.STG_IPFLOW_INPUT_DATA")

    rows = cs.fetchall()

    # print("rows:", rows)


    # Set the request headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {IP_FLOW_TOKEN}"
    }

    # Send the request and save the response for each row in the table
    for row in rows:
        ip_address = row[0]
        time.sleep(0.01)
        
        # print("row:", row)
        # print("row[0]:", row[0])
        # print('data:', data)

        try:
            response = requests.get(f"https://api.ipflow.com/v1.0/search/ipaddress/{ip_address}", headers=headers)

            # print('response code:', response.status_code)

            # Insert the response data into the table
            cs.execute(f"INSERT INTO {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_OUTPUT_DATA (USER_IP, LAST_RESPONSE_CODE, LAST_QUERY_DATE, API_RESPONSE) VALUES (%s, %s, %s, %s)", 
            [ip_address, response.status_code, today, response.text])
        except ConnectionError as err:
            time.sleep(10)

    ctx.commit()
    cs.close()
    ctx.close()

# Cleanup tables

snowflake_cleanup_tables_query = [
    f"""truncate table DEV_QA.DBT_POC.STG_IPFLOW_INPUT_DATA;""",
    f"""truncate table {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_OUTPUT_DATA;""",
    f"""truncate table DEV_QA.DBT_POC.stg_ipflow_success_data;""",
    f"""truncate table DEV_QA.DBT_POC.src_ipflow_normalized_location;"""
]


with DAG(
    'dbt_ipflow_dag',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        # 'on_failure_callback': on_failure_callback,
        # 'on_success_callback': None
    },
    description = 'Run dbt commands',
    schedule_interval = '@daily',
    start_date = datetime(2023, 1, 13),
    catchup=False,
    tags=['IP-Flow', 'Intent'], 
    # on_failure_callback=on_failure_callback, 
    # on_success_callback=None,
    
) as dag:

    get_input_data = BashOperator(
        task_id="get_input_data",
        bash_command=f"dbt run --select stg_ipflow_input_data --profiles-dir {PATH_TO_DBT_PROJECT} --project-dir {PATH_TO_DBT_PROJECT}  --target {ENV} --vars '{VAR_DICT}' ",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    test_for_duplicate = BashOperator(
        task_id="test_for_duplicate",
        bash_command=f"dbt test --profiles-dir {PATH_TO_DBT_PROJECT} --project-dir {PATH_TO_DBT_PROJECT}  --target {ENV} --vars '{VAR_DICT}' ",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    send_get_requests = PythonOperator(
        task_id = "send_get_requests",
        python_callable=ip_api_search 
        
    )

    run_success_outputs_workflow = BashOperator(
        task_id="run_success_outputs_workflow",
        bash_command=f"dbt run --select stg_ipflow_success_data+ --profiles-dir {PATH_TO_DBT_PROJECT} --project-dir {PATH_TO_DBT_PROJECT}  --target {ENV} --vars '{VAR_DICT}' ",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    run_full_outputs_workflow = BashOperator(
        task_id="run_full_outputs_workflow",
        bash_command=f"dbt run --select src_ipflow_merge_output+ --profiles-dir {PATH_TO_DBT_PROJECT} --project-dir {PATH_TO_DBT_PROJECT}  --target {ENV} --vars '{VAR_DICT}' ",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    snowflake_cleanup_tables_exec = SnowflakeOperator(
        task_id= "cleanup_tables",
        sql= snowflake_cleanup_tables_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )



    get_input_data >> test_for_duplicate >> send_get_requests >> [run_success_outputs_workflow , run_full_outputs_workflow]  
    [run_success_outputs_workflow , run_full_outputs_workflow] >> snowflake_cleanup_tables_exec