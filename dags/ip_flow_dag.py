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
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.models import Variable
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

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
# today's date
today = date.today()

# ---- error handling ----
def on_failure_callback(context):
    op = SnsPublishOperator(
        task_id="dag_failure"
        ,target_arn=SNS_ARN
        ,subject="IP FLOW DAG FAILED"
        ,message=f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    op.execute(context)

#-----SNS Success notification----
    
def on_success_callback(context):
    op = SnsPublishOperator(
        task_id="dag_success"
        ,target_arn=SNS_ARN
        ,subject="IP FLOW DAG Success"
        ,message=f"IP Flow ingestion DAG has succeeded, run_id: {context['run_id']}"
    )
    op.execute(context)

#---- Python definitions ------

def end_success():
  logger.info("DAG Ended Successfully.")

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
    cs.execute(f"SELECT * FROM {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_INPUT_DATA")

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


# ---- Query definitions ------


snowflake_insert_input_data_query = [
    f"""insert into {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_INPUT_DATA
with mapped_ips as (
select  DISTINCT BD.USER_IP 
 
FROM {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY AS BD 
INNER JOIN {FIVE_BY_FIVE_TABLE} AS FBF 
ON IP_ADDRESS =  USER_IP

UNION 

select  DISTINCT BD.USER_IP 
FROM {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY AS BD 
INNER JOIN {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_DATA AS IPF 
ON BD.USER_IP =  IPF.USER_IP
WHERE IPF.LAST_RESPONSE_CODE = 200
)
, unmapped_ips as (
select DISTINCT BD.USER_IP 
FROM {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY AS BD 
where BD.USER_IP not in (select user_ip from mapped_ips)
) ,
unmapped_ips_frequency as (
select a.USER_IP, count(b.PAGE_URL) as frequency from unmapped_ips as a 
left join {BIDSTREAM_DATABASE}.ACTIVITY.USER_ACTIVITY b 
on a.USER_IP = b.USER_IP
group by a.USER_IP
),
unmapped_ips_ranked as (
select a.*, row_number() over (order by frequency desc) as row_number_ranked
  from unmapped_ips_frequency as a
),
unmapped_ips_zero_deprioritized as (
select USER_IP from unmapped_ips_ranked where SPLIT_PART(USER_IP, '.', 4) <> 0
union
select USER_IP from unmapped_ips_ranked where SPLIT_PART(USER_IP, '.', 4) = 0 
)
select USER_IP from unmapped_ips_zero_deprioritized a
where not exists  
( select null from {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_DATA as b  
where a.USER_IP = b.USER_IP   
and ( b.LAST_QUERY_DATE >= DATE( DATEADD(day, -30, GETDATE()) ) 
and b.LAST_RESPONSE_CODE = 204 )  )
limit 3700;"""
    ]

snowflake_insert_success_staging_query = [
    f"""insert into {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_SUCCESS_DATA
select * from {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_OUTPUT_DATA 
where last_response_code = 200;"""
    ]


snowflake_merge_output_staging_query = [
    f"""MERGE INTO {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_DATA as target_table
USING (select user_ip, min(last_response_code) as last_response_code,
       max(last_query_date) as last_query_date, any_value(api_response) as api_response 
       from {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_OUTPUT_DATA 
      group by user_ip) as source_table
ON (source_table.USER_IP = target_table.USER_IP)
WHEN MATCHED THEN
    UPDATE SET 
    target_table.USER_IP = source_table.USER_IP, 
    target_table.LAST_RESPONSE_CODE = source_table.LAST_RESPONSE_CODE, 
    target_table.LAST_QUERY_DATE = source_table.LAST_QUERY_DATE,
    target_table.API_RESPONSE = source_table.API_RESPONSE 
WHEN NOT MATCHED THEN
    INSERT (USER_IP,LAST_RESPONSE_CODE,LAST_QUERY_DATE,API_RESPONSE)
    VALUES(source_table.USER_IP,
          source_table.LAST_RESPONSE_CODE,
          source_table.LAST_QUERY_DATE,
          source_table.API_RESPONSE);"""
    ]

snowflake_merge_output_mappings_query = [
    f"""MERGE INTO {IP_FLOW_DATABASE}.MAPPINGS.IP_FLOW_API_MAPPINGS as target_table
USING (select user_ip, min(last_response_code) as last_response_code,
       max(last_query_date) as last_query_date, any_value(api_response) as api_response 
       from {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_SUCCESS_DATA 
      group by user_ip) as source_table
ON (source_table.USER_IP = target_table.USER_IP)
WHEN MATCHED THEN
    UPDATE SET 
    target_table.USER_IP = source_table.USER_IP, 
    target_table.LAST_RESPONSE_CODE = source_table.LAST_RESPONSE_CODE, 
    target_table.LAST_QUERY_DATE = source_table.LAST_QUERY_DATE,
    target_table.API_RESPONSE = source_table.API_RESPONSE 
WHEN NOT MATCHED THEN
    INSERT (USER_IP,LAST_RESPONSE_CODE,LAST_QUERY_DATE,API_RESPONSE)
    VALUES(source_table.USER_IP,
          source_table.LAST_RESPONSE_CODE,
          source_table.LAST_QUERY_DATE,
          source_table.API_RESPONSE);"""
    ]

merge_devmart_domain_observations_query = [
    f"""MERGE INTO {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_COMPANY_DOMAIN_OBSERVATIONS as target_table
USING 

(SELECT DISTINCT USER_IP,MIN(LAST_RESPONSE_CODE) AS LAST_RESPONSE_CODE,
 MAX(LAST_QUERY_DATE) AS LAST_QUERY_DATE, ANY_VALUE(API_RESPONSE) AS API_RESPONSE, 'IP FLOW' AS SOURCE 
 FROM  {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_SUCCESS_DATA 
GROUP BY USER_IP) as source_table

ON (source_table.USER_IP = target_table.IP AND source_table.SOURCE = target_table.SOURCE)
WHEN MATCHED THEN
    UPDATE SET 
      
    target_table.DATE = source_table.LAST_QUERY_DATE,
    target_table.NORMALIZED_COMPANY_DOMAIN =   SPLIT_PART(PARSE_JSON(source_table.API_RESPONSE):website::string, '.', 2) || '.' || SPLIT_PART(PARSE_JSON(source_table.API_RESPONSE):website::string, '.', 3),
    target_table.SOURCE_CONFIDENCE =    NULL   
WHEN NOT MATCHED THEN
    INSERT (IP, NORMALIZED_COMPANY_DOMAIN, SOURCE, SOURCE_CONFIDENCE, DATE )
    VALUES(source_table.USER_IP,
           SPLIT_PART(PARSE_JSON(source_table.API_RESPONSE):website::string, '.', 2) || '.' || SPLIT_PART(PARSE_JSON(source_table.API_RESPONSE):website::string, '.', 3),
           'IP FLOW',
            NULL   ,
          source_table.LAST_QUERY_DATE
          );"""
    ]

snowflake_create_devmart_domain_query = [
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

snowflake_normalize_loc_staging_query = [
    f"""insert into {IP_FLOW_DATABASE}.staging.normalized_location
with normalized_country as (

  select 
  a.user_ip, 
  a.last_query_date, 
  b.isocode_3 as country_isocode_3,
  PARSE_JSON(a.API_RESPONSE):location:region::string as region_name, 
  PARSE_JSON(a.API_RESPONSE):location:town::string as city_name, 
  
  ---THE IP_TO_LOCATION table only allows numeric zipcodes which is only in the US
  CASE WHEN PARSE_JSON(a.API_RESPONSE):location:countryCode::string = 'US' THEN 
  PARSE_JSON(a.API_RESPONSE):location:postalCode::string END as postal_code 
  
  from {IP_FLOW_DATABASE}.MAPPINGS.IP_FLOW_API_MAPPINGS as a 
  left join {IP_FLOW_DATABASE}.raw_data.country_iso_codes as b
  on PARSE_JSON(a.API_RESPONSE):location:countryCode::string = b.isocode_2
), 

normalized_region as (
select a.*, b.region_isocode_2
  from normalized_country as a 
  left join {IP_FLOW_DATABASE}.raw_data.usa_region as b
  on a.region_name = b.region_name
)

select distinct
USER_IP as IP,
LAST_QUERY_DATE as DATE_UPDATED,
COUNTRY_ISOCODE_3 as NORMALIZED_COUNTRY_CODE,
REGION_ISOCODE_2 as NORMALIZED_REGION_CODE,
CITY_NAME as NORMALIZED_CITY_NAME,
POSTAL_CODE as NORMALIZED_ZIP
from normalized_region;"""
]

snowflake_merge_devmart_location_query = [
    f"""MERGE INTO {DATAMART_DATABASE}.ENTITY_MAPPINGS.IP_TO_LOCATION as target_table
USING ( SELECT IP, MAX(DATE_UPDATED) AS DATE_UPDATED, ANY_VALUE(NORMALIZED_COUNTRY_CODE) AS NORMALIZED_COUNTRY_CODE, 
       ANY_VALUE(NORMALIZED_REGION_CODE) AS NORMALIZED_REGION_CODE, 
       ANY_VALUE(NORMALIZED_CITY_NAME) AS NORMALIZED_CITY_NAME, 
       ANY_VALUE(NORMALIZED_ZIP) AS NORMALIZED_ZIP
       FROM {IP_FLOW_DATABASE}.STAGING.NORMALIZED_LOCATION 
       GROUP BY IP ) as source_table
ON (source_table.IP = target_table.IP)
WHEN MATCHED THEN
    UPDATE SET 
    target_table.IP = source_table.IP, 
    target_table.DATE_UPDATED = source_table.DATE_UPDATED, 
    target_table.NORMALIZED_COUNTRY_CODE = source_table.NORMALIZED_COUNTRY_CODE,
    target_table.NORMALIZED_REGION_CODE = source_table.NORMALIZED_REGION_CODE,
    target_table.NORMALIZED_CITY_NAME = source_table.NORMALIZED_CITY_NAME, 
    target_table.NORMALIZED_ZIP = source_table.NORMALIZED_ZIP
WHEN NOT MATCHED THEN
    INSERT (IP,DATE_UPDATED,NORMALIZED_COUNTRY_CODE,NORMALIZED_REGION_CODE,NORMALIZED_CITY_NAME,NORMALIZED_ZIP)
    VALUES(source_table.IP,
          source_table.DATE_UPDATED,
          source_table.NORMALIZED_COUNTRY_CODE,
          source_table.NORMALIZED_REGION_CODE,
          source_table.NORMALIZED_CITY_NAME,
          source_table.NORMALIZED_ZIP);"""
]


snowflake_cleanup_tables_query = [
    f"""truncate table {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_INPUT_DATA;""",
    f"""truncate table {IP_FLOW_DATABASE}.RAW_DATA.IP_FLOW_API_OUTPUT_DATA;""",
    f"""truncate table {IP_FLOW_DATABASE}.STAGING.IP_FLOW_API_OUTPUT_SUCCESS_DATA;""",
    f"""truncate table {IP_FLOW_DATABASE}.STAGING.NORMALIZED_LOCATION;"""
]

#----- begin DAG definition -------
with DAG(
    'IP-Flow-Ingestion',
    default_args={
        'depends_on_past' : False,
        'retries' : 0,
        'on_failure_callback': on_failure_callback,
        'on_success_callback': None
    },
    description = 'Ingests IP mapping data from IP Flow via HTTPS API get requests',
    schedule_interval = '@daily',
    start_date = datetime(2023, 1, 13),
    catchup=False,
    tags=['IP-Flow', 'Intent'], 
    on_failure_callback=on_failure_callback, 
    on_success_callback=None,
    
) as dag:

    snowflake_insert_input_data_exec = SnowflakeOperator(
        task_id= "insert_api_input_data",
        sql= snowflake_insert_input_data_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )

    send_get_requests = PythonOperator(
        task_id = "send_get_requests",
        python_callable=ip_api_search 
        
    )

    snowflake_insert_success_staging_exec = SnowflakeOperator(
        task_id= "insert_success_staging",
        sql= snowflake_insert_success_staging_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )   

    snowflake_merge_output_staging_exec = SnowflakeOperator(
        task_id= "merge_output_staging",
        sql= snowflake_merge_output_staging_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )   

    snowflake_merge_output_mappings_exec = SnowflakeOperator(
        task_id= "merge_output_mappings",
        sql= snowflake_merge_output_mappings_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )   

    merge_devmart_domain_observations_exec = SnowflakeOperator(
        task_id= "merge_devmart_domain_observations",
        sql= merge_devmart_domain_observations_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    ) 

    snowflake_create_devmart_domain_exec = SnowflakeOperator(
        task_id= "create_devmart_domain",
        sql= snowflake_create_devmart_domain_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    ) 

    snowflake_normalize_loc_staging_exec = SnowflakeOperator(
        task_id= "normalize_loc_staging",
        sql= snowflake_normalize_loc_staging_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    ) 

    snowflake_merge_devmart_location_exec = SnowflakeOperator(
        task_id= "merge_devmart_location",
        sql= snowflake_merge_devmart_location_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    ) 

    snowflake_cleanup_tables_exec = SnowflakeOperator(
        task_id= "cleanup_tables",
        sql= snowflake_cleanup_tables_query,
        snowflake_conn_id= SNOWFLAKE_TRANSFORM_CONNECTION,
    )

    end_success_exec = PythonOperator(
        task_id= "end_success",
        python_callable = end_success,
        on_success_callback = on_success_callback
        )

    snowflake_insert_input_data_exec >> send_get_requests >> snowflake_insert_success_staging_exec >> [snowflake_merge_output_staging_exec , snowflake_merge_output_mappings_exec] >> [merge_devmart_domain_observations_exec, snowflake_normalize_loc_staging_exec] >>  [snowflake_create_devmart_domain_exec, snowflake_merge_devmart_location_exec] >> snowflake_cleanup_tables_exec >> end_success_exec
