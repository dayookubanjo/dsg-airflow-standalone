from datetime import datetime, timedelta  
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'adedayo.okubanjo',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='This is my first dag.',
    start_date=datetime(2022, 12, 11),
    catchup=False,
    schedule_interval='@daily'
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hello world, this is the second task!"
    )

    task1 >> task2
