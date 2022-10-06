import os

from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

data_extract = DAG(
    "data_extract_dag",
    schedule= None,
    start_date= pendulum.datetime(2022, 1, 1)
)

  
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
OUTPUT =  AIRFLOW_HOME + '/trips_2022-01.parquet'


with data_extract:
    extract_task = BashOperator(
        task_id='extract',
        bash_command=f'curl -o {OUTPUT} {URL}'
    ) 