import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pendulum


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

trip_db_user = os.getenv('TRIP_DB_USER')
trip_db_password = os.getenv('TRIP_DB_PASSWORD')
trip_db_host = os.getenv('TRIP_DB_HOST')
trip_db_database = os.getenv('TRIP_DB_DATABASE')

def load_data (trip_db_user, trip_db_password, trip_db_host, trip_db_database, output_file,table_name):

    import pandas as pd
    import pyarrow.parquet as pq
    from sqlalchemy import create_engine

    engine = create_engine(f'postgresql://{trip_db_user}:{trip_db_password}@{trip_db_host}:5432/{trip_db_database}')
    
    trips = pq.read_table(output_file)

    trips = trips.to_pandas()

    trips.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=10000)

    print (f'Congrats! Data loaded to the {table_name} table.')



data_etl_monthly = DAG(
    "data_etl_monthly",
    schedule= "@monthly",
    start_date= pendulum.datetime(2022, 1, 1),
    end_date= pendulum.datetime(2022,3,1)
)


  
URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'+ '{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.parquet'
output_file = AIRFLOW_HOME + '/trips_{{ ds }}.parquet'
table_name = 'trips_{{ dag_run.logical_date.strftime(\'%Y_%m\') }}'


with data_etl_monthly:
    extract_data_task = BashOperator(
        task_id='extract_data',
        bash_command=f'curl -o {output_file} {URL}'
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs=dict(
            trip_db_user=trip_db_user,
            trip_db_password=trip_db_password,
            trip_db_host=trip_db_host,
            trip_db_database=trip_db_database,
            output_file=output_file,
            table_name=table_name
            
        ),
    )

    extract_data_task >> load_data_task
