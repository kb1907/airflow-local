import os

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


import pendulum




AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'+ '{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.parquet'
output_file = AIRFLOW_HOME + '/trips_{{ ds }}.parquet'
table_name = 'trips_{{ dag_run.logical_date.strftime(\'%Y_%m\') }}'


data_etl_monthly = DAG(
    "data_etl_monthly",
    schedule= "@monthly",
    start_date= pendulum.datetime(2022, 1, 1),
    end_date= pendulum.datetime(2022,2,1)
)



with data_etl_monthly:

    extract_data_task = BashOperator(
        task_id='extract_data',
        bash_command=f'curl -o {output_file} {URL}'
    )

    @task()
    def load_data ():
       
        import pandas as pd
        import pyarrow.parquet as pq
        from sqlalchemy import create_engine
        import jinja2

        trip_db_user = os.getenv('TRIP_DB_USER')
        trip_db_password = os.getenv('TRIP_DB_PASSWORD')
        trip_db_host = os.getenv('TRIP_DB_HOST')
        trip_db_database = os.getenv('TRIP_DB_DATABASE')
        
        


        engine = create_engine(f'postgresql://{trip_db_user}:{trip_db_password}@{trip_db_host}:5432/{trip_db_database}')
        
        trips = pq.read_table(output_file)

        trips = trips.to_pandas()

        trips.heads(n=100).to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=10)

        print (f'Congrats! Data loaded to the {table_name} table.')

    extract_data_task >> load_data()
    



    