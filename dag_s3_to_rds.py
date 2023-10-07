from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable


from includes.data_load_to_rds import get_date_to_process
from includes.data_load_to_rds import load_city_attributes
from includes.data_load_to_rds import complete_process
from includes.data_load_to_rds import setup_database
from includes.data_load_to_rds import load_data

from includes.rds_to_snow import agg_create_database
from includes.rds_to_snow import agg_load_cities
from includes.rds_to_snow import agg_load_humidity_aggregates
from includes.rds_to_snow import agg_load_pressure_aggregates
from includes.rds_to_snow import agg_load_temperature_aggregates
from includes.rds_to_snow import agg_load_wind_direction_aggregates
from includes.rds_to_snow import agg_load_wind_speed_aggregates


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='s3_to_rds',
    default_args=default_args,
    description='s3_to_rds',
    start_date=datetime(2023, 4, 30),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='load_date_to_process',
        python_callable=get_date_to_process,
        dag=dag
    )
    task2 = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database,
        dag=dag
    )
    task3 = PythonOperator(
        task_id='load_cities',
        python_callable=load_city_attributes,
        dag=dag,
    )
    task4 = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['pressure', 'temperature', 'humidity',
                                  'weather_description', 'wind_direction', 'wind_speed']}
    )
    task5 = PythonOperator(
        task_id='complete_loading_process',
        python_callable=complete_process,
        dag=dag
    )

    

    task1 >> task2 >> [task3, task4] >> task5
    