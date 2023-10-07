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
    dag_id='rds_to_snow',
    default_args=default_args,
    description='rds_to_snow',
    start_date=datetime(2023, 4, 30),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='load_date_to_process',
        python_callable=get_date_to_process,
        dag=dag
    )
    task6 = PythonOperator(
        task_id='agg_create_database',
        python_callable=agg_create_database,
        dag=dag
    )
    task7 = PythonOperator(
        task_id='agg_load_cities',
        python_callable=agg_load_cities,
        dag=dag
    )
    task8 = PythonOperator(
        task_id='agg_load_humidity_aggregates',
        python_callable=agg_load_humidity_aggregates,
        dag=dag,
    )
    task9 = PythonOperator(
        task_id='agg_load_wind_speed_aggregates',
        python_callable=agg_load_wind_speed_aggregates,
        dag=dag,
    )
    task10 = PythonOperator(
        task_id='agg_load_wind_direction_aggregates',
        python_callable=agg_load_wind_direction_aggregates,
        dag=dag,
    )
    task11 = PythonOperator(
        task_id='agg_load_temperature_aggregates',
        python_callable=agg_load_temperature_aggregates,
        dag=dag,
    )
    task12 = PythonOperator(
        task_id='agg_load_pressure_aggregates',
        python_callable=agg_load_pressure_aggregates,
        dag=dag,
    )

    task1 >> task6 >> [task7, task8] >> task9 >> [task10, task11] >> task12
