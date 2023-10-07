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


def completed():
    print("process completed")


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
    dag_id='gp_load_data_from_s3_to_rds',
    default_args=default_args,
    description='load_data_from_s3_to_rds',
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
        task_id='load_data_pressure',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['pressure']}
    )
    task5 = PythonOperator(
        task_id='load_data_temperature',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['temperature']}
    )
    task6 = PythonOperator(
        task_id='load_data_humidity',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['humidity']}
    )
    task7 = PythonOperator(
        task_id='load_data_weather_description',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['weather_description']}
    )
    task8 = PythonOperator(
        task_id='load_data_wind_direction',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['wind_direction']}
    )
    task9 = PythonOperator(
        task_id='load_data_wind_speed',
        python_callable=load_data,
        dag=dag,
        op_kwargs={'attributes': ['wind_speed']}
    )
    task10 = PythonOperator(
        task_id='complete_loading_process',
        python_callable=complete_process,
        dag=dag
    )

    task11 = PythonOperator(
        task_id='agg_create_database',
        python_callable=agg_create_database,
        dag=dag
    )
    task12 = PythonOperator(
        task_id='agg_load_cities',
        python_callable=agg_load_cities,
        dag=dag
    )
    task13 = PythonOperator(
        task_id='agg_load_humidity_aggregates',
        python_callable=agg_load_humidity_aggregates,
        dag=dag,
    )
    task14 = PythonOperator(
        task_id='agg_load_wind_speed_aggregates',
        python_callable=agg_load_wind_speed_aggregates,
        dag=dag,
    )
    task15 = PythonOperator(
        task_id='agg_load_wind_direction_aggregates',
        python_callable=agg_load_wind_direction_aggregates,
        dag=dag,
    )
    task16 = PythonOperator(
        task_id='agg_load_temperature_aggregates',
        python_callable=agg_load_temperature_aggregates,
        dag=dag,
    )
    task17 = PythonOperator(
        task_id='agg_load_pressure_aggregates',
        python_callable=agg_load_pressure_aggregates,
        dag=dag,
    )
    task18 = PythonOperator(
        task_id='complete_process',
        python_callable=completed,
        dag=dag,
    )

    # task1 >> task2 >> [task3, task4] >> task5
    # task5 >> task6 >> [task7, task8] >> task9
    # task9 >> [task10, task11] >> task12

    task1 >> task2 >> [task3, task4, task5,
                       task6, task7, task8, task9] >> task10
    task10 >> task11 >> [task12, task13, task14,
                         task15, task16, task17] >> task18
