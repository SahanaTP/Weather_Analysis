import os
import json
import pandas as pd
import numpy as np
import glob
import pandas as pd
import numpy as np
from tqdm import tqdm
from urllib.parse import quote
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import INT
import pymysql
from airflow.models import Variable

global cursor
global conn
global engine
global destination

destination = 'mysql'
script_folder_path = '/home/ubuntu/airflow/dags/scripts'

script_file = (
    f'{script_folder_path}/mysql_aggregate_db_creation.sql' if destination == 'mysql'
    else f'{script_folder_path}/aggregate_db_creation.sql')


# user = 'ANJALIHIMANSHUOJHA0505'  # 'samkumar'
# password = 'Snowflake1'  # 'Snowflake1'
# account = 'shb26496.us-east-1'  # 'zmb35619',
user = 'samkumar'
password = 'Snowflake1'
account = 'zmb35619',
database = 'group_project_aggregates'
schema = "weather_data"
mysql_aggregate_db = "data"

'''
    Source DB credentials
'''
db_user = "admin"
db_password = "awspassword"
db_name = "group_project"
server = "group-project.c84yji6wklcf.us-west-2.rds.amazonaws.com"


def get_date(ti):
    print(ti.xcom_pull)
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    print(f"running job for the date = {date}")
    return date


def get_engine(destination='mysql'):
    if destination == 'snowflake':
        engine = create_engine(
            'snowflake://{user}:{password}@{account}'.format(
                user=user,
                password=password,
                account=account
            )
        )
        return engine
    else:
        engine = create_engine(
            f'mysql+mysqlconnector://{db_user}:{db_password}@{server}:3306/')
        return engine


def get_query_engine(db=db_name):
    engine = create_engine(
        f'mysql+mysqlconnector://{db_user}:{db_password}@{server}:3306/{db}')
    return engine


def get_table_name(destination, table):
    if destination == 'snowflake':
        return f'{database}.{schema}.{table}'
    else:
        return f'{mysql_aggregate_db}.{table}'


def insert_data_frame_using_insert_statement(df, table):
    print(f'dataframe length = {df.index.size}')
    try:
        engine = get_engine(destination)
        db = engine.connect()
        tableName = get_table_name('mysql', table)
        columnNames = list(df.columns.values)
        columns = ','.join(columnNames)
        valuePlaceHolder = ', '.join([f':{column}' for column in columnNames])
        inserted = 0

        with tqdm(total=len(df.index)) as pbar:
            batch, tempPlaceHolder, tempValues = 0, [], {}
            for i, row in enumerate(df.values):
                tempPlaceHolder.append(
                    "(" + ', '.join([f':{column}_{batch}' for column in columnNames]) + ")")
                for i, column in enumerate(columnNames):
                    tempValues[f'{column}_{batch}'] = row[i]

                batch += 1
                if batch == 16384:
                    valuePlaceHolder = ','.join(tempPlaceHolder)
                    sql = text(
                        f'insert into {tableName}({columns}) values {valuePlaceHolder}')
                    insert = db.execute(sql, tempValues)
                    tempValues = {}
                    tempPlaceHolder = []
                    inserted += batch
                    batch = 0

                pbar.update(1)
            if batch > 0:
                valuePlaceHolder = ','.join(tempPlaceHolder)
                sql = text(
                    f'insert into  {tableName}({columns}) values {valuePlaceHolder}')
                insert = db.execute(sql, tempValues)
                inserted += batch

    except Exception as e:
        print("there is some error")
        raise e
    finally:
        db.close()

    print(f'{inserted} - values has been inserted in the table = {table}')


def agg_create_database(ti):
    print(os. getcwd())
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    print(f"execution date = {date}")
    if(date != '2017-10-31'):
        print("no need to setup database")
        return
    import re
    engine = get_engine()
    with engine.connect() as con:
        with open(script_file) as file:
            statements = re.split(r';\s*$', file.read(), flags=re.MULTILINE)
            for statement in statements:
                if statement:
                    engine.execute(text(statement))
        con.close()


query_engine = get_query_engine()


def agg_load_cities(ti):
    date = get_date(ti)
    if date != '2017-10-31':
        print(f"Job is finished successfully for date = {date}")
        return
    query = """
    SELECT city, country,latitude,longitude
    FROM city_attributes;
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(df, f'cities')


def agg_load_humidity_aggregates(ti):
    date = get_date(ti)
    where_predicate = '' if date == '' or date == None else f"where date(datetime) = '{date}'"
    query = f"""
    SELECT date(DATETIME) AS date, city, 
    ROUND(avg(HUMIDITY),0) as humidity,
    ROUND(min(HUMIDITY),0) as min_humidity,
    ROUND(max(HUMIDITY),0) as max_humidity
    FROM humidity 
    {where_predicate} 
    GROUP BY CITY, date(datetime)
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(df, f'humidity_daily_average')


def agg_load_wind_speed_aggregates(ti):
    date = get_date(ti)
    where_predicate = '' if date == '' or date == None else f"where date(datetime) = '{date}'"
    query = f"""
    SELECT date(DATETIME) AS date, city, 
    ROUND(avg(wind_speed),0) as wind_speed,
    ROUND(min(wind_speed),0) as min_wind_speed,
    ROUND(max(wind_speed),0) as max_wind_speed
    FROM wind_speed 
    {where_predicate} 
    GROUP BY CITY, date(datetime)
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(df, f'wind_speed_daily_average')


def agg_load_wind_direction_aggregates(ti):
    date = get_date(ti)
    where_predicate = '' if date == '' or date == None else f"where date(datetime) = '{date}'"
    query = f"""
    select 
        date, 
        city, 
        direction_of_wind as wind_direction 
    from
        (With wind_with_directions as
            (
                SELECT date(DATETIME) AS date, city, 
                    CASE 
                        WHEN wind_direction >= 292.6 and wind_direction <= 337.5 THEN 'NorthWest'
                        WHEN wind_direction >= 247.6 and wind_direction <= 292.5 THEN 'West'
                        WHEN wind_direction >= 202.6 and wind_direction <= 247.5 THEN 'SouthWest'
                        WHEN wind_direction >= 157.6 and wind_direction <= 202.5 THEN 'South'
                        WHEN wind_direction >= 112.6 and wind_direction <= 157.5 THEN 'SouthEast'
                        WHEN wind_direction >= 67.6 and wind_direction <= 112.5 THEN 'East'
                        WHEN wind_direction >= 22.6 and wind_direction <= 67.5 THEN 'NorthEast'
                        ELSE 'North'
                    end direction
                FROM group_project.wind_direction {where_predicate} 
            )
            select 
                date, 
                city, 
                nth_value(direction, 1) over (partition by CITY, date) as direction_of_wind,
                row_number() over (partition by CITY, date) as row_index
            from wind_with_directions
        ) final
    where row_index =1;
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(
        df, f'wind_direction_daily_average')


def agg_load_temperature_aggregates(ti):
    date = get_date(ti)
    where_predicate = '' if date == '' or date == None else f"where date(datetime) = '{date}'"
    query = f"""
    SELECT DATE(datetime) as date, city,
    round(AVG((temperature-273.15)*9/5+32)) as temperature,
    round(min((temperature-273.15)*9/5+32)) as min_temperature,
    round(max((temperature-273.15)*9/5+32)) as max_temperature
    FROM temperature
    {where_predicate} 
    GROUP BY city, date
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(df, f'daily_temperature')


def agg_load_pressure_aggregates(ti):
    date = get_date(ti)
    where_predicate = '' if date == '' or date == None else f"where date(datetime) = '{date}'"
    query = f"""
    SELECT DATE(datetime) as date,city,
    round(AVG(pressure)) as pressure,
    round(min(pressure)) as min_pressure,
    round(max(pressure)) as max_pressure
    FROM pressure
    {where_predicate} 
    GROUP BY city, date
    """
    df = pd.read_sql_query(query, query_engine)
    insert_data_frame_using_insert_statement(df, f'daily_pressure')


# def get_connection():
#     conn = None
#     try:
#         conn = pymysql.connect(host=server,
#                                user=db_user,
#                                password=db_password)
#     except pymysql.Error as e:
#         print("make_connection_to_rds: Failed to connect to the AWS RDS")
#         return conn
#     else:
#         return conn


# def execute_sql_query(sql):
#     try:
#         cursor.execute(sql)
#     except (pymysql.Error, pymysql.Warning) as e:
#         print(
#             "execute_sql_query: Failed to execute sql query {sql}".format(sql=sql))
#         return False
#     return True


# def get_output_df(query):
#     df = pd.read_sql_query(query, engine)
#     return
