import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable

from urllib.parse import quote
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import INT
import sys
import os
import awswrangler as wr
import re


os.environ["no_proxy"] = "*"
db_user = "admin"
db_password = "awspassword"
db_name = "group_project"
server = "group-project.c84yji6wklcf.us-west-2.rds.amazonaws.com"
script_file_path = '/home/ubuntu/airflow/dags/scripts'


def get_engine(db=''):
    engine = create_engine(
        f'mysql+mysqlconnector://{db_user}:{db_password}@{server}:3306/{db}')
    return engine


def setup_database(ti):
    print(os. getcwd())
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    print(f"execution date = {date}")
    if(date != '2017-10-31'):
        print("no need to setup database")
        return
    engine = get_engine()
    with get_engine().connect() as con:
        with open(f'{script_file_path}/db_creation.sql') as file:
            statements = re.split(r';\s*$', file.read(), flags=re.MULTILINE)
            for statement in statements:
                if statement:
                    engine.execute(text(statement))


def insert_data_frame_using_insert_statement(df, table):
    print(f'dataframe length = {df.index.size}')
    try:
        with get_engine().connect() as db:
            columnNames = list(df.columns.values)
            print(columnNames)
            columns = ','.join(columnNames)
            valuePlaceHolder = ', '.join(
                [f':{column}' for column in columnNames])

            baseQuery = f'insert into {db_name}.{table}({columns}) '
            inserted = 0

            with tqdm(total=len(df.index)) as pbar:

                batch, tempPlaceHolder, tempValues = 0, [], {}

                for i, row in enumerate(df.values):
                    tempPlaceHolder.append(
                        "(" + ', '.join([f':{column}_{batch}' for column in columnNames]) + ")")
                    for i, column in enumerate(columnNames):
                        tempValues[f'{column}_{batch}'] = str(row[i])

                    batch += 1
                    if batch == 40000:
                        valuePlaceHolder = ','.join(tempPlaceHolder)
                        sql = text(f'{baseQuery} values {valuePlaceHolder}')
                        insert = db.execute(sql, tempValues)
                        tempValues = {}
                        tempPlaceHolder = []
                        inserted += batch
                        batch = 0

                    pbar.update(1)

                if batch > 0:
                    valuePlaceHolder = ','.join(tempPlaceHolder)
                    sql = text(f'{baseQuery} values {valuePlaceHolder}')
                    insert = db.execute(sql, tempValues)
                    inserted += batch

    except Exception as e:
        print("there is some error")
        raise e
    finally:
        db.close()

    print(f'{inserted} - values has been inserted in the table = {table}')


def load_data_from_s3(attribute, date=''):
    raw_s3_bucket = "group-project-one"
    # f"input_full/{attribute}" if date == '' else
    raw_path_dir = f"input/input_{date}/{attribute}"
    rawPath = f"s3://{raw_s3_bucket}/{raw_path_dir}"
    print(f'loading data from = {rawPath}')
    try:
        rawDf = wr.s3.read_csv(path=rawPath, path_suffix=[".csv"])
        print(rawDf.head())
        return rawDf
    except Exception as err:
        print(err)
        raise


def resolve_data(attribute, date):
    df = load_data_from_s3(attribute, date)
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(
            df['datetime'], format='%Y-%m-%d %H:%M:%S')

    if 'datetime' in df.columns:
        df.head()
        df = df.set_index('datetime').stack().reset_index(
            name=attribute).rename(columns={'level_1': 'city'})
        df.head()
    # if not os.path.exists('data_formatted'):
    #     print('data_formatted not exists so creating')
    #     os.mkdir('data_formatted')
    # with open(f'data_formatted/{attribute}.csv', "w") as writer:
    #     writer.write(df.to_csv(index=False))
    # df = pd.read_csv(f'data_formatted/{attribute}.csv')
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(
            df['datetime'], format='%Y-%m-%d %H:%M:%S')
    return df


def get_date_to_process(ti):
    print("loading date to process")
    currentDate, dates = '2017-10-30', []
    if os.path.exists("processing.db"):
        file = open("processing.db", "r")
        dates = [date.replace("\n", "") for date in file.readlines(
        ) if date.replace("\n", "") != '']
        if len(dates) > 0:
            currentDate = dates[-1]

    date_object = datetime.strptime(currentDate, '%Y-%m-%d').date()
    date_object += timedelta(days=1)
    print(f"current processing date is {date_object}")
    ti.xcom_push(key='date', value=str(date_object))


def complete_process(ti):
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    if os.path.exists("processing.db"):
        file = open("processing.db", "r")
        dates = [date.replace("\n", "") for date in file.readlines(
        ) if date.replace("\n", "") != '']
    else:
        dates = []
    dates.append(date)
    file = open("processing.db", "w")
    file.write("\n".join(dates))


def load_city_attributes(ti):
    print(ti.xcom_pull)
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    if date != '2017-10-31':
        print(f"Job is finished successfully for date = {date}")
        return

    attributes = ['city_attributes']
    for attribute in attributes:
        df = resolve_data(attribute, date)
        print(df.dtypes)
        insert_data_frame_using_insert_statement(df, attribute)
    print("all data loaded")


def load_data(attributes, ti):
    date = ti.xcom_pull(task_ids='load_date_to_process', key='date')
    if date == '' or date == '2017-11-30':
        print("Job is finished successfully")
        return
    for attribute in attributes:
        df = resolve_data(attribute, date)
        print(df.dtypes)
        insert_data_frame_using_insert_statement(df, attribute)
    print("all data loaded")
