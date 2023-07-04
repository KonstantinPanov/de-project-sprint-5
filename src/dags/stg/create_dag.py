import requests
import json

import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from sqlalchemy import create_engine

sort_field = 'id'
sort_direction = 'asc'
offset='0'
nickname = 'kpanovpanov'
cohort_number = '12'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort_number,
    'X-API-KEY': api_key
}

engine = create_engine('postgresql+psycopg2://jovyan:jovyan@localhost:5432/de')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)

}

# изначальное кол-во рядов для загрузки
limit_dict = {
    'restaurants': 50,
    'couriers': 10,
    'deliveries': 50
}

def restaurants(limit_dict):

    limit = str(limit_dict['restaurants'])
    url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'

    r = requests.get(url=url,headers=headers)
    df = pd.DataFrame(json.loads(r.content), columns=['_id', 'name'])
    df.to_sql('restaurants', engine, index=False, if_exists='replace', schema='stg')

def couriers(limit_dict):

    limit = str(limit_dict['couriers'])
    url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'

    r = requests.get(url=url,headers=headers)
    df = pd.DataFrame(json.loads(r.content), columns=['_id', 'name'])
    df.to_sql('couriers', engine, index=False, if_exists='replace', schema='stg')

def deliveries(limit_dict):

    limit = str(limit_dict['deliveries'])
    url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'

    r = requests.get(url=url,headers=headers)
    df = pd.DataFrame(json.loads(r.content), columns=['order_id','order_ts','delivery_id','courier_id','address','delivery_ts','rate','sum','tip_sum'])
    df.to_sql('deliveries', engine, index=False, if_exists='replace', schema='stg')


with DAG (
    dag_id='create_dag',
    default_args = default_args,
    start_date=datetime(2023, 6, 28),
    schedule_interval='@once'

 ) as dag:

    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='''
            CREATE TABLE IF NOT EXISTS stg.restaurants (
                id serial4 NOT NULL,
                _id varchar NOT NULL,
                name varchar NOT NULL,
                CONSTRAINT restaurants_pkey PRIMARY KEY (id)
            );

            CREATE TABLE IF NOT EXISTS stg.couriers (
                id serial4 NOT NULL,
                _id varchar NOT NULL,
                name varchar NOT NULL,
                CONSTRAINT couriers_pkey PRIMARY KEY (id)
            );

            CREATE TABLE IF NOT EXISTS stg.deliveries (
                id serial4 NOT NULL,
                order_id varchar NOT NULL,
                order_ts timestamp NOT NULL,
                delivery_id varchar NOT NULL,
                courier_id varchar NOT NULL,
                address varchar NOT NULL,
                delivery_ts timestamp NOT NULL,
                rate int2 NOT NULL,
                sum numeric(14, 2) NOT NULL DEFAULT 0,
                tip_sum numeric(14, 2) NOT NULL DEFAULT 0,
                CONSTRAINT deliveries_pkey PRIMARY KEY (id)
            );
        ''',
    )

    restaurants = PythonOperator(
        task_id='insert_into_restaurants',
        python_callable=restaurants,
        op_kwargs={'limit_dict': limit_dict}
    )

    couriers = PythonOperator(
        task_id='insert_into_couriers',
        python_callable=couriers,
        op_kwargs={'limit_dict': limit_dict}
    )

    deliveries = PythonOperator(
        task_id='insert_into_deliveries',
        python_callable=deliveries,
        op_kwargs={'limit_dict': limit_dict}
    )

    update_workflow = PostgresOperator(
        task_id='update_workflow',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql=f'''
            INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) VALUES
            ('couriers_deliveries_update', '\u007b"last_loaded_delivery_row": "{limit_dict['deliveries']}", "last_loaded_courier_row": "{limit_dict['couriers']}"\u007d');
        ''',
    )

    create_postgres_table >> restaurants >> couriers >> deliveries >> update_workflow
