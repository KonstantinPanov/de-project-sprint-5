import requests
import json

import pandas as pd
import psycopg2
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

conn = psycopg2.connect(
    host="localhost",
    database="de",
    user="jovyan",
    password="jovyan")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': True,
    'depends_on_the_past': True
}

# инкрементальное кол-во рядов для загрузки
limit_dict = {
    'couriers': 10,
    'deliveries': 50
}

def couriers(limit_dict, **context):

    limit = str(limit_dict['couriers'])
    offset =context['ti'].xcom_pull(task_ids='get_last_loaded_rows', key='last_cour_row')[0]
    print(f'Loading courier rows from {int(offset) + 1} to {int(offset) + int(limit)}')

    url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'

    r = requests.get(url=url,headers=headers)
    df = pd.DataFrame(json.loads(r.content), columns=['_id', 'name'])
    df.to_sql('couriers', engine, index=False, if_exists='append', schema='stg')

def deliveries(limit_dict, **context):

    limit = str(limit_dict['deliveries'])
    offset =context['ti'].xcom_pull(task_ids='get_last_loaded_rows', key='last_del_row')[0]
    print(f'Loading delivery rows from {int(offset) + 1} to {int(offset) + int(limit)}')

    url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'

    r = requests.get(url=url,headers=headers)
    df = pd.DataFrame(json.loads(r.content), columns=['order_id','order_ts','delivery_id','courier_id','address','delivery_ts','rate','sum','tip_sum'])
    df.to_sql('deliveries', engine, index=False, if_exists='append', schema='stg')


def get_last_loaded_rows(conn, **context):
    cur = conn.cursor()
    
    last_del_qry = '''
        SELECT workflow_settings->>'last_loaded_delivery_row'
        FROM stg.srv_wf_settings
        WHERE workflow_key = 'couriers_deliveries_update'
    '''

    last_cour_qry = '''
        SELECT workflow_settings->>'last_loaded_courier_row'
        FROM stg.srv_wf_settings
        WHERE workflow_key = 'couriers_deliveries_update'
    '''
    
    try:
        # получим последнюю строку в таблице deliveries
        cur.execute(last_del_qry)
        del_row = cur.fetchone()

        # получим последнюю строку в таблице couriers
        cur.execute(last_cour_qry)
        cour_row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    # добавим данные в контекст
    ti = context['ti']
    ti.xcom_push(key='last_del_row', value=del_row)
    ti.xcom_push(key='last_cour_row', value=cour_row)


def update_workflow(limit_dict, conn, **context):
    cur = conn.cursor()
    
    last_loaded_delivery_row = int(context['ti'].xcom_pull(
        task_ids='get_last_loaded_rows', 
        key='last_del_row')[0]) + limit_dict['deliveries']
    last_loaded_courier_row = int(context['ti'].xcom_pull(
        task_ids='get_last_loaded_rows', 
        key='last_cour_row')[0]) + limit_dict['couriers']

    sql=f'''
            UPDATE stg.srv_wf_settings 
            SET workflow_settings = '\u007b"last_loaded_delivery_row": "{last_loaded_delivery_row}", "last_loaded_courier_row": "{last_loaded_courier_row}"\u007d'
            WHERE workflow_key = 'couriers_deliveries_update';
        '''
    
    try:
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



with DAG (
    dag_id='update_dag',
    default_args = default_args,
    start_date=datetime(2023, 6, 29),
    schedule_interval='*/15 * * * *',
    max_active_runs=1

 ) as dag:
    
    get_last_loaded_rows = PythonOperator(
        task_id='get_last_loaded_rows',
        python_callable=get_last_loaded_rows,
        op_kwargs={'conn': conn},
        provide_context=True
    )

    couriers = PythonOperator(
        task_id='insert_into_couriers',
        python_callable=couriers,
        op_kwargs={'limit_dict': limit_dict},
        provide_context=True
    )

    deliveries = PythonOperator(
        task_id='insert_into_deliveries',
        python_callable=deliveries,
        op_kwargs={'limit_dict': limit_dict},
        provide_context=True
    )

    update_workflow = PythonOperator(
        task_id='update_workflow',
        python_callable=update_workflow,
        op_kwargs={'limit_dict': limit_dict, 'conn': conn},
        provide_context=True
    )

    get_last_loaded_rows >> couriers >> deliveries >> update_workflow
