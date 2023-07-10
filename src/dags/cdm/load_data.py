from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)

}

with DAG (
    dag_id='load_data',
    default_args = default_args,
    start_date=datetime(2023, 6, 17),
    schedule_interval='0 0 1 * *' # At 00:00 on day-of-month 1.

 ) as dag:

    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='''

            CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
                courier_id varchar NOT NULL,
                courier_name varchar NOT NULL,
                settlement_year int4 NOT NULL,
                settlement_month int4 NOT NULL,
                orders_count int4 NOT NULL,
                orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
                rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
                order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
                courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
                courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
                courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
                CONSTRAINT courier_ledger UNIQUE (courier_id, settlement_year, settlement_month)
            );

        '''
    )


    load_data = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',

        sql=''' insert into cdm.dm_courier_ledger
                select t3.*
                    from (
                        select t2.*,
                            courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                        from
                            (select t1.*,
                                case
                                    when t1.rate_avg < 4 then case when t1.orders_total_sum * 0.05 < 100 then 100 else t1.orders_total_sum * 0.05 end
                                    when t1.rate_avg >= 4 and t1.rate_avg < 4.5 then case when t1.orders_total_sum * 0.07 < 150 then 150 else t1.orders_total_sum * 0.07 end
                                    when t1.rate_avg >= 4.5 and t1.rate_avg < 4.9 then case when t1.orders_total_sum * 0.08 < 175 then 175 else t1.orders_total_sum * 0.08 end
                                    when t1.rate_avg > 4.9 then case when t1.orders_total_sum * 0.1 < 200 then 200 else t1.orders_total_sum * 0.1 end
                                end as courier_order_sum

                            from
                                (select
                                    d.courier_id,
                                    c.name as courier_name,
                                    extract(year from d.delivery_ts)::INT as settlement_year,
                                    extract(month from d.delivery_ts)::INT as settlement_month,
                                    count(d.order_id)::INT as orders_count,
                                    sum(d.sum) as orders_total_sum,
                                    avg(d.rate) as rate_avg,
                                    sum(d.sum)*0.25 as order_processing_fee,
                                    sum(tip_sum) as courier_tips_sum

                                from dds.dm_deliveries d
                                inner join dds.dm_couriers c on d.courier_id=c._id
                                group by courier_id, courier_name, extract(year from d.delivery_ts), extract(month from d.delivery_ts))

                                as t1)
                            as t2
                        ) as t3

                        where settlement_year=extract(year from now())::INT and settlement_month=extract(month from now())::INT
        '''


    )

    create_postgres_table >> load_data
