from logging import Logger
from typing import List

from examples.cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from datetime import datetime
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class СourierObj(BaseModel):
	courier_id: str
	courier_name: str
	settlement_year: int
	settlement_month: int
	orders_count: int
	orders_total_sum: float
	rate_avg: float
	order_processing_fee: float
	courier_tips_sum: float
	courier_order_sum: float
	courier_reward_sum: float

class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int, limit: int) -> List[СourierObj]:
        with self._db.client().cursor(row_factory=class_row(СourierObj)) as cur:

            cur.execute(
                """
                    SELECT table.*
                    FROM (
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
                        ) as table

                    WHERE table.id > %(threshold)s
                    ORDER BY table.id ASC
                    LIMIT %(limit)s;

                """,
                {
                    "threshold": courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

        return objs


class CouriersDestRepository:

    def insert_courier(self, conn: Connection, courier: СourierObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_tips_sum, courier_order_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_tips_sum)s, %(courier_order_sum)s, %(courier_order_sum)s, %(courier_reward_sum)s);
                """,
                {

                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                    "settlement_year": courier.settlement_year,
                    "settlement_month": courier.settlement_month,
                    "orders_count": courier.orders_count,
                    "orders_total_sum": courier.orders_total_sum,
                    "rate_avg": courier.rate_avg,
                    "order_processing_fee": courier.order_processing_fee,
                    "courier_tips_sum": courier.courier_tips_sum,
                    "courier_order_sum": courier.courier_order_sum,
                    "courier_reward_sum": courier.courier_reward_sum
                },
            )


class CourierLoader:
    WF_KEY = "dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(pg_origin)
        self.cdm = CouriersDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.cdm.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
