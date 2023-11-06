from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


import decimal
class FctObj(BaseModel):
    rn: int
    product_id: int
    order_id: int
    count: int
    price: decimal.Decimal
    total_sum:  decimal.Decimal
    bonus_payment:  decimal.Decimal
    bonus_grant:  decimal.Decimal


class FctsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fcts(self, fct_threshold: int, limit: int) -> List[FctObj]:
        # with self._db.client().cursor(row_factory=class_row(FctObj)) as cur:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    select
                        s.rn,
                        p.id as product_id,
                        o.id as order_id,
                        s.quantity::int as count,
                        s.price::numeric(19, 5) as price,
                        s.quantity::int * s.price::numeric(19, 5) as total_sum,
                        coalesce(s.bonus_payment, '0')::numeric(19,2) as bonus_payment,
                        coalesce(s.bonus_grant, '0')::numeric(19,2) as bonus_grant
                    from (
                        select
                            ROW_NUMBER() OVER() as rn,
                            o.order_key,
                            o.product_key,
                            o.quantity,
                            o.price,
                            a.bonus_grant,
                            a.bonus_payment
                        from
                        (select
                            object_id as order_key,
                            json_array_elements((object_value::JSON->>'order_items')::JSON)::JSON->>'id' as product_key,
                            json_array_elements((object_value::JSON->>'order_items')::JSON)::JSON->>'quantity' as quantity,
                            json_array_elements((object_value::JSON->>'order_items')::JSON)::JSON->>'price' as price
                        from stg.ordersystem_orders where (object_value::JSON->>'final_status')::varchar in ('CLOSED')) o
                        left join (select
                            (be.event_value::JSON->>'order_id')::varchar as order_key,
                            json_array_elements((event_value::JSON->>'product_payments')::JSON)::JSON->>'product_id' as product_key,
                            json_array_elements((event_value::JSON->>'product_payments')::JSON)::JSON->>'bonus_payment' as bonus_payment,
                            json_array_elements((event_value::JSON->>'product_payments')::JSON)::JSON->>'bonus_grant' as bonus_grant
                        from stg.bonussystem_events be
                        where be.event_type = 'bonus_transaction') a on o.order_key = a.order_key and o.product_key = a.product_key
                        ) s
                        join dds.dm_orders o on s.order_key = o.order_key
                        join dds.dm_products p on s.product_key = p.product_id
                    WHERE rn > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY rn ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": fct_threshold,
                    "limit": limit
                }
            )
            response = cur.fetchall()

            objs = []
            for item in response:
                new_item = FctObj(
                    rn = item[0],
                    product_id = item[1],
                    order_id = item[2],
                    count = item[3],
                    price = item[4],
                    total_sum = item[5],
                    bonus_payment = item[6],
                    bonus_grant = item[7]
                )
                objs.append(new_item)
            return objs


class FctDestRepository:
    def insert_fct(self, conn: Connection, fct: FctObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                        #             ON CONFLICT (fct_key) DO UPDATE
                        # SET product_id = EXCLUDED.product_id,
                        #     count = EXCLUDED.count,
                        #     price = EXCLUDED.price,
                        #     total_sum = EXCLUDED.total_sum,
                        #     bonus_payment = EXCLUDED.bonus_payment,
                        #     bonus_grant = EXCLUDED.bonus_grant;
                {
                    "product_id": fct.product_id,
                    "order_id": fct.order_id,
                    "count": fct.count,
                    "price": fct.price,
                    "total_sum": fct.total_sum,
                    "bonus_payment": fct.bonus_payment,
                    "bonus_grant": fct.bonus_grant,                }
            )

class FctLoader:
    WF_KEY = "fcts_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctsOriginRepository(pg_origin)
        self.dds = FctDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fcts(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: '0'})

            print(wf_setting)
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fcts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fcts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct in load_queue:
                self.dds.insert_fct(conn, fct)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.rn for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
