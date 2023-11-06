from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    key: str
    status: str
    restaurant_long_id: str
    ts_long_id: str
    user_long_id: str


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        # with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT object_value
                    FROM stg.ordersystem_orders
                    WHERE object_id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY object_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            response = cur.fetchall()
            s = [str2json(row[0]) for row in response]

            objs = []
            for item in s:
                new_item = OrderObj(
                    key = item.get('_id', ''),
                    status = item.get('final_status', ''),
                    restaurant_long_id = item.get('restaurant').get('id'),
                    ts_long_id = item.get('update_ts'),
                    user_long_id = item.get('user').get('id')
                )
                objs.append(new_item)
            return objs


class OrderDestRepository:
    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM dds.dm_restaurants where restaurant_id ='{order.restaurant_long_id}' and active_to='2099-12-31 00:00:00.000'")
            restaurant = cur.fetchall()
            restaurant_id = restaurant[0][0]

            cur.execute(f"SELECT * FROM dds.dm_users where user_id ='{order.user_long_id}'")
            user = cur.fetchall()
            user_id = user[0][0]

            cur.execute(f"SELECT * FROM dds.dm_timestamps where ts ='{order.ts_long_id}' ")
            ts = cur.fetchall()
            print(ts)
            if (ts==[]):
                return
            timestamp_id = ts[0][0]

            cur.execute(
                """
                    INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                    ON CONFLICT (order_key) DO UPDATE
                        SET order_status = EXCLUDED.order_status,
                            restaurant_id = EXCLUDED.restaurant_id,
                            timestamp_id = EXCLUDED.timestamp_id,
                            user_id = EXCLUDED.user_id;
                """,
                {
                    "order_key": order.key,
                    "order_status": order.status,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "user_id": user_id
                }
            )


class OrderLoader:
    WF_KEY = "orders_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 5000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.dds = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
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
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.dds.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.key for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
