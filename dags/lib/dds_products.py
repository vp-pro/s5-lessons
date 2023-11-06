from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductObj(BaseModel):
    product_id: str
    product_name: str
    product_price: float
    restaurant_id_string_long_real: str
    active_from: str


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductObj]:
        # with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT object_value
                    FROM stg.ordersystem_restaurants
                    WHERE object_id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY object_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            response = cur.fetchall()
            s = [str2json(row[0]) for row in response]

            objs = []
            for row in s:
                order_items = row.get('menu', [])
                for item in order_items:
                    new_item = ProductObj(
                        product_id = item.get('_id', ''),
                        product_name = item.get('name', ''),
                        product_price = item.get('price', 0),
                        restaurant_id_string_long_real = row['_id'],
                        active_from = row['update_ts']
                    )
                    objs.append(new_item)
            return objs


class ProductDestRepository:
    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM dds.dm_restaurants where restaurant_id ='{product.restaurant_id_string_long_real}' and active_to='2099-12-31 00:00:00.000'")
            restaurant = cur.fetchall()
            restaurant_id = restaurant[0][0]

            cur.execute(
                """INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, '2099-12-31 00:00:00.000', %(restaurant_id)s)
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "restaurant_id": restaurant_id
                },
            )


class ProductLoader:
    WF_KEY = "products_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_origin)
        self.dds = ProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.dds.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.product_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
