from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    active_from: str


class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[RestaurantObj]:
        # with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT object_value
                    FROM stg.ordersystem_restaurants
                    WHERE object_id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY object_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurant_threshold,
                    "limit": limit
                }
            )
            response = cur.fetchall()
            s = [str2json(row[0]) for row in response]
            objs = []
            for item in s:
                new_item = RestaurantObj(
                    restaurant_id = item['_id'],
                    restaurant_name = item['name'],
                    active_from = item['update_ts'],
                )
                objs.append(new_item)
        return objs





class RestaurantDestRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
               """INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, '2099-12-31 00:00:00.000')
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                    restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                        """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                        "active_from": restaurant.active_from
                },
            )



class RestaurantLoader:
    WF_KEY = "restaurants_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.dds = RestaurantDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
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
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.restaurant_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
