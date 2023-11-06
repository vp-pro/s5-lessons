from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime as dt
import datetime

class TsObj(BaseModel):
    obj_id: str
    ts: str
    year: int
    month: int
    day: int
    date: datetime.date
    time: datetime.time


class TssOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_tss(self, ts_threshold: int, limit: int) -> List[TsObj]:
        # with self._db.client().cursor(row_factory=class_row(TsObj)) as cur:
        with self._db.client().cursor() as cur:
            cur.execute(
                """SELECT object_value
                    FROM stg.ordersystem_orders
                    WHERE object_id > %(threshold)s
                    AND (object_value::json->>'final_status' = 'CLOSED' OR object_value::json->>'final_status' = 'CANCELLED')
                    ORDER BY object_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов."""
                    , {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            response = cur.fetchall()
            s = [str2json(row[0]) for row in response]
            objs = []
            for item in s:
                parsed_datetime = dt.strptime(item['date'], '%Y-%m-%d %H:%M:%S')
                formatted_ts = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
                date = parsed_datetime.date()
                time = parsed_datetime.time()
                new_item = TsObj(
                    obj_id = item['_id'],
                    ts = formatted_ts,
                    year = parsed_datetime.year,
                    month = parsed_datetime.month,
                    day = parsed_datetime.day,
                    date = date,
                    time = time
                )
                objs.append(new_item)
        return objs


class TsDestRepository:
    def insert_ts(self, conn: Connection, ts: TsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                        """,
                {"ts": ts.ts,
                "year": ts.year,
                "month": ts.month,
                "day": ts.day,
                "date": ts.date,
                "time": ts.time},
            )

class TsLoader:
    WF_KEY = "ts_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TssOriginRepository(pg_origin)
        self.dds = TsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_tss(self):
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
            load_queue = self.origin.list_tss(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} tss to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for ts in load_queue:
                self.dds.insert_ts(conn, ts)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.obj_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
