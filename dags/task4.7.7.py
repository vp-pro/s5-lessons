import logging

import pendulum
from airflow.decorators import dag, task
from lib.dds_fcts import FctLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    'dds_load_fcts',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_fcts_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="fcts_load")
    def load_fcts():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FctLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_fcts()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fcts_dict = load_fcts()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    fcts_dict  # type: ignore


stg_bonus_system_fcts_dag = sprint5_dds_fcts_dag()





# import logging

# import pendulum
# from airflow.decorators import dag, task
# from airflow.models.variable import Variable
# from examples.stg.fct_system_fcts_dag.pg_saver import PgSaver
# from lib.mongo_fcts import ProducfctLoader
# from lib.mongo_fcts import ProducfctReader
# from lib.mongo_fcts import ProducfctSaver
# from lib import ConnectionBuilder, MongoConnect

# log = logging.getLogger(__name__)


# @dag(
#     'load_dds_fcts',
#     schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
#     start_date=pendulum.datetime(2023, 11, 4, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
#     catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
#     tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
#     is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
# )


# def sprint5_stg_fct_system_fcts():
#     # Создаем подключение к базе dwh.
#     dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

#     # Получаем переменные из Airflow.
#     cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
#     db_fct = Variable.get("MONGO_DB_USER")
#     db_pw = Variable.get("MONGO_DB_PASSWORD")
#     rs = Variable.get("MONGO_DB_REPLICA_SET")
#     db = Variable.get("MONGO_DB_DATABASE_NAME")
#     host = Variable.get("MONGO_DB_HOST")

#     @task()
#     def get_fcts():
#         pg_connection_source = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

#         with pg_connection_source.connection() as conn:
#             cursor = conn.cursor()
#             cursor.execute('SELECT * FROM stg.fctsystem_fcts;')
#             data = cursor.fetchall()

#         # Convert Decimal values to float for JSON serialization
#         data = [(row[0], row[1], float(row[2]), float(row[3])) for row in data]
#         return data

#     @task()
#     def load_fcts(**kwargs):
#         data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_source')
#         pg_connection_dwh = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

#         with pg_connection_dwh.connection() as conn:
#             cursor = conn.cursor()
#             for row in data:
#                 cursor.execute(
#                     'INSERT INTO stg.bonussystem_fcts (id, name, bonus_percent, min_payment_threshold) VALUES (%s, %s, %s, %s);',
#                     row
#                 )

#     fcts_loader = load_fcts()

#     # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
#     fcts_loader  # type: ignore


# fct_stg_dag = sprint5_stg_fct_system_fcts()  # noqa
