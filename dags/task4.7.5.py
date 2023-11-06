import logging

import pendulum
from airflow.decorators import dag, task
from lib.dds_products import ProductLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    'dds_load_products',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_products_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    products_dict = load_products()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    products_dict  # type: ignore


stg_bonus_system_products_dag = sprint5_dds_products_dag()





# import logging

# import pendulum
# from airflow.decorators import dag, task
# from airflow.models.variable import Variable
# from examples.stg.product_system_products_dag.pg_saver import PgSaver
# from lib.mongo_products import ProducproductLoader
# from lib.mongo_products import ProducproductReader
# from lib.mongo_products import ProducproductSaver
# from lib import ConnectionBuilder, MongoConnect

# log = logging.getLogger(__name__)


# @dag(
#     'load_dds_products',
#     schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
#     start_date=pendulum.datetime(2023, 11, 4, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
#     catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
#     tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
#     is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
# )


# def sprint5_stg_product_system_products():
#     # Создаем подключение к базе dwh.
#     dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

#     # Получаем переменные из Airflow.
#     cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
#     db_product = Variable.get("MONGO_DB_USER")
#     db_pw = Variable.get("MONGO_DB_PASSWORD")
#     rs = Variable.get("MONGO_DB_REPLICA_SET")
#     db = Variable.get("MONGO_DB_DATABASE_NAME")
#     host = Variable.get("MONGO_DB_HOST")

#     @task()
#     def get_products():
#         pg_connection_source = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

#         with pg_connection_source.connection() as conn:
#             cursor = conn.cursor()
#             cursor.execute('SELECT * FROM stg.productsystem_products;')
#             data = cursor.fetchall()

#         # Convert Decimal values to float for JSON serialization
#         data = [(row[0], row[1], float(row[2]), float(row[3])) for row in data]
#         return data

#     @task()
#     def load_products(**kwargs):
#         data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_source')
#         pg_connection_dwh = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

#         with pg_connection_dwh.connection() as conn:
#             cursor = conn.cursor()
#             for row in data:
#                 cursor.execute(
#                     'INSERT INTO stg.bonussystem_products (id, name, bonus_percent, min_payment_threshold) VALUES (%s, %s, %s, %s);',
#                     row
#                 )

#     products_loader = load_products()

#     # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
#     products_loader  # type: ignore


# product_stg_dag = sprint5_stg_product_system_products()  # noqa
