from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.pg_connect import ConnectionBuilder
import json

# Define the DAG
dag = DAG(
    'load_ranks_and_users_to_dwh',
    default_args={
        'owner': 'Vladislav Panferov',
        'start_date': datetime(2023, 11, 3),
        'schedule_interval': '*/15 * * * *',  # Schedule to run every 15 minutes
    },
    catchup=False,
)

# Create a Python function to fetch data from the source database
def fetch_ranks_from_source():
    pg_connection_source = ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')

    with pg_connection_source.connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM public.ranks;')
        data = cursor.fetchall()

    # Convert Decimal values to float for JSON serialization
    data = [(row[0], row[1], float(row[2]), float(row[3])) for row in data]
    return data

def fetch_users_from_source():
    pg_connection_source = ConnectionBuilder.pg_conn('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')

    with pg_connection_source.connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, order_user_id FROM public.users;')
        data = cursor.fetchall()

    # Convert Decimal values to float for JSON serialization
    data = [(row[1]) for row in data]

    return data


# Define the task to fetch data from the source
fetch_ranks_task = PythonOperator(
    task_id='fetch_ranks_from_source',
    python_callable=fetch_ranks_from_source,
    dag=dag,
)

fetch_users_task = PythonOperator(
    task_id='fetch_users_from_source',
    python_callable=fetch_users_from_source,
    dag=dag,
)

# Create a Python function to load data into the DWH
def load_ranks_to_dwh(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_ranks_from_source')
    pg_connection_dwh = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

    with pg_connection_dwh.connection() as conn:
        cursor = conn.cursor()
        for row in data:
            cursor.execute(
                'INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold) VALUES (%s, %s, %s, %s);',
                row
            )

def load_users_to_dwh(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_users_from_source')
    pg_connection_dwh = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

    with pg_connection_dwh.connection() as conn:
        cursor = conn.cursor()
        for row in data:
            cursor.execute(
                'INSERT INTO stg.bonussystem_users (order_user_id) VALUES (%s);',
                (row,)  # Wrap the single value in a tuple
            )



# Define the task to load data into the DWH
load_ranks_to_dwh_task = PythonOperator(
    task_id='load_ranks_to_dwh',
    python_callable=load_ranks_to_dwh,
    provide_context=True,
    dag=dag,
)

load_users_to_dwh_task = PythonOperator(
    task_id='load_users_to_dwh',
    python_callable=load_users_to_dwh,
    provide_context=True,
    dag=dag,
)



# Set task dependencies
[fetch_ranks_task, fetch_users_task] >> load_ranks_to_dwh_task >> load_users_to_dwh_task

if __name__ == "__main__":
    dag.cli()
