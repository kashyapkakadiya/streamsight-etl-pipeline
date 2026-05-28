import sys
import os
sys.path.insert(0, '/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# ──────────────────────────────────────────
# DEFAULT ARGUMENTS
# ──────────────────────────────────────────
default_args = {
    'owner': 'kashyap',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


# ──────────────────────────────────────────
# TASK FUNCTIONS
# ──────────────────────────────────────────
def run_extract(**context):
    from extract import extract
    filepath = '/opt/airflow/data/Most Streamed Spotify Songs 2024.csv'
    df = extract(filepath)
    # Save to XCom as JSON for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"[DAG] Extract complete. Rows: {len(df)}")


def run_transform(**context):
    from transform import transform
    # Pull raw data from XCom
    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    df_raw = pd.read_json(raw_json)
    df_clean = transform(df_raw)
    # Push cleaned data to XCom
    context['ti'].xcom_push(key='clean_data', value=df_clean.to_json())
    print(f"[DAG] Transform complete. Rows: {len(df_clean)}")


def run_load(**context):
    from load import load
    import pandas as pd
    # Pull clean data from XCom
    clean_json = context['ti'].xcom_pull(key='clean_data', task_ids='transform_task')
    df_clean = pd.read_json(clean_json)

    # Create spotify_elt database if it doesn't exist
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM pg_database WHERE datname = 'spotify_elt'
    """)
    if not cursor.fetchone():
        cursor.execute("CREATE DATABASE spotify_elt;")
        print("[DAG] Created database: spotify_elt")
    cursor.close()
    conn.close()

    load(df_clean, table_name='spotify_songs')
    print(f"[DAG] Load complete. Rows loaded: {len(df_clean)}")


def run_verify(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM spotify_songs;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    print(f"[DAG] Verification complete. Total rows in DB: {count}")
    if count == 0:
        raise ValueError("Load verification failed — 0 rows found in spotify_songs")


# ──────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────
with DAG(
    dag_id='spotify_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for Spotify Songs 2024 dataset',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spotify', 'elt', 'postgresql'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform,
    )

    verify_task = PythonOperator(
        task_id='verify_task',
        python_callable=run_verify,
    )

    # Task dependency chain
    extract_task >> load_task >> transform_task >> verify_task
