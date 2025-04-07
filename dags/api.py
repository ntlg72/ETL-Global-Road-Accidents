from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import pandas as pd

# Añadir al path los scripts del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../source/extract')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../source/transform')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../source/load')))

from extract import extract_data
from transform import transform_accidents_data
from load import load_data_to_db

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='etl_accidents_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL de accidentes de tráfico a PostgreSQL',
)

# Funciones Python para las tareas
def task_extract():
    df = extract_data()
    df.to_csv('/tmp/extracted_accidents.csv', index=False)
    print("✅ Extracción completada")

def task_transform():
    df = pd.read_csv('/tmp/extracted_accidents.csv')
    df_transformed = transform_accidents_data(df)
    df_transformed.to_csv('/tmp/road_accident_dataset_transformed.csv', index=False)
    print("✅ Transformación completada")

def task_load():
    df = pd.read_csv('/tmp/road_accident_dataset_transformed.csv')
    load_data_to_db(df)
    print("✅ Carga completada")

# Operadores del DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=task_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=task_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=task_load,
    dag=dag,
)

# Flujo del DAG
extract_task >> transform_task >> load_task

