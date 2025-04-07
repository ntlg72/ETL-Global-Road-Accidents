from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import pandas as pd
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from source.extract.extract import extract_data
from source.transform.transform import transform_accidents_data, split_transformed_data
from source.load.load import load_each_table_to_db


# Configuración del DAG
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

# Rutas temporales
EXTRACTED_PATH = '/tmp/extracted_accidents.csv'
TRANSFORMED_PATH = '/tmp/road_accident_dataset_transformed.csv'


# Tarea: Extracción de datos desde PostgreSQL
def task_extract():
    df = extract_data()  # extrae desde la tabla "accidents"
    df.to_csv(EXTRACTED_PATH, index=False)
    logging.info("✅ Extracción completada y guardada en CSV.")


# Tarea: Transformación de datos
def task_transform():
    df = pd.read_csv(EXTRACTED_PATH)
    df_transformed = transform_accidents_data(df)
    split_transformed_data(df_transformed)
    #df_transformed.to_csv(TRANSFORMED_PATH, index=False)

    logging.info("✅ Transformación completada y guardada en CSVs.")

# Tarea: Carga a la base de datos
def task_load():
    load_each_table_to_db()
    logging.info("✅ Carga completada en la base de datos.")

# Definición de tareas en el DAG
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

# Flujo de tareas
extract_task >> transform_task >> load_task

