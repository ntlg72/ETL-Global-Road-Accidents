from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import pandas as pd
import logging
import tempfile 

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from source.extract.extract import extract_data
from source.transform.transform import transform_accidents_data, split_transformed_data
from source.load.load import *

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

# ✅ Rutas temporales seguras
EXTRACTED_PATH = os.path.join(tempfile.gettempdir(), 'extracted_accidents.csv')
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')

# Tarea: Extracción de datos desde PostgreSQL
def task_extract():
    df = extract_data()
    df.to_csv(EXTRACTED_PATH, index=False)
    logging.info(f"✅ Extracción completada: {EXTRACTED_PATH}")

# Tarea: Transformación de datos
def task_transform():
    df = pd.read_csv(EXTRACTED_PATH)
    df_transformed = transform_accidents_data(df)
    split_transformed_data(df_transformed, ruta_salida=TRANSFORMED_DIR)
    logging.info(f"✅ Transformación completada. Archivos guardados en: {TRANSFORMED_DIR}")

# Tarea: Carga a la base de datos
def task_load():
    create_dimensional_schema()
    insert_csv_into_table()
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
