import os
import sys
import logging
import tempfile
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(tempfile.gettempdir(), "airflow.log")),
        logging.StreamHandler()
    ]
)

# Añadir el directorio raíz al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data.producer import send_hechos_to_kafka
from source.extract.extract import extract_data
from source.transform.transform import transform_accidents_data
from source.load.load import create_dimensional_schema, procesar_y_guardar_modelo_dimensional, insert_csv_into_table
from source.extract.extract_api import download_accident_data, download_person_data, load_accident_data, load_person_data, merge_accident_person_data
from source.transform.transform_api import transform_data
from source.merge.merge import merge_transformed_data
from source.gx.gx import ejecutar_validaciones

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 18),
    'retries': 2,
    'retry_delay': 300,  # 5 minutos
    'depends_on_past': False
}

dag = DAG(
    dag_id='etl_accidents_pipeline',
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
    description='ETL de accidentes de tráfico con streaming a Kafka',
)

# Rutas temporales seguras
EXTRACTED_PATH = os.path.join(tempfile.gettempdir(), 'extracted_accidents.csv')
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')
os.makedirs(TRANSFORMED_DIR, exist_ok=True)
GX_CSV_PATH = os.path.join(TRANSFORMED_DIR, "merge_accidents_data.csv")
GX_RESULTS_DIR = os.path.join(tempfile.gettempdir(), 'gx_results')
os.makedirs(GX_RESULTS_DIR, exist_ok=True)

# Tarea: Extracción de datos desde la API FARS
def task_extract_api():
    try:
        download_accident_data(output_dir=TRANSFORMED_DIR)
        download_person_data(output_dir=TRANSFORMED_DIR)
        logging.info("✅ Datos extraídos desde la API FARS")
    except Exception as e:
        logging.error(f"❌ Error en task_extract_api: {e}")
        raise

# Tarea: Extracción de datos desde PostgreSQL
def task_extract_postgres():
    try:
        df = extract_data()
        if df.empty:
            raise ValueError("No se extrajeron datos desde PostgreSQL")
        df.to_csv(EXTRACTED_PATH, index=False)
        logging.info(f"✅ Extracción desde PostgreSQL completada: {EXTRACTED_PATH}")
    except Exception as e:
        logging.error(f"❌ Error en task_extract_postgres: {e}")
        raise

# Tarea: Procesamiento de datos de la API
def task_process_data():
    try:
        accidents = load_accident_data(input_dir=TRANSFORMED_DIR)
        persons = load_person_data(input_dir=TRANSFORMED_DIR)
        merged_df = merge_accident_person_data(accidents, persons)
        output_file = os.path.join(TRANSFORMED_DIR, "merged_fars_data.csv")
        merged_df.to_csv(output_file, index=False)
        logging.info(f"✅ Procesamiento completado. Archivo fusionado guardado: {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"❌ Error en task_process_data: {e}")
        raise

# Tarea: Transformación de datos de PostgreSQL
def task_transform_postgres():
    try:
        df = pd.read_csv(EXTRACTED_PATH)
        df_transformed = transform_accidents_data(df)
        output_path = os.path.join(TRANSFORMED_DIR, "transformed_postgres_data.csv")
        df_transformed.to_csv(output_path, index=False)
        logging.info(f"✅ Transformación desde PostgreSQL completada: {output_path}")
    except Exception as e:
        logging.error(f"❌ Error en task_transform_postgres: {e}")
        raise

# Tarea: Transformación de datos de la API
def task_transform_api():
    try:
        input_path = os.path.join(TRANSFORMED_DIR, "merged_fars_data.csv")
        df = pd.read_csv(input_path)
        df_transformed = transform_data(df)
        output_path = os.path.join(TRANSFORMED_DIR, "transformed_api_data.csv")
        df_transformed.to_csv(output_path, index=False)
        logging.info(f"✅ Transformación de datos API completada: {output_path}")
    except Exception as e:
        logging.error(f"❌ Error en task_transform_api: {e}")
        raise

# Tarea: Merge final
def task_merge_final():
    try:
        df_transformed_postgres = pd.read_csv(os.path.join(TRANSFORMED_DIR, "transformed_postgres_data.csv"))
        df_transformed_api = pd.read_csv(os.path.join(TRANSFORMED_DIR, "transformed_api_data.csv"))
        df_final, output_file = merge_transformed_data(df_transformed_postgres, df_transformed_api, ruta_salida=TRANSFORMED_DIR)
        if df_final is None or output_file is None:
            raise ValueError("Merge final falló: df_final o output_file es None")
        logging.info(f"✅ Merge final completado. Archivo combinado guardado en: {output_file}")
    except Exception as e:
        logging.error(f"❌ Error en task_merge_final: {e}")
        raise

# Tarea: Validación con Great Expectations
def task_gx_validation():
    try:
        logging.info("▶️ Ejecutando validaciones con Great Expectations...")
        ejecutar_validaciones(GX_CSV_PATH, GX_RESULTS_DIR)
        logging.info("✅ Validaciones Great Expectations completadas.")
    except Exception as e:
        logging.error(f"❌ Error en task_gx_validation: {e}")
        raise

# Tarea: Carga a la base de datos
def task_load():
    try:
        create_dimensional_schema()
        df_final = pd.read_csv(os.path.join(TRANSFORMED_DIR, "merge_accidents_data.csv"))
        procesar_y_guardar_modelo_dimensional(df_final, TRANSFORMED_DIR)
        insert_csv_into_table(ruta_csvs=TRANSFORMED_DIR)
        logging.info("✅ Carga completada en la base de datos.")
    except Exception as e:
        logging.error(f"❌ Error en task_load: {e}")
        raise

# Tarea: Enviar datos a Kafka
def task_send_hechos_dimensiones_to_kafka():
    try:
        hechos_path = os.path.join(TRANSFORMED_DIR, "hechos_accidentes.csv")
        logging.info(f"📂 Cargando dataset desde: {hechos_path}")
        df = pd.read_csv(hechos_path, dtype=str)
        if df.empty:
            raise ValueError("El archivo hechos_accidentes.csv está vacío")
        send_hechos_to_kafka(df, topic="road_accidents", sleep_seconds=0.5)
        logging.info("✅ Transmisión de datos a Kafka completada.")
    except Exception as e:
        logging.error(f"❌ Error en task_send_hechos_dimensiones_to_kafka: {e}")
        raise



# Definición de tareas en Airflow
extract_api_task = PythonOperator(
    task_id='extract_data_api',
    python_callable=task_extract_api,
    dag=dag,
)

extract_postgres_task = PythonOperator(
    task_id='extract_data_postgres',
    python_callable=task_extract_postgres,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_api_data',
    python_callable=task_process_data,
    dag=dag,
)

transform_postgres_task = PythonOperator(
    task_id='transform_postgres_data',
    python_callable=task_transform_postgres,
    dag=dag,
)

transform_api_task = PythonOperator(
    task_id='transform_api_data',
    python_callable=task_transform_api,
    dag=dag,
)

merge_final_task = PythonOperator(
    task_id='merge_transformed_data',
    python_callable=task_merge_final,
    dag=dag,
)

gx_validation_task = PythonOperator(
    task_id='validate_data_great_expectations',
    python_callable=task_gx_validation,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=task_load,
    dag=dag,
)

kafka_task = PythonOperator(
    task_id='kafka_stream',
    python_callable=task_send_hechos_dimensiones_to_kafka,
    dag=dag,
)


# Flujo actualizado
extract_api_task >> process_data_task >> transform_api_task
extract_postgres_task >> transform_postgres_task
[transform_postgres_task, transform_api_task] >> merge_final_task >> gx_validation_task >> load_task >> kafka_task 
