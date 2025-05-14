import os
import sys
import logging
import tempfile
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Importar funciones de PostgreSQL y transformación
from source.extract.extract import extract_data
from source.transform.transform import transform_accidents_data, split_transformed_data
from source.load.load import create_dimensional_schema, insert_csv_into_table

# Importar funciones de API
from source.extract.extract_api import download_accident_data, download_person_data
from source.extract.extract_api import load_accident_data, load_person_data, merge_accident_person_data
from source.transform.transform_api import transform_data
from source.merge.merge import merge_transformed_data # Nueva función de merge

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id='etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL de accidentes de tráfico',
)

# ✅ Rutas temporales seguras
EXTRACTED_PATH = os.path.join(tempfile.gettempdir(), 'extracted_accidents.csv')
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')
os.makedirs(TRANSFORMED_DIR, exist_ok=True)

# **Tarea: Extracción de datos desde la API FARS**
def task_extract_api():
    """Descarga datos de accidentes y personas desde la API FARS."""
    download_accident_data(output_dir=TRANSFORMED_DIR)
    download_person_data(output_dir=TRANSFORMED_DIR)
    logging.info("✅ Datos extraídos desde la API FARS")

# **Tarea: Extracción de datos desde PostgreSQL**
def task_extract_postgres():
    """Extrae datos desde PostgreSQL y los almacena temporalmente."""
    df = extract_data()
    df.to_csv(EXTRACTED_PATH, index=False)
    logging.info(f"✅ Extracción desde PostgreSQL completada: {EXTRACTED_PATH}")

# **Tarea: Procesamiento de datos de la API**
def process_data():
    """Carga los archivos CSV y fusiona los DataFrames extraídos desde la API."""
    accidents = load_accident_data(input_dir=TRANSFORMED_DIR)
    persons = load_person_data(input_dir=TRANSFORMED_DIR)

    merged_df = merge_accident_person_data(accidents, persons)

    # Guardar el archivo procesado
    output_file = os.path.join(TRANSFORMED_DIR, "merged_fars_data.csv")
    merged_df.to_csv(output_file, index=False)

    logging.info(f"✅ Procesamiento completado. Archivo fusionado guardado: {output_file}")

    return output_file, merged_df

# **Tarea: Transformación de datos de PostgreSQL**
def task_transform_postgres():
    """Transforma datos de accidentes desde PostgreSQL."""
    df = pd.read_csv(EXTRACTED_PATH)
    df_transformed = transform_accidents_data(df)
    split_transformed_data(df_transformed, ruta_salida=TRANSFORMED_DIR)
    logging.info(f"✅ Transformación completada. Archivos guardados en: {TRANSFORMED_DIR}")

# **Tarea: Transformación de datos de la API**
def task_transform_api():
    """Transforma los datos fusionados de la API."""
    input_path = os.path.join(TRANSFORMED_DIR, "merged_fars_data.csv")
    df = pd.read_csv(input_path)
    df_transformed = transform_data(df)
    output_path = os.path.join(TRANSFORMED_DIR, "transformed_api_data.csv")
    df_transformed.to_csv(output_path, index=False)
    logging.info(f"✅ Transformación de datos API completada: {output_path}")

# **Tarea: Merge final**
def task_merge_final():
    """Fusiona los datos transformados desde PostgreSQL y la API externa y los divide en conjuntos procesables."""
    try:
        df_transformed_postgres = pd.read_csv(os.path.join(TRANSFORMED_DIR, "transformed_postgres_data.csv"))
        df_transformed_api = pd.read_csv(os.path.join(TRANSFORMED_DIR, "transformed_api_data.csv"))

        df_final, output_file = merge_transformed_data(df_transformed_postgres, df_transformed_api, ruta_salida=TRANSFORMED_DIR)

        if df_final is not None:
            split_transformed_data(df_final, ruta_salida=TRANSFORMED_DIR)  # División final de datos
            logging.info(f"✅ Merge final completado y datos divididos correctamente. Archivo guardado en: {output_file}")
        else:
            logging.error("❌ Error en la tarea de merge final.")

    except Exception as e:
        logging.error(f"❌ Error en `task_merge_final()`: {e}")

# **Tarea: Carga a la base de datos**
def task_load():
    """Carga los datos transformados en PostgreSQL."""
    create_dimensional_schema()
    insert_csv_into_table()
    logging.info("✅ Carga completada en la base de datos.")

# **Definición de tareas en Airflow**
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
    python_callable=process_data,
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

# Flujo de ejecución en el DAG
extract_api_task >> process_data_task >> transform_api_task  
extract_postgres_task >> transform_postgres_task  

[transform_postgres_task, transform_api_task] >> merge_final_task 