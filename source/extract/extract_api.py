import os
import requests
import logging
import tempfile  # Importamos tempfile para definir la ruta temporal
import pandas as pd
import sqlite3

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Definir el directorio de salida temporal para Airflow DAGs
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')
os.makedirs(TRANSFORMED_DIR, exist_ok=True)  # Crear la carpeta si no existe

def download_accident_data(from_year=2017, to_year=2022, output_dir=TRANSFORMED_DIR):
    """
    Descarga datos de accidentes desde la API FARS (NHTSA) y los guarda como archivos CSV.

    Args:
        from_year (int): Año inicial del rango de descarga (por defecto: 2017).
        to_year (int): Año final del rango de descarga (por defecto: 2022).
        output_dir (str): Directorio donde se guardarán los archivos CSV 
                          (por defecto: `TRANSFORMED_DIR` en la ruta temporal del sistema).

    Returns:
        None: Los archivos descargados se guardan en `output_dir`, pero no se devuelve ninguna salida.
    """
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"
    headers = {
        "Accept": "text/csv",
        "User-Agent": "Mozilla/5.0"
    }

    os.makedirs(output_dir, exist_ok=True)

    for year in range(2017, 2023):  # Corregido el rango
        url = f"{base_url}?dataset=Accident&FromYear={year}&ToYear={year}&State=*&format=csv"
        logging.info(f"Descargando datos de Accidentes para el año {year}...")

        try:
            response = requests.get(url, headers=headers, timeout=600, stream=True)

            if response.status_code == 200:
                output_file = os.path.join(output_dir, f"FARS_accident_{year}.csv")
                with open(output_file, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                logging.info(f"✔️ Archivo guardado: {output_file}")
            else:
                logging.error(f"❌ Error HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            logging.warning(f"⏱️ Timeout para el año {year}")
        except requests.exceptions.RequestException as e:
            logging.error(f"⚠️ Error al descargar datos de Accidentes para {year}: {e}")

def download_person_data(from_year=2017, to_year=2022, output_dir=TRANSFORMED_DIR):
    """
    Descarga datos de personas desde la API FARS (NHTSA) y los guarda como archivos CSV.

    Args:
        from_year (int): Año inicial del rango de descarga (por defecto: 2017).
        to_year (int): Año final del rango de descarga (por defecto: 2022).
        output_dir (str): Directorio donde se guardarán los archivos CSV 
                          (por defecto: `TRANSFORMED_DIR` en la ruta temporal del sistema).

    Returns:
        None: Los archivos descargados se guardan en `output_dir`, pero no se devuelve ninguna salida.
    """
    base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"
    headers = {
        "Accept": "text/csv",
        "User-Agent": "Mozilla/5.0"
    }

    os.makedirs(output_dir, exist_ok=True)

    for year in range(2017, 2023):  # Corregido el rango
        url = f"{base_url}?dataset=Person&FromYear={year}&ToYear={year}&State=*&format=csv"
        logging.info(f"Descargando datos de Personas para el año {year}...")

        try:
            response = requests.get(url, headers=headers, timeout=600, stream=True)

            if response.status_code == 200:
                output_file = os.path.join(output_dir, f"FARS_person_{year}.csv")
                with open(output_file, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                logging.info(f"✔️ Archivo guardado: {output_file}")
            else:
                logging.error(f"❌ Error HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            logging.warning(f"⏱️ Timeout para el año {year}")
        except requests.exceptions.RequestException as e:
            logging.error(f"⚠️ Error al descargar datos de Personas para {year}: {e}")


def load_accident_data(from_year=2017, to_year=2022, input_dir=TRANSFORMED_DIR, batch_size=10000):
    """
    Carga los datos de accidentes FARS desde archivos CSV y los concatena en un solo DataFrame
    usando una base de datos temporal para optimizar memoria.
    """
    conn = sqlite3.connect(':memory:')
    table_name = 'accidents'

    for year in range(from_year, to_year + 1):
        file_path = os.path.join(input_dir, f'FARS_accident_{year}.csv')
        if os.path.exists(file_path):
            chunks = pd.read_csv(file_path, low_memory=False, chunksize=batch_size)
            for i, chunk in enumerate(chunks):
                # Depuración: Mostrar columnas existentes
                logging.info(f"Procesando chunk {i} para el año {year}. Columnas: {chunk.columns.tolist()}")
                # Verificar si existe 'Year' o 'year' (insensible a mayúsculas)
                year_column_exists = any(col.lower() == 'year' for col in chunk.columns)
                if not year_column_exists:
                    chunk["Year"] = year
                    logging.info(f"Agregada columna Year con valor {year}")
                else:
                    # Encontrar el nombre exacto de la columna (para depuración)
                    existing_year_col = next(col for col in chunk.columns if col.lower() == 'year')
                    logging.info(f"Columna '{existing_year_col}' ya existe, no se agrega. Valores únicos: {chunk[existing_year_col].unique()}")
                if i == 0:
                    chunk.to_sql(table_name, conn, if_exists='replace', index=False)
                else:
                    chunk.to_sql(table_name, conn, if_exists='append', index=False)
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

    try:
        accidents = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        return accidents
    except Exception as e:
        print(f"❌ Error al leer la tabla: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def load_person_data(from_year=2017, to_year=2022, input_dir=TRANSFORMED_DIR, batch_size=10000):
    """
    Carga los datos de personas FARS desde archivos CSV y los concatena en un solo DataFrame
    usando una base de datos temporal para optimizar memoria.
    """
    conn = sqlite3.connect(':memory:')
    table_name = 'persons'

    for year in range(from_year, to_year + 1):
        file_path = os.path.join(input_dir, f'FARS_person_{year}.csv')
        if os.path.exists(file_path):
            chunks = pd.read_csv(file_path, low_memory=False, chunksize=batch_size)
            for i, chunk in enumerate(chunks):
                # Depuración: Mostrar columnas existentes
                logging.info(f"Procesando chunk {i} para el año {year}. Columnas: {chunk.columns.tolist()}")
                # Verificar si existe 'Year' o 'year' (insensible a mayúsculas)
                year_column_exists = any(col.lower() == 'year' for col in chunk.columns)
                if not year_column_exists:
                    chunk["Year"] = year
                    logging.info(f"Agregada columna Year con valor {year}")
                else:
                    # Encontrar el nombre exacto de la columna (para depuración)
                    existing_year_col = next(col for col in chunk.columns if col.lower() == 'year')
                    logging.info(f"Columna '{existing_year_col}' ya existe, no se agrega. Valores únicos: {chunk[existing_year_col].unique()}")
                if i == 0:
                    chunk.to_sql(table_name, conn, if_exists='replace', index=False)
                else:
                    chunk.to_sql(table_name, conn, if_exists='append', index=False)
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

    try:
        persons = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        return persons
    except Exception as e:
        print(f"❌ Error al leer la tabla: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def merge_accident_person_data(accidents, persons):
    """
    Realiza un merge entre los datos de accidentes y personas basado en 'st_case'.
    
    Args:
        accidents (pd.DataFrame): DataFrame con datos de accidentes FARS.
        persons (pd.DataFrame): DataFrame con datos de personas FARS.

    Returns:
        pd.DataFrame: DataFrame combinado con información de accidentes y detalles de personas.
    """
    merged_df = accidents.merge(persons[['st_case', 'age', 'sex', 'alc_res']], 
                                on='st_case', 
                                how='left')
    
    return merged_df