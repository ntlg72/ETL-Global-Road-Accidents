import os
import requests
import logging
import tempfile  # Importamos tempfile para definir la ruta temporal
import pandas as pd

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


def load_accident_data(from_year=2017, to_year=2022, input_dir=TRANSFORMED_DIR):
    """
    Carga los datos de accidentes FARS desde archivos CSV y los concatena en un solo DataFrame.

    Args:
        from_year (int): Año inicial del rango de carga (por defecto: 2017).
        to_year (int): Año final del rango de carga (por defecto: 2022).
        input_dir (str): Directorio donde se almacenan los archivos CSV.

    Returns:
        pd.DataFrame: DataFrame combinado con los datos de accidentes de todos los años especificados.
    """
    dataframes = []

    for year in range(from_year, to_year + 1):
        file_path = os.path.join(input_dir, f'FARS_accident_{year}.csv')  # Ajuste de ruta
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df["Year"] = year  # Agregar columna de referencia de año
            dataframes.append(df)
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

    if dataframes:
        accidents = pd.concat(dataframes, ignore_index=True)
        return accidents
    else:
        print("❌ No se encontraron archivos de accidentes.")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error

def load_person_data(from_year=2017, to_year=2022, input_dir=TRANSFORMED_DIR):
    """
    Carga los datos de personas FARS desde archivos CSV y los concatena en un solo DataFrame.

    Args:
        from_year (int): Año inicial del rango de carga (por defecto: 2017).
        to_year (int): Año final del rango de carga (por defecto: 2022).
        input_dir (str): Directorio donde se almacenan los archivos CSV.

    Returns:
        pd.DataFrame: DataFrame combinado con los datos de personas de todos los años especificados.
    """
    dataframes = []

    for year in range(from_year, to_year + 1):
        file_path = os.path.join(input_dir, f'FARS_person_{year}.csv')  # Ajuste de ruta
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df["Year"] = year  # Agregar columna de referencia de año
            dataframes.append(df)
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

    if dataframes:
        persons = pd.concat(dataframes, ignore_index=True)
        return persons
    else:
        print("❌ No se encontraron archivos de personas.")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error


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