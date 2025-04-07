import pandas as pd
import logging
from source.connection_db.db_utils import get_connection, close_connection

"""
Extract module to retrieve data from a database.
"""

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def extract_data():
    """
    Ejecuta una consulta SQL sobre la tabla 'accidents' y retorna los resultados como un DataFrame.

    Returns:
        pd.DataFrame: Resultados de la consulta.
    Raises:
        Exception: Si ocurre un error durante la conexión o extracción.
    """
    engine = get_connection()

    try:
        logging.info("Conexión a la base de datos establecida exitosamente.")
        df = pd.read_sql_table("accidents", engine)
        logging.info(f"{len(df)} registros extraídos de la tabla 'accidents'.")
        return df
    except Exception as e:
        logging.error(f"Error al extraer los datos: {e}")
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info("Conexión cerrada correctamente.")
