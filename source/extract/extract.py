import sys
import os
import pandas as pd

# Añadir el path al módulo de conexión
sys.path.append(os.path.abspath('../source'))

from connection_db.db_utils import get_connection, close_connection


def extract_data(query: str) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL y retorna los resultados como un DataFrame.
    """
    engine = None
    try:
        engine = get_connection()
        print("Conexión a la base de datos establecida exitosamente.")

        df = pd.read_sql_query(query, con=engine)
        print(f"{len(df)} registros extraídos.")
        return df

    except Exception as e:
        print(f"Error al extraer los datos: {e}")
        raise

    finally:
        if engine is not None:
            close_connection(engine)
            print("Conexión cerrada correctamente.")


