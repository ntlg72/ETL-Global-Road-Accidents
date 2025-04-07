import sys
import os
import pandas as pd

# Añadir el path al módulo de conexión
sys.path.append(os.path.abspath('../source'))

from connection_db.db_utils import get_connection, close_connection

def load_data_to_db(df: pd.DataFrame, table_name: str = "accidents"):
    """
    Inserta un DataFrame en la tabla especificada de PostgreSQL.
    Si la tabla existe, la reemplaza.
    """
    engine = None
    try:
        # Conexión
        engine = get_connection()
        print("Conexión establecida correctamente.")

        # Inserción
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"{len(df)} registros insertados en la tabla '{table_name}'.")

    except Exception as e:
        print(f"Error al insertar los datos: {e}")
        raise

    finally:
        if engine is not None:
            close_connection(engine)
            print("Conexión cerrada correctamente.")

