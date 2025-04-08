import pandas as pd
from source.connection_db.db_utils import get_connection, close_connection
import logging
import os
import tempfile

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Ruta temporal donde se guardaron los CSV transformados
ruta_salida = os.path.join(tempfile.gettempdir(), "data")


def load_data_to_db(df: pd.DataFrame, table_name: str):
    """
    Inserta un DataFrame en la tabla especificada de PostgreSQL.
    Si la tabla existe, la reemplaza.
    """
    engine = None
    try:
        engine = get_connection("dimensional")
        logging.info(f"✅ Conexión establecida correctamente para la tabla '{table_name}'.")
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"📥 {len(df)} registros insertados en la tabla '{table_name}'.")
    except Exception as e:
        logging.error(f"❌ Error al insertar los datos en '{table_name}': {e}")
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info(f"🔌 Conexión cerrada correctamente para la tabla '{table_name}'.\n")


def load_each_table_to_db(ruta: str = ruta_salida):
    tablas = {
        "dim_lugar": "dim_lugar.csv",
        "dim_fecha": "dim_fecha.csv",
        "dim_condiciones": "dim_condiciones.csv",
        "dim_conductor": "dim_conductor.csv",
        "dim_incidente": "dim_incidente.csv",
        "dim_vehiculo": "dim_vehiculo.csv",
        "hechos_accidentes": "hechos_accidentes.csv"
    }

    for tabla, archivo_csv in tablas.items():
        try:
            path_archivo = os.path.join(ruta, archivo_csv)
            logging.info(f"🔎 Leyendo archivo: {path_archivo}")
            df = pd.read_csv(path_archivo)
            load_data_to_db(df, table_name=tabla)
        except Exception as e:
            logging.warning(f"⚠️ Error al procesar '{tabla}': {e}\n")
