import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

"""
db_utils.py
Este módulo contiene funciones para manejar la conexión a una base de
datos PostgreSQL utilizando SQLAlchemy.
"""


# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Cargar las variables de entorno al inicio
load_dotenv()

# Mapeo lógico de alias a nombres reales de base de datos definidos en el .env
DATABASE_MAP = {
    "default": os.getenv("PG_DATABASE"),
    "dimensional": os.getenv("PG_DATABASE_DIMENSIONAL"),
}

def get_connection(database_name: str = "default"):
    """
    Crea una conexión a la base de datos PostgreSQL utilizando SQLAlchemy.
    Permite cambiar dinámicamente el nombre de la base de datos a partir de un alias lógico.

    :param database_name: Clave lógica de la base de datos ('default', 'dimensional').
    :return: Objeto de conexión (engine).
    """
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")

    dbname = DATABASE_MAP.get(database_name)

    if not all([user, password, host, port, dbname]):
        logging.error("❌ Faltan variables de entorno para la conexión a la base de datos.")
        raise EnvironmentError("Variables de entorno incompletas para la conexión.")

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        engine = create_engine(db_url)
        logging.info(f"✅ Conexión a la base de datos '{dbname}' creada exitosamente.")
        return engine
    except Exception as e:
        logging.error(f"❌ Error al crear el motor de conexión para '{dbname}': {e}")
        raise


def close_connection(engine):
    """
    Cierra la conexión al engine de SQLAlchemy.
    """
    if engine:
        try:
            engine.dispose()
            logging.info("🔌 Conexión al engine cerrada correctamente.")
        except Exception as e:
            logging.error(f"❌ Error al cerrar la conexión: {e}")
    else:
        logging.warning("⚠ No hay engine para cerrar.")
