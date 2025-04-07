"""
db_utils.py
Este módulo contiene funciones para manejar la conexión a una base de
datos PostgreSQL utilizando SQLAlchemy.
"""
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def get_connection():
    """
    Crea una conexión a la base de datos PostgreSQL utilizando SQLAlchemy.
    Retorna un objeto de conexión.
    """
    load_dotenv()
    user = os.getenv('PG_USER')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')

    if not all([user, password, host, port, dbname]):
        logging.error("Faltan variables de entorno para la conexión a la base de datos.")
        raise EnvironmentError("Variables de entorno incompletas para la conexión.")

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        engine = create_engine(db_url)
        logging.info("Engine creado exitosamente.")
        return engine
    except Exception as e:
        logging.error(f"Error al crear el engine: {e}")
        raise


def close_connection(engine):
    """
    Cierra la conexión al engine de SQLAlchemy.
    """
    if engine:
        try:
            engine.dispose()
            logging.info("Conexión al engine cerrada correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {e}")
    else:
        logging.warning("No hay engine para cerrar.")
