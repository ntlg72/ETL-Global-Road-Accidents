import os
import pandas as pd
import logging
import tempfile
from sqlalchemy import inspect, text
from source.connection_db.db_utils import get_connection, close_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Ruta donde están los CSVs transformados
ruta_salida = os.path.join(tempfile.gettempdir(), "data")


def create_dimensional_schema():
    """
    Crea el esquema dimensional en la base de datos PostgreSQL.
    """
    engine = None
    try:
        engine = get_connection("dimensional")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_lugar (
                    id_lugar SERIAL PRIMARY KEY,
                    country TEXT,
                    urban_rural TEXT,
                    road_type TEXT,
                    road_condition TEXT
                );
                
                CREATE TABLE IF NOT EXISTS dim_fecha (
                    id_fecha SERIAL PRIMARY KEY,
                    year INT,
                    month TEXT,
                    day_of_week TEXT,
                    time_of_day TEXT
                );
                
                CREATE TABLE IF NOT EXISTS dim_condiciones (
                    id_condiciones SERIAL PRIMARY KEY,
                    weather_conditions TEXT,
                    visibility_level TEXT
                );
                
                CREATE TABLE IF NOT EXISTS dim_conductor (
                    id_conductor SERIAL PRIMARY KEY,
                    driver_age_group TEXT,
                    driver_alcohol_level TEXT,
                    driver_fatigue BOOLEAN
                );
                
                CREATE TABLE IF NOT EXISTS dim_incidente (
                    id_incidente SERIAL PRIMARY KEY,
                    accident_severity TEXT,
                    accident_cause TEXT
                );
                
                CREATE TABLE IF NOT EXISTS dim_vehiculo (
                    id_vehiculo SERIAL PRIMARY KEY,
                    vehicle_condition TEXT
                );
                
                CREATE TABLE IF NOT EXISTS hechos_accidentes (
                    id SERIAL PRIMARY KEY,
                    number_of_vehicles_involved INT,
                    speed_limit INT,
                    number_of_injuries INT,
                    number_of_fatalities INT,
                    emergency_response_time FLOAT,
                    traffic_volume FLOAT,
                    pedestrians_involved INT,
                    cyclists_involved INT,
                    population_density FLOAT,
                    id_lugar INT REFERENCES dim_lugar(id_lugar),
                    id_fecha INT REFERENCES dim_fecha(id_fecha),
                    id_condiciones INT REFERENCES dim_condiciones(id_condiciones),
                    id_conductor INT REFERENCES dim_conductor(id_conductor),
                    id_incidente INT REFERENCES dim_incidente(id_incidente),
                    id_vehiculo INT REFERENCES dim_vehiculo(id_vehiculo)
                );
            """))
        logging.info("✅ Esquema dimensional creado correctamente.")
    except Exception as e:
        logging.error(f"❌ Error al crear el esquema dimensional: {e}")
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info("🔌 Conexión cerrada.")


def insert_csv_into_table(ruta_csvs: str = ruta_salida):
    """
    Inserta los datos de los CSVs en las tablas existentes de la base de datos,
    considerando solo los campos que coinciden con cada tabla.
    """
    tablas_csv = {
        "dim_lugar": "dim_lugar.csv",
        "dim_fecha": "dim_fecha.csv",
        "dim_condiciones": "dim_condiciones.csv",
        "dim_conductor": "dim_conductor.csv",
        "dim_incidente": "dim_incidente.csv",
        "dim_vehiculo": "dim_vehiculo.csv",
        "hechos_accidentes": "hechos_accidentes.csv"
    }

    engine = None
    try:
        engine = get_connection("dimensional")
        inspector = inspect(engine)

        for tabla, archivo in tablas_csv.items():
            path_archivo = os.path.join(ruta_csvs, archivo)
            if not os.path.exists(path_archivo):
                logging.warning(f"⚠️ Archivo no encontrado: {path_archivo}")
                continue

            logging.info(f"📄 Leyendo archivo: {archivo}")
            df = pd.read_csv(path_archivo)

            # Obtener columnas reales de la tabla en la base de datos
            columnas_bd = [col["name"] for col in inspector.get_columns(tabla)]

            # Filtrar el DataFrame con solo las columnas que coinciden
            columnas_validas = [col for col in df.columns if col in columnas_bd]
            df_filtrado = df[columnas_validas]

            # Insertar datos sin reemplazar la tabla
            df_filtrado.to_sql(name=tabla, con=engine, if_exists='replace', index=False)
            logging.info(f"✅ Datos insertados en '{tabla}' ({len(df_filtrado)} filas).")

    except Exception as e:
        logging.error(f"❌ Error al insertar datos en las tablas: {e}")
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info("🔌 Conexión cerrada.")
