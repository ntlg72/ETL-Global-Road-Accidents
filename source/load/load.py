import os
import pandas as pd
import logging
import hashlib
import tempfile
from sqlalchemy import inspect, text
from source.connection_db.db_utils import get_connection, close_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Ruta donde están los CSVs transformados
ruta_salida = os.path.join(tempfile.gettempdir(), "data")


def generar_id_unico(registro):
    """Genera un ID único dentro del rango de INTEGER"""
    cadena = ''.join(str(v) for v in registro.values)
    # Usamos los primeros 7 caracteres hex (28 bits) para asegurar que sea < 2^31-1
    return int(hashlib.sha256(cadena.encode()).hexdigest()[:7], 16)

def procesar_y_guardar_modelo_dimensional(df, ruta_salida):
    # 1. Crear directorio de salida si no existe
    os.makedirs(ruta_salida, exist_ok=True)
    
    # 2. Procesar cada dimensión y generar IDs únicos
    dimension_tables = {}
    
    # Dimensión Lugar
    dim_lugar = df[["country", "urban_rural", "road_type", "road_condition"]].drop_duplicates()
    dim_lugar['id_lugar'] = [generar_id_unico(row) for _, row in dim_lugar.iterrows()]
    dimension_tables['lugar'] = dim_lugar
    
    # Dimensión Fecha
    dim_fecha = df[["year", "month", "day_of_week", "time_of_day"]].drop_duplicates()
    dim_fecha['id_fecha'] = [generar_id_unico(row) for _, row in dim_fecha.iterrows()]
    dimension_tables['fecha'] = dim_fecha
    
    # Dimensión Condiciones
    dim_condiciones = df[["weather_conditions", "visibility_level"]].drop_duplicates()
    dim_condiciones['id_condiciones'] = [generar_id_unico(row) for _, row in dim_condiciones.iterrows()]
    dimension_tables['condiciones'] = dim_condiciones
    
    # Dimensión Conductor
    dim_conductor = df[["driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender"]].drop_duplicates()
    dim_conductor['id_conductor'] = [generar_id_unico(row) for _, row in dim_conductor.iterrows()]
    dimension_tables['conductor'] = dim_conductor
    
    # Dimensión Incidente
    dim_incidente = df[["accident_severity", "accident_cause"]].drop_duplicates()
    dim_incidente['id_incidente'] = [generar_id_unico(row) for _, row in dim_incidente.iterrows()]
    dimension_tables['incidente'] = dim_incidente
    
    # Dimensión Vehículo
    dim_vehiculo = df[["vehicle_condition"]].drop_duplicates()
    dim_vehiculo['id_vehiculo'] = [generar_id_unico(row) for _, row in dim_vehiculo.iterrows()]
    dimension_tables['vehiculo'] = dim_vehiculo
    
    # 3. Construir la tabla de hechos
    hechos = df.copy()
    
    # Merge para cada dimensión
    merge_cols = {
        'lugar': ["country", "urban_rural", "road_type", "road_condition"],
        'fecha': ["year", "month", "day_of_week", "time_of_day"],
        'condiciones': ["weather_conditions", "visibility_level"],
        'conductor': ["driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender"],
        'incidente': ["accident_severity", "accident_cause"],
        'vehiculo': ["vehicle_condition"]
    }
    
    for dim_name, cols in merge_cols.items():
        hechos = pd.merge(hechos, dimension_tables[dim_name], on=cols)
    
    # Seleccionar columnas para la tabla de hechos
    hechos_df = hechos[[
        "number_of_vehicles_involved", "speed_limit", "number_of_injuries", "number_of_fatalities",
        "emergency_response_time", "traffic_volume", "pedestrians_involved", "cyclists_involved",
        "population_density", "id_lugar", "id_fecha", "id_condiciones", "id_conductor",
        "id_incidente", "id_vehiculo"
    ]].copy()
    
    # Generar ID único para cada hecho
    hechos_df['id_hecho'] = [generar_id_unico(row) for _, row in hechos_df.iterrows()]

    # Mover id_hecho al principio
    columnas = ["id_hecho"] + [col for col in hechos_df.columns if col != "id_hecho"]
    hechos_df = hechos_df.reindex(columns=columnas)
    
    # 4. Guardar todas las tablas a CSV
    # Guardar dimensiones
    for dim_name, dim_df in dimension_tables.items():
        dim_df.to_csv(os.path.join(ruta_salida, f"dim_{dim_name}.csv"), index=False)
    
    # Guardar hechos
    hechos_df.to_csv(os.path.join(ruta_salida, "hechos_accidentes.csv"), index=False)
    
    return dimension_tables, hechos_df

def create_dimensional_schema():
    """
    Crea el esquema dimensional en PostgreSQL con BIGINT para los IDs y limpia las tablas antes de la carga.
    """
    engine = None
    try:
        engine = get_connection("dimensional")
        with engine.begin() as conn:
            
            conn.execute(text("""
                DROP TABLE IF EXISTS hechos_accidentes CASCADE;
                DROP TABLE IF EXISTS dim_lugar CASCADE;
                DROP TABLE IF EXISTS dim_fecha CASCADE;
                DROP TABLE IF EXISTS dim_condiciones CASCADE;
                DROP TABLE IF EXISTS dim_conductor CASCADE;
                DROP TABLE IF EXISTS dim_incidente CASCADE;
                DROP TABLE IF EXISTS dim_vehiculo CASCADE;
            """))

            # Crear las tablas con BIGINT para los IDs
            conn.execute(text("""
                CREATE TABLE dim_lugar (
                    id_lugar BIGSERIAL PRIMARY KEY,
                    country TEXT,
                    urban_rural TEXT,
                    road_type TEXT,
                    road_condition TEXT,
                    CONSTRAINT unique_lugar UNIQUE (country, urban_rural, road_type, road_condition)
                );

                CREATE TABLE dim_fecha (
                    id_fecha BIGSERIAL PRIMARY KEY,
                    year INT,
                    month TEXT,
                    day_of_week TEXT,
                    time_of_day TEXT,
                    CONSTRAINT unique_fecha UNIQUE (year, month, day_of_week, time_of_day)
                );

                CREATE TABLE dim_condiciones (
                    id_condiciones BIGSERIAL PRIMARY KEY,
                    weather_conditions TEXT,
                    visibility_level TEXT,
                    CONSTRAINT unique_condiciones UNIQUE (weather_conditions, visibility_level)
                );

                CREATE TABLE dim_conductor (
                    id_conductor BIGSERIAL PRIMARY KEY,
                    driver_age_group TEXT,
                    driver_alcohol_level TEXT,
                    driver_fatigue INTEGER,
                    driver_gender TEXT,
                    CONSTRAINT unique_conductor UNIQUE (driver_age_group, driver_alcohol_level, driver_fatigue, driver_gender)
                );

                CREATE TABLE dim_incidente (
                    id_incidente BIGSERIAL PRIMARY KEY,
                    accident_severity TEXT,
                    accident_cause TEXT,
                    CONSTRAINT unique_incidente UNIQUE (accident_severity, accident_cause)
                );

                CREATE TABLE dim_vehiculo (
                    id_vehiculo BIGSERIAL PRIMARY KEY,
                    vehicle_condition TEXT,
                    CONSTRAINT unique_vehiculo UNIQUE (vehicle_condition)
                );

                CREATE TABLE hechos_accidentes (
                    id_hecho BIGSERIAL PRIMARY KEY,
                    number_of_vehicles_involved INT,
                    speed_limit INT,
                    number_of_injuries INT,
                    number_of_fatalities INT,
                    emergency_response_time FLOAT,
                    traffic_volume FLOAT,
                    pedestrians_involved INT,
                    cyclists_involved INT,
                    population_density FLOAT,
                    id_lugar BIGINT REFERENCES dim_lugar(id_lugar),
                    id_fecha BIGINT REFERENCES dim_fecha(id_fecha),
                    id_condiciones BIGINT REFERENCES dim_condiciones(id_condiciones),
                    id_conductor BIGINT REFERENCES dim_conductor(id_conductor),
                    id_incidente BIGINT REFERENCES dim_incidente(id_incidente),
                    id_vehiculo BIGINT REFERENCES dim_vehiculo(id_vehiculo),
                    CONSTRAINT unique_hecho UNIQUE (id_lugar, id_fecha, id_condiciones, id_conductor, id_incidente, id_vehiculo)
                );
            """))

        logging.info("✅ Esquema dimensional creado con BIGINT y tablas truncadas correctamente.")
    except Exception as e:
        logging.error(f"❌ Error al crear el esquema dimensional: {e}")
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info("🔌 Conexión cerrada.")


def insert_csv_into_table(ruta_csvs: str = ruta_salida):
    """
    Carga datos dimensionales y de hechos desde CSV a PostgreSQL con:
    - UPSERT para todas las tablas
    - Carga por chunks para grandes volúmenes
    - Manejo robusto de errores
    """
    engine = None
    chunksize = 50000  # Tamaño del chunk para procesamiento por lotes
    
    try:
        engine = get_connection("dimensional")
        with engine.begin() as conn:
            # Configuración de dimensiones y sus constraints
            dimensiones = {
                'lugar': {
                    'columns': ['country', 'urban_rural', 'road_type', 'road_condition'],
                    'constraint': 'unique_lugar'
                },
                'fecha': {
                    'columns': ['year', 'month', 'day_of_week', 'time_of_day'],
                    'constraint': 'unique_fecha'
                },
                'condiciones': {
                    'columns': ['weather_conditions', 'visibility_level'],
                    'constraint': 'unique_condiciones'
                },
                'conductor': {
                    'columns': ['driver_age_group', 'driver_alcohol_level', 'driver_fatigue', 'driver_gender'],
                    'constraint': 'unique_conductor'
                },
                'incidente': {
                    'columns': ['accident_severity', 'accident_cause'],
                    'constraint': 'unique_incidente'
                },
                'vehiculo': {
                    'columns': ['vehicle_condition'],
                    'constraint': 'unique_vehiculo'
                }
            }

            # Procesar cada dimensión con UPSERT por chunks
            for dim_name, dim_config in dimensiones.items():
                csv_path = os.path.join(ruta_csvs, f"dim_{dim_name}.csv")
                if not os.path.exists(csv_path):
                    logging.warning(f"⚠️ Archivo no encontrado: dim_{dim_name}.csv")
                    continue

                total_rows = 0
                cols = dim_config['columns']
                insert_cols = [f'id_{dim_name}'] + cols
                placeholders = ', '.join([f':{col}' for col in insert_cols])
                update_set = ', '.join([f'{col} = EXCLUDED.{col}' for col in cols])
                
                query = f"""
                    INSERT INTO dim_{dim_name} ({', '.join(insert_cols)})
                    VALUES ({placeholders})
                    ON CONFLICT ON CONSTRAINT {dim_config['constraint']}
                    DO UPDATE SET {update_set}
                """

                for chunk in pd.read_csv(csv_path, chunksize=chunksize):
                    records = chunk.to_dict('records')
                    conn.execute(text(query), records)
                    total_rows += len(records)
                    logging.info(f"↳ Procesados {len(records)} registros (total: {total_rows})")

                logging.info(f"✅ Dimensión {dim_name} cargada: {total_rows} registros")

            # Cargar tabla de hechos con UPSERT por chunks
            hechos_path = os.path.join(ruta_csvs, "hechos_accidentes.csv")
            if os.path.exists(hechos_path):
                fact_columns = [ 
                    'number_of_vehicles_involved', 'speed_limit', 'number_of_injuries',
                    'number_of_fatalities', 'emergency_response_time', 'traffic_volume',
                    'pedestrians_involved', 'cyclists_involved', 'population_density',
                    'id_lugar', 'id_fecha', 'id_condiciones', 'id_conductor',
                    'id_incidente', 'id_vehiculo'
                ]
                
                # Query con UPSERT para hechos (asume constraint unique_hecho)
                fact_query = f"""
                    INSERT INTO hechos_accidentes ({', '.join(fact_columns)})
                    VALUES ({', '.join([f':{col}' for col in fact_columns])})
                    ON CONFLICT ON CONSTRAINT unique_hecho
                    DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in fact_columns])}
                """

                total_facts = 0
                for chunk in pd.read_csv(hechos_path, chunksize=chunksize):
                    records = chunk.to_dict('records')
                    conn.execute(text(fact_query), records)
                    total_facts += len(records)
                    logging.info(f"↳ Procesados {len(records)} hechos (total: {total_facts})")

                logging.info(f"✅ Hechos cargados: {total_facts} registros")
            else:
                logging.warning("⚠️ Archivo no encontrado: hechos_accidentes.csv")

        logging.info("✅ Carga de datos completada exitosamente.")
        
    except Exception as e:
        logging.error(f"❌ Error en insert_csv_into_table(): {str(e)}", exc_info=True)
        raise
    finally:
        if engine is not None:
            close_connection(engine)
            logging.info("🔌 Conexión cerrada correctamente.")