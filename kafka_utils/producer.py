import json
from json import dumps
import pandas as pd
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def kafka_producer():
    return KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )


import pandas as pd
import logging
from pathlib import Path

TRANSFORMED_DIR = Path("/ruta/a/tu/TRANSFORMED_DIR")  # Ajusta esta ruta

def generar_tablas_dimensiones() -> dict:
    """
    Lee el archivo transformado y genera las tablas dimensionales como DataFrames.
    Agrega columnas de ID. Retorna un dict con cada tabla dimensional.
    """
    try:
        csv_path = TRANSFORMED_DIR / "merge_accidents_data.csv"
        logging.info(f"📂 Leyendo archivo: {csv_path}")
        df = pd.read_csv(csv_path)

        logging.info("📌 Generando tablas de dimensiones...")

        dims = {}

        dims["dim_lugar"] = df[["country", "urban_rural", "road_type", "road_condition"]].drop_duplicates().reset_index(drop=True)
        dims["dim_lugar"]["id_lugar"] = dims["dim_lugar"].index + 1

        dims["dim_fecha"] = df[["year", "month", "day_of_week", "time_of_day"]].drop_duplicates().reset_index(drop=True)
        dims["dim_fecha"]["id_fecha"] = dims["dim_fecha"].index + 1

        dims["dim_condiciones"] = df[["weather_conditions", "visibility_level"]].drop_duplicates().reset_index(drop=True)
        dims["dim_condiciones"]["id_condiciones"] = dims["dim_condiciones"].index + 1

        dims["dim_conductor"] = df[["driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender"]].drop_duplicates().reset_index(drop=True)
        dims["dim_conductor"]["id_conductor"] = dims["dim_conductor"].index + 1

        dims["dim_incidente"] = df[["accident_severity", "accident_cause"]].drop_duplicates().reset_index(drop=True)
        dims["dim_incidente"]["id_incidente"] = dims["dim_incidente"].index + 1

        dims["dim_vehiculo"] = df[["vehicle_condition"]].drop_duplicates().reset_index(drop=True)
        dims["dim_vehiculo"]["id_vehiculo"] = dims["dim_vehiculo"].index + 1

        logging.info("✅ Tablas de dimensiones generadas correctamente.")
        return dims

    except Exception as e:
        logging.error(f"❌ Error al generar tablas de dimensiones: {e}")
        raise


def transformar_fila_dimensional(fila: pd.Series, dimensiones: dict) -> dict:
    """
    Dada una fila del dataframe original, devuelve el hecho dimensional con IDs,
    buscando los IDs en las tablas dimensionales generadas.
    """
    def buscar_id(df_dim, condiciones, id_col):
        match = df_dim
        for col, val in condiciones.items():
            match = match[match[col] == val]
        if not match.empty:
            return match[id_col].values[0]
        return None

    hecho = {
        "number_of_vehicles_involved": fila["number_of_vehicles_involved"],
        "speed_limit": fila["speed_limit"],
        "number_of_injuries": fila["number_of_injuries"],
        "number_of_fatalities": fila["number_of_fatalities"],
        "emergency_response_time": fila["emergency_response_time"],
        "traffic_volume": fila["traffic_volume"],
        "pedestrians_involved": fila["pedestrians_involved"],
        "cyclists_involved": fila["cyclists_involved"],
        "population_density": fila["population_density"],
        "id_lugar": buscar_id(dimensiones["dim_lugar"], {
            "country": fila["country"], "urban_rural": fila["urban_rural"],
            "road_type": fila["road_type"], "road_condition": fila["road_condition"]
        }, "id_lugar"),
        "id_fecha": buscar_id(dimensiones["dim_fecha"], {
            "year": fila["year"], "month": fila["month"],
            "day_of_week": fila["day_of_week"], "time_of_day": fila["time_of_day"]
        }, "id_fecha"),
        "id_condiciones": buscar_id(dimensiones["dim_condiciones"], {
            "weather_conditions": fila["weather_conditions"], "visibility_level": fila["visibility_level"]
        }, "id_condiciones"),
        "id_conductor": buscar_id(dimensiones["dim_conductor"], {
            "driver_age_group": fila["driver_age_group"], "driver_alcohol_level": fila["driver_alcohol_level"],
            "driver_fatigue": fila["driver_fatigue"], "driver_gender": fila["driver_gender"]
        }, "id_conductor"),
        "id_incidente": buscar_id(dimensiones["dim_incidente"], {
            "accident_severity": fila["accident_severity"], "accident_cause": fila["accident_cause"]
        }, "id_incidente"),
        "id_vehiculo": buscar_id(dimensiones["dim_vehiculo"], {
            "vehicle_condition": fila["vehicle_condition"]
        }, "id_vehiculo")
    }
    return hecho

def send_dimensional_stream_to_kafka(df: pd.DataFrame, dimensiones: dict, topic: str = "road_accidents", sleep_seconds: float = 0.5):
    """
    Transforma cada fila del dataframe a un hecho dimensional y lo envía a Kafka en JSON.
    """
    try:
        logging.info(f"🚀 Iniciando envío de hechos dimensionales a Kafka (topic: {topic})...")

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        for i, (_, fila) in enumerate(df.iterrows(), start=1):
            hecho = transformar_fila_dimensional(fila, dimensiones)
            producer.send(topic, value=hecho)
            logging.info(f"📤 Hecho {i} enviado a Kafka: {hecho}")
            time.sleep(sleep_seconds)

        producer.flush()
        logging.info("✅ Todos los hechos enviados correctamente a Kafka.")

    except Exception as e:
        logging.error(f"❌ Error durante el envío a Kafka: {e}")
        raise
