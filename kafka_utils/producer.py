import os
import json
from json import dumps
import pandas as pd
import time
import logging
from kafka import KafkaProducer
from pathlib import Path
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')


def kafka_producer():
    return KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )



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
