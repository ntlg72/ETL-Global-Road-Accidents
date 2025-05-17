import json
import pandas as pd
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

def send_hechos_to_kafka(df: pd.DataFrame, topic: str = "road_accidents", sleep_seconds: float = 1):
    """
    Envía cada fila del DataFrame de hechos_accidentes.csv a Kafka como un mensaje JSON.
    """
    try:
        logging.info(f"Iniciando envío de hechos a Kafka (topic: {topic})...")

        producer = kafka_producer()

        for i, (_, fila) in enumerate(df.iterrows(), start=1):
            hecho = {
                "number_of_vehicles_involved": int(fila["number_of_vehicles_involved"]),
                "speed_limit": float(fila["speed_limit"]),
                "number_of_injuries": int(fila["number_of_injuries"]),
                "number_of_fatalities": int(fila["number_of_fatalities"]),
                "emergency_response_time": float(fila["emergency_response_time"]),
                "traffic_volume": float(fila["traffic_volume"]),
                "pedestrians_involved": int(fila["pedestrians_involved"]),
                "cyclists_involved": int(fila["cyclists_involved"]),
                "population_density": float(fila["population_density"]),
                "id_lugar": int(fila["id_lugar"]),
                "id_fecha": int(fila["id_fecha"]),
                "id_condiciones": int(fila["id_condiciones"]),
                "id_conductor": int(fila["id_conductor"]),
                "id_incidente": int(fila["id_incidente"]),
                "id_vehiculo": int(fila["id_vehiculo"]),
                "event_time": int(time.time() * 1000)  # Añadido para Pinot
            }
            producer.send(topic, value=hecho)
            logging.info(f"Hecho {i} enviado a Kafka: {hecho}")
            time.sleep(sleep_seconds)

        producer.flush()
        logging.info("Todos los hechos enviados correctamente a Kafka.")

    except Exception as e:
        logging.error(f"Error durante el envío a Kafka: {e}")
        raise