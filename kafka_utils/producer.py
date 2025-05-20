import json
import pandas as pd
import time
import logging
from kafka import KafkaProducer
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/producer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        return producer
    except Exception as e:
        logging.error(f"No se pudo conectar a Kafka: {e}")
        raise

def send_hechos_to_kafka(df: pd.DataFrame, topic: str = "road_accidents", sleep_seconds: float = 1):
    try:
        logging.info("🚀 Enviando hechos a Kafka...")
        producer = kafka_producer()

        for i, (_, fila) in enumerate(df.iterrows(), start=1):
            hecho = fila.to_dict()
            hecho["event_time"] = int(time.time() * 1000)
            producer.send(topic, value=hecho)
            logging.info(f"📤 Enviado hecho {i}: {hecho}")
            time.sleep(sleep_seconds)

        producer.flush()
        logging.info("✅ Todos los hechos enviados.")
    except Exception as e:
        logging.error(f"❌ Error en el envío: {e}")
        raise