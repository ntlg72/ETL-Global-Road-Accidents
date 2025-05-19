import os
import json
import pandas as pd
import time
import logging
from kafka import KafkaProducer
from pathlib import Path
import tempfile

# Configuración de logs y directorio temporal
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')


def kafka_producer(bootstrap_servers='localhost:9092'):
    """
    Create a Kafka producer instance with JSON serialization.
    """
    return KafkaProducer(
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        bootstrap_servers=[bootstrap_servers],
    )


def send_df_to_kafka(df: pd.DataFrame, topic: str, producer: KafkaProducer, sleep_time: float = 0.001):
    """
    Send each row of a DataFrame to a Kafka topic as a JSON message,
    adding 'event_timestamp' as current epoch milliseconds.

    Parameters:
    - df: pandas DataFrame with the facts to send.
    - topic: str, Kafka topic name.
    - producer: KafkaProducer instance.
    - sleep_time: float, seconds to wait between each message (default 0.001).
    """
    logging.info(f"Sending {len(df)} rows to Kafka topic '{topic}'...")

    for index, row in df.iterrows():
        message = row.to_dict()
        # Añadir event_timestamp como epoch ms actual
        message['event_timestamp'] = int(time.time() * 1000)
        try:
            producer.send(topic, value=message)
            logging.info(f"Fila {index + 1}/{len(df)} enviada: {message}")
        except Exception as e:
            logging.error(f"Error enviando la fila {index + 1}: {e}")
        time.sleep(sleep_time)

    producer.flush()
    logging.info("All messages sent and flushed.")