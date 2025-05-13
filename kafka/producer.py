from kafka import KafkaProducer
from json import dumps
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def kafka_producer():
    return KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

def send_dataframe_to_kafka(df: pd.DataFrame, topic: str = "road_accidents", sleep_seconds: float = 3):
    """
    Sends each row of a DataFrame to a Kafka topic with a delay between messages.

    Args:
        df (pd.DataFrame): DataFrame to send.
        topic (str): Kafka topic.
        sleep_seconds (float): Delay between messages in seconds.
    """
    producer = kafka_producer()

    for i, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic, value=message)
        logging.info(f"✅ Mensaje {i+1}/{len(df)} enviado: {message}")
        time.sleep(sleep_seconds)

    producer.flush()
    logging.info("✅ Todos los mensajes fueron enviados con retardo.")
