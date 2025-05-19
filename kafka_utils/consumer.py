import logging
import json
import time
import os
from confluent_kafka import Consumer, KafkaError, KafkaException
from fastapi import FastAPI
import uvicorn
import pandas as pd
from threading import Thread
from collections import deque

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/consumer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

messages = deque(maxlen=100)
app = FastAPI()

def kafka_consumer():
    config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'road_accidents_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(config)
    consumer.subscribe(['road_accidents'])
    return consumer

def consume_messages():
    consumer = kafka_consumer()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                message_value = json.loads(msg.value().decode('utf-8'))
                logging.info(f"📥 Recibido: {message_value}")
                messages.append(message_value)
            time.sleep(1)
    except Exception as e:
        logging.error(f"❌ Error en el consumidor: {e}")
    finally:
        consumer.close()
        logging.info("🔒 Consumidor cerrado.")

@app.get("/data")
async def get_data():
    try:
        data = list(messages)  # Todos los mensajes, sin límite
        df = pd.DataFrame(data)
        logging.info(f"📤 Enviados {len(data)} mensajes a la API")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"❌ Error en la API: {e}")
        return {"error": str(e)}

def start_consumer():
    consumer_thread = Thread(target=consume_messages)
    consumer_thread.daemon = True
    consumer_thread.start()

if __name__ == "__main__":
    start_consumer()
    uvicorn.run(app, host="0.0.0.0", port=8000)