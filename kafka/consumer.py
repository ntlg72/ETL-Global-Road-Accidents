from confluent_kafka import Consumer, KafkaException, KafkaError
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
import json
import logging
import time
from source.connection_db import get_connection  # Importar la función de conexión de tu módulo

# Función para consumir mensajes de Kafka y almacenarlos en PostgreSQL
def kafka_consumer():
    """Configura el consumidor de Kafka para leer mensajes del topic road_accidents."""
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Ajusta a tu broker Kafka
        'group.id': 'road_accidents_group',     # Grupo de consumidores
        'auto.offset.reset': 'earliest'         # Comienza desde el primer mensaje
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['road_accidents'])  # Se suscribe al topic 'road_accidents'
    
    return consumer

def save_to_postgresql(message_value):
    """Guarda los mensajes consumidos en PostgreSQL."""
    try:
        # Crear conexión a la base de datos 'kafka' usando la función get_connection
        engine = get_connection("kafka")
        metadata = MetaData(bind=engine)
        session = sessionmaker(bind=engine)()

        # Suponiendo que la tabla ya está creada en PostgreSQL
        # Especifica el nombre de tu tabla y las columnas
        table = Table('road_accidents', metadata, autoload_with=engine)

        # Convertir el mensaje en formato adecuado para la inserción en la tabla
        session.execute(table.insert(), [message_value])
        session.commit()
        logging.info(f"✅ Mensaje guardado en PostgreSQL.")
    except Exception as e:
        logging.error(f"Error al guardar mensaje en PostgreSQL: {e}")

def process_message(message):
    """Procesa el mensaje recibido de Kafka."""
    try:
        # Decodificar el mensaje JSON
        message_value = json.loads(message.value().decode('utf-8'))
        logging.info(f"Procesando mensaje: {message_value}")
        
        # Guardar en PostgreSQL
        save_to_postgresql(message_value)

    except Exception as e:
        logging.error(f"Error al procesar mensaje: {e}")

def consume_kafka_messages():
    """Consume los mensajes de Kafka y los guarda en PostgreSQL."""
    consumer = kafka_consumer()

    try:
        while True:
            # Obtener el mensaje desde Kafka
            msg = consumer.poll(timeout=1.0)  # Timeout de 1 segundo

            if msg is None:
                # No hay nuevos mensajes
                continue
            elif msg.error():
                # Manejar errores del consumidor
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partición, continuar esperando
                    continue
                else:
                    # Otro tipo de error
                    raise KafkaException(msg.error())
            else:
                # Procesar el mensaje
                process_message(msg)
                
            time.sleep(3)  # Añadir un retardo entre mensajes (ajustable)

    except KeyboardInterrupt:
        logging.info("Consumiendo mensajes interrumpido por el usuario.")
    finally:
        # Cerrar el consumidor cuando se termine
        consumer.close()
        logging.info("Consumidor de Kafka cerrado.")
