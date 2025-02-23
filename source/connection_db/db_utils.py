import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    try:
        connection = psycopg2.connect(
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            password=os.getenv("PG_PASSWORD")
        )
        print("Conexión exitosa")
        return connection
    except Exception as e:
        print("Error al conectar a la DB:", e)
        return None

def close_connection(connection):
    if connection:
        connection.close()
        print("Conexión cerrada")