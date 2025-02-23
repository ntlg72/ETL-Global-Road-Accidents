import psycopg2 
import os
from dotenv import load_dotenv

load_dotenv(encoding="utf-8") 

name_db=os.getenv("PG_DATABASE")
user_db=os.getenv("PG_USER")
host_db=os.getenv("PG_HOST")
port_db=os.getenv("PG_PORT")
password_db=os.getenv("PG_PASSWORD")

try:
    connection = psycopg2.connect(
        dbname = name_db,
        user=user_db,
        host=host_db,
        port=port_db,
        password=password_db
    )
    print("Conexión exitosa")
except Exception as e:
    print("Error al conectar a la DB:", e)



