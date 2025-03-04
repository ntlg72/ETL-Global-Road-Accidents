from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

def get_connection():
    load_dotenv()
    user = os.getenv('PG_USER')
    password = os.getenv('PG_PASSWORD')
    host = os.getenv('PG_HOST')
    port = os.getenv('PG_PORT')
    dbname = os.getenv('PG_DATABASE')

    
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        engine = create_engine(db_url)
        print("Engine creado exitosamente")
        return engine
    except Exception as e:
        print(f"Error al crear el engine: {e}")
        return None
