# Pipeline ETL para Análisis de Accidentes Viales Globales

Este proyecto proporciona un pipeline ETL completo para procesar y analizar datos de accidentes viales globales (procedentes del dataset de Kaggle "Global Road Accidents Data" y de "Fatality Analysis Reporting System (FARS) API") etutilizando Kafka para streaming de datos, Airflow para orquestación y PostgreSQL para almacenamiento.

## Requisitos Previos

- Docker (versión 20.10.0 o superior)
- Docker Compose (versión 1.29.0 o superior)
- Git
- Python 3.9+ (para desarrollo local)
- Jupyter Notebook (para ejecutar los cuadernos de análisis)

## Configuración Inicial

### 1. Clonar el Repositorios

Primero, clona el repositorio:

```bash

git clone https://github.com/ntlg72/ETL-Global-Road-Accidents.git
```

### 2. Configurar Variables de Entorno

Crea un archivo `.env` en el directorio raíz con las siguientes variables:

```ini
# Configuración de Postgres
PG_PASSWORD=tu_contraseña_segura
PG_DATABASE=road_accidents
PG_DATABASE_DIMENSIONAL=road_accidents_dimensional
PG_DATABASE_KAFKA=road_accidents_kafka
PG_PORT=5433

# Configuración de Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
```

### 3. Estructura del Proyecto

Después de clonar, la estructura debería verse así:

```
docker-etl-pipeline/
├── ETL-Global-Road-Accidents/
│   ├── notebooks/
│   │   └── 001_extraction.ipynb
├── consumer.py
├── docker-compose.yml
├── .env
└── logs/
```
Nótese que esto es una estructura reducida del directorio del proyecto.

### 4. Iniciar los Servicios con Docker Compose

Ejecuta el siguiente comando para iniciar todos los servicios:

```bash
docker-compose up -d --build
```

Esto iniciará:
- Zookeeper y Kafka para streaming de eventos
- Bases de datos PostgreSQL
- Redis como broker de mensajes para Airflow
- Servicios de Airflow
- El servicio consumer de Python

### 5. Ejecutar el Notebook de Extracción

Después de que todos los servicios estén en funcionamiento:

1. Navega al directorio de notebooks:

```bash
cd ETL-Global-Road-Accidents/notebooks
```

2. Instala Jupyter si no lo tienes:

```bash
pip install jupyter
```

3. Inicia Jupyter Notebook:

```bash
jupyter notebook
```

4. Abre y ejecuta el notebook `001_extraction.ipynb`:
   - Sigue las instrucciones dentro del notebook
   - Asegúrate de que los servicios de Kafka y PostgreSQL estén corriendo
   - Verifica las conexiones a las bases de datos

### 6. Acceder a los Servicios

| Servicio         | URL/Puerto          | Credenciales            |
|------------------|---------------------|-------------------------|
| Airflow Web UI   | http://localhost:8080 | admin/admin           |
| Flower (Celery)  | http://localhost:5555 | -                     |
| Kafka            | localhost:9092      | -                     |
| PostgreSQL (ETL) | localhost:5433      | Variables del .env    |
| Consumer API     | http://localhost:8000 | -                     |

### 7. Iniciar el Proceso ETL

1. **Accede a Airflow**: http://localhost:8080
2. **Ejecuta el DAG**: Busca y activa el DAG `etl_accidents_pipeline`
3. **Monitorea el progreso**: Puedes ver los logs en Airflow o con:

```bash
docker logs consumer -f
```

---

## 🧠 Créditos

Proyecto desarrollado por:

- Michel Burgos Santos  
- Juan David Daza Rivera  
- Natalia López Gallego
