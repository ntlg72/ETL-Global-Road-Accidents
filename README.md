# 🚦 **Pipeline ETL para Análisis de Accidentes Viales Globales**

Este proyecto proporciona un pipeline **ETL completo** para procesar y analizar datos de accidentes viales globales, utilizando datos del dataset de Kaggle ["Global Road Accidents Data"](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) y de la **API FARS (Fatality Analysis Reporting System)**. Se emplean herramientas modernas como:

- 🛰️ **Kafka** para streaming de datos  
- 🛠️ **Apache Airflow** para orquestación de flujos  
- 🐘 **PostgreSQL** para almacenamiento  
- 🐳 **Docker** para contenedores  

---

## 📋 Requisitos Previos

- 🐳 Docker `>= 20.10.0`  
- 📦 Docker Compose `>= 1.29.0`  
- 🧬 Git  
- 🐍 Python `>= 3.9`  
- 📓 Jupyter Notebook (para los cuadernos de análisis)

---

## 🚀 Configuración Inicial

### 1. Clonar el Repositorio

```bash
git clone https://github.com/ntlg72/ETL-Global-Road-Accidents.git
cd ETL-Global-Road-Accidents
```

### 2. Configurar Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```env
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

```
ETL-Global-Road-Accidents/
├── dags/                  # DAGs de Airflow
├── data/                  # Datos sin procesar y procesados
├── kafka_utils/           # Utilidades para Kafka
├── notebooks/             # Cuadernos de Jupyter para análisis
├── postgres/              # Scripts de inicialización de PostgreSQL
├── source/                # Código fuente del proyecto
├── unit_tests/            # Pruebas unitarias
├── .gitattributes
├── .gitignore
├── Dashboard.pdf          # Dashboard generado
├── Dockerfile
├── README.md
├── consumer.py            # Servicio consumidor de Kafka
├── docker-compose.yaml
├── requirements.txt
└── requirements_docker.txt

```

---

## 🧱 Construcción y Ejecución de Servicios

### 4. Levantar los Servicios

```bash
docker-compose up -d --build
```

Esto iniciará:

- 🐘 PostgreSQL  
- 📡 Zookeeper + Kafka  
- 🔄 Redis (broker para Airflow)  
- 🌬️ Apache Airflow (scheduler, webserver, worker)  
- 🐍 Servicio `consumer.py`

---

## 📊 Análisis y Extracción de Datos

### 5. Ejecutar el Notebook de Extracción

```bash
cd ETL-Global-Road-Accidents/notebooks
pip install jupyter
jupyter notebook
```
🔎 **Verifica:**
- Que Kafka y PostgreSQL estén funcionando  
- Que las conexiones a las bases de datos estén activas

---

## 🌐 Acceso a los Servicios

| Servicio             | URL/Puerto              | Credenciales     |
|----------------------|--------------------------|------------------|
| 🌬️ Airflow Web UI    | http://localhost:8080     | admin / admin    |
| 🌼 Flower (Celery)   | http://localhost:5555     | -                |
| 📡 Kafka             | localhost:9092           | -                |
| 🐘 PostgreSQL        | localhost:5433           | `.env` variables |
| 🐍 Consumer API      | http://localhost:8000     | -                |

---

## 🔁 Iniciar Proceso ETL

1. Accede a Airflow en: [http://localhost:8080](http://localhost:8080)  
2. Busca y activa el DAG `etl_accidents_pipeline`  
3. Monitorea el progreso en la interfaz de Airflow o usando:

```bash
docker logs consumer -f
```

---

## 🚦 Dashboard en Tiempo Real

Este proyecto incluye un **dashboard interactivo en tiempo real** construido con **Dash** para visualizar dinámicamente los datos de accidentes viales conforme se actualizan.

### ▶️ Cómo ejecutar el dashboard

Ejecuta el siguiente comando desde la raíz del proyecto:

bash

CopiarEditar

`python source/real_time_dash.py` 

### 🌐 Acceso al dashboard

Abre tu navegador y visita la siguiente URL para ver el dashboard en acción:

[http://localhost:8050/](http://localhost:8050/)

Aquí encontrarás:

-   KPIs clave actualizándose cada 2 segundos
    
-   Gráfica interactiva que muestra la evolución del número de vehículos involucrados en accidentes
    

### ⚠️ Consideraciones importantes

-   Asegúrate de que el servicio **consumer** y la API estén activos en `http://localhost:8000`, ya que el dashboard depende de esta fuente para obtener los datos en tiempo real.
    
-   El dashboard se refresca automáticamente cada 2 segundos para mostrar la información más reciente.

## 🧠 Créditos

Proyecto desarrollado por:

- 👨‍💻 Michel Burgos Santos  
- 👨‍💻 Juan David Daza Rivera  
- 👩‍💻 Natalia López Gallego

---
