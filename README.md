
# 🛣️ Global Road Accidents Analysis

Por **Michel Burgos Santos**, **Juan David Daza Rivera** y **Natalia López Gallego**

Este proyecto analiza el conjunto de datos **"Global Road Accidents Dataset"** de Kaggle, con más de 132,000 registros y 30 variables sobre accidentes de tráfico a nivel mundial. El objetivo principal es realizar un análisis exploratorio y dimensional automatizado para comprender patrones y apoyar la toma de decisiones basadas en datos.

----------

## 📌 Tabla de Contenidos

-   [Visión General](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#visi%C3%B3n-general)
    
-   [Prerrequisitos](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#prerrequisitos)
    
-   [Instalación](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#instalaci%C3%B3n)
    
-   [Entorno Virtual y Jupyter en WSL2](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#entorno-virtual-y-jupyter-en-wsl2)
    
-   [Instalación de PostgreSQL en WSL2](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#instalaci%C3%B3n-de-postgresql-en-wsl2)
    
-   [Configuración de PostgreSQL](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#configuraci%C3%B3n-de-postgresql)
    
-   [Ejecución del Proyecto](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#ejecuci%C3%B3n-del-proyecto)
    
-   [Integración con Power BI](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#integraci%C3%B3n-con-power-bi)
    
-   [Orquestación con Apache Airflow](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#orquestaci%C3%B3n-con-apache-airflow)
    
-   [Documentación](https://chatgpt.com/c/67f5a803-2fd0-8007-8003-291bac70c758#documentaci%C3%B3n)
    

----------

## 🎯 Visión General

Objetivos clave:

-   Identificar áreas geográficas de alto riesgo.
    
-   Analizar condiciones climáticas y gravedad de accidentes.
    
-   Investigar vehículos más involucrados.
    
-   Evaluar la infraestructura vial.
    

Tecnologías utilizadas:

Herramienta

Propósito

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

Análisis de datos

![Jupyter](https://img.shields.io/badge/Jupyter%20Notebook-F37626?style=flat-square&logo=jupyter&logoColor=white)

Análisis interactivo

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)

Almacenamiento de datos

![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

Visualización

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)

Orquestación de procesos

----------

## ✅ Prerrequisitos

-   Python 3.12.9
    
-   PostgreSQL 14+
    
-   Power BI Desktop
    
-   Visual Studio Code (con WSL2 y la extensión de Jupyter)
    
-   Apache Airflow
    
-   Acceso desde WSL2 a tu instalación de PostgreSQL
    

----------

## 🔧 Instalación

1.  Clona el repositorio:
    

```bash
git clone https://github.com/ntlg72/road-accidents-analysis.git
cd road-accidents-analysis

```

2.  Crea un entorno virtual y actívalo:
    

```bash
python -m venv venv
source venv/bin/activate

```

3.  Instala las dependencias:
    

```bash
pip install -r requirements.txt

```

----------

## 💻 Entorno Virtual y Jupyter en WSL2

1.  Instala Jupyter y registra el kernel:
    

```bash
pip install notebook jupyterlab ipykernel
python -m ipykernel install --user --name=gra_env --display-name "Python (gra_env)"

```

2.  Abre VS Code → archivo `.ipynb` → selecciona el kernel `Python (gra_env)`.
    

----------

## 🐘 Instalación de PostgreSQL en WSL2

Para instalar PostgreSQL 14 en tu entorno WSL2 (Ubuntu recomendado):

```bash
sudo apt update
sudo apt install wget ca-certificates
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

sudo apt update
sudo apt install postgresql-14 postgresql-client-14

```

Una vez instalado:

```bash
sudo service postgresql start

```

Accede al prompt de `psql`:

```bash
sudo -u postgres psql

```

----------

## 🛠️ Configuración de PostgreSQL

### 1. Permitir conexiones externas:

```bash
sudo nano /etc/postgresql/14/main/postgresql.conf

```

-   Cambia:
    

```ini
listen_addresses = '*'
ssl = off

```

### 2. Reinicia el servicio:

```bash
sudo service postgresql restart

```

### 3. Obtener la IP de tu WSL2

```bash
hostname -I

```

> Esta IP será usada como `PG_HOST` en el archivo `.env`, y también para conectar Power BI.

### 4. Crear bases de datos desde WSL2

Accede al cliente de PostgreSQL:

```bash
sudo -u postgres psql

```

Dentro de `psql`, ejecuta:

```sql
CREATE DATABASE gra;
CREATE DATABASE gra_dimensional;
\q

```

----------

## 📁 Archivo `.env`

Crea un archivo `.env` en la raíz del proyecto con esta estructura:

```env
PG_USER=postgres
PG_PASSWORD=pg
PG_HOST=<IP_OBTENIDA_CON_hostname -I>
PG_PORT=5432
PG_DATABASE=gra
PG_DATABASE_DIMENSIONAL=gra_dimensional

```

⚠️ Reemplaza `<IP_OBTENIDA_CON_hostname -I>` con la IP real mostrada en consola.

----------

## 🚀 Ejecución del Proyecto

1.  Asegúrate de tener las bases de datos creadas y el archivo `.env` configurado.
    
2.  Ejecuta los notebooks desde el directorio `/notebooks` en orden:
    

```text
001_e.ipynb       # Extracción y limpieza
001_eda.ipynb     # Análisis Exploratorio
001_tl.ipynb      # Transformación y carga al modelo dimensional

```

----------

## 📊 Integración con Power BI

1.  Abre Power BI Desktop
    
2.  Selecciona _Obtener datos → PostgreSQL_
    
3.  Ingresa:
    

Campo

Valor

Servidor

`<IP de WSL2>`

Base de datos

`gra_dimensional`

Usuario

`postgres`

Contraseña

`pg`

4.  Conecta, importa las tablas y construye dashboards:
    

-   Accidentes por país
    
-   Análisis de condiciones climáticas
    
-   Tipos de colisión más frecuentes
    

----------

## 🌀 Orquestación con Apache Airflow

1.  Crea un entorno virtual para Airflow:
    

```bash
python -m venv airflow_venv
source airflow_venv/bin/activate
pip install apache-airflow

```

2.  Inicializa Airflow en modo standalone:
    

```bash
export AIRFLOW_HOME=~/airflow
airflow standalone

```

3.  Copia los DAGs:
    

```bash
mkdir -p ~/airflow/dags
cp -r dags/* ~/airflow/dags/

```

4.  Asegúrate de que el archivo `.env` esté bien configurado.
    
5.  Corre los DAGs desde la UI de Airflow en [http://localhost:8080](http://localhost:8080/)
    

----------

## 📚 Documentación

-   [PostgreSQL Docs](https://www.postgresql.org/docs/)
    
-   [Apache Airflow Docs](https://airflow.apache.org/docs/)
    
-   [Power BI Desktop](https://powerbi.microsoft.com/desktop/)
    
-   [Jupyter en VS Code](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)
    

