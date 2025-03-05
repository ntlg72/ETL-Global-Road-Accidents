## Descripción del Proyecto  

Este proyecto analiza el conjunto de datos **"Global Road Accidents Dataset"** de Kaggle, que contiene 132,000 registros y 30 variables relacionadas con accidentes de tráfico a nivel mundial. El objetivo principal es realizar un Análisis Exploratorio de Datos (EDA) exhaustivo para comprender los patrones, las relaciones y los factores que contribuyen a los accidentes de tráfico, y luego crear visualizaciones claras y efectivas para comunicar estos hallazgos.  

### Objetivos clave:  
- Identificar áreas geográficas de alto riesgo y patrones temporales.  
- Analizar la relación entre las condiciones climáticas y la gravedad de los accidentes.  
- Investigar los tipos de vehículos más involucrados en accidentes graves.  
- Evaluar el impacto de la infraestructura vial en las tasas de accidentes.  

### Tecnologías utilizadas:  
- ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54): Para procesamiento y análisis de datos.  
- ![Pandas](https://img.shields.io/badge/Pandas-2C2D72?logo=pandas&logoColor=white): Para manipulación de datos.  
- ![Jupyter](https://img.shields.io/badge/Jupyter-F37626?logo=jupyter&logoColor=white): Para análisis interactivo.  
- ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white): Para almacenamiento de datos.  
- ![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi&logoColor=black): Para visualizaciones en dashboards.  



#


# Global Road Accidents a Dataset
Por **Natalia López Gallego**

## Visión General

Este proyecto analiza el conjunto de datos **"Global Road Accidents Dataset"** de Kaggle, que contiene 132,000 registros y 30 variables relacionadas con accidentes de tráfico a nivel mundial. El objetivo principal es realizar un Análisis Exploratorio de Datos (EDA) exhaustivo para comprender los patrones, las relaciones y los factores que contribuyen a los accidentes de tráfico, y luego crear visualizaciones claras y efectivas para comunicar estos hallazgos.  

### Objetivos clave:  
- Identificar áreas geográficas de alto riesgo y patrones temporales.  
- Analizar la relación entre las condiciones climáticas y la gravedad de los accidentes.  
- Investigar los tipos de vehículos más involucrados en accidentes graves.  
- Evaluar el impacto de la infraestructura vial en las tasas de accidentes.  

Las tecnologías utilizadas en este proyecto incluyen:

- ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54): Para el manejo y análisis de datos.
- ![Jupyter Notebook](https://img.shields.io/badge/Jupyter%20Notebook-F37626?style=flat-square&logo=jupyter&logoColor=white): Para el análisis interactivo de datos.
- ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white): Para la gestión de bases de datos.
- ![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black): Para la visualización de datos.

## Tabla de Contenidos

- [Prerrequisitos](#prerrequisitos)
- [Instalación](#instalación)
- [Entorno Virtual de Python y Dependencias](#entorno-virtual-de-python--dependencias)
- [Configuración de PostgreSQL](#configuración-de-postgresql)
- [Uso](#uso)
- [Integración con Power BI](#integración-con-power-bi)
- [Documentación](#documentación)

## Prerrequisitos

Antes de comenzar, asegúrate de cumplir con los siguientes requisitos:

- ![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff): 3.12.9
- [PostgreSQL](https://www.postgresql.org/download/): 14+ (instancia local o remota)
- [Power BI Desktop](https://powerbi.microsoft.com/desktop/) para visualización
- [Visual Studio Code](https://code.visualstudio.com/) o tu IDE de Python preferido.

## Instalación

1. Clona el repositorio:

    ```bash
    git clone [https://github.com/ntlg72/etl-ws-1.git](https://github.com/ntlg72/etl-ws-1.git)
    ```

2. Navega al directorio del proyecto:

    ```bash
    cd etl-ws-1
    ```

3. Instala las dependencias:

    ```bash
    pip install -r requirements.txt
    ```

## Entorno Virtual de Python y Dependencias

### Implementación

1. Crea un entorno virtual:

    ```bash
    python -m venv venv
    ```

2. Activa el entorno:

    - **Windows (CMD):**

        ```bash
        venv\Scripts\activate.bat
        ```

    - **Windows (PowerShell):**

        ```bash
        venv\Scripts\Activate.ps1
        ```

3. Instala las dependencias:

    ```bash
    pip install -r requirements.txt
    ```

## Configuración de PostgreSQL

1. Crea una base de datos PostgreSQL (por ejemplo, `ws_001`).
2. Configura las variables de entorno en un archivo `.env`:

    ```bash
    PG_USER=tu_usuario_postgres
    PG_PASSWORD=tu_contraseña_postgres
    PG_HOST=localhost_o_host_remoto
    PG_PORT=5432
    PG_DATABASE=ws_001
    ```

3. Asegúrate de que el esquema de la base de datos coincida con la estructura de datos en `candidates.csv`.

## Uso

Ejecuta el script ETL para cargar datos en PostgreSQL:

```bash
python main.py
```

# Integración con Power BI

**Conéctate a PostgreSQL:**

-   Abre Power BI Desktop
-   Selecciona _Obtener datos > Base de datos de PostgreSQL_
-   Ingresa los detalles de conexión:
    -   Servidor: `tu_host_postgres`
    -   Base de datos: `ws_001`
    -   Autenticación: `Nombre de usuario/Contraseña`
    -   Usuario: `PG_USER` desde `.env`
    -   Contraseña: `PG_PASSWORD` desde `.env`

**Crea Visualizaciones:**

-   Importa las tablas de la base de datos PostgreSQL
-   Usa las herramientas de visualización de Power BI para construir dashboards
-   Ejemplos de gráficos:
    -   Distribución de candidatos por estado
    -   Análisis de la duración del proceso de selección
    -   Distribución geográfica de los candidatos
    
> Written with [StackEdit](https://stackedit.io/).
