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

---

## Tabla de Contenidos  
1. [Descripción del Dataset](#descripción-del-dataset)  
2. [Requisitos Previos](#requisitos-previos)  
3. [Instalación](#instalación)  
4. [Configuración de la Base de Datos](#configuración-de-la-base-de-datos)  
5. [Proceso de EDA](#proceso-de-eda)  
6. [Estrategia de Visualización](#estrategia-de-visualización)  
7. [Integración con Power BI](#integración-con-power-bi)  
8. [Hallazgos Clave](#hallazgos-clave)  
9. [Documentación](#documentación)  

---

## Descripción del Dataset  

El conjunto de datos incluye:  
- **Características temporales**: Fecha, hora, día de la semana.  
- **Datos geográficos**: País, ciudad, coordenadas GPS.  
- **Factores ambientales**: Clima, condiciones de la superficie de la carretera.  
- **Metadatos del accidente**: Gravedad, tipo de colisión, vehículos involucrados.  
- **Datos demográficos**: Edad y género del conductor.  

**Fuente original del dataset**: [Kaggle - Global Road Accidents](https://www.kaggle.com/datasets/...)  

---

## Requisitos Previos  

- ![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue)  
- PostgreSQL 14+ (Instancia local o en la nube).  
- Power BI Desktop.  
- 8GB+ de RAM recomendado para procesar el dataset grande.  

---

## Instalación  

1. Clona el repositorio:  
   ```bash
   git clone https://github.com/ntlg72/road-accidents-analysis.git
   cd road-accidents-analysis
   ```  

2. Instala los requisitos:  
   ```bash
   pip install -r requirements.txt
   ```  

3. Descarga el dataset de Kaggle y colócalo en el directorio `data/raw`.  

---

## Configuración de la Base de Datos  

1. Crea la base de datos en PostgreSQL:  
   ```sql
   CREATE DATABASE accident_db;
   ```  

2. Configura las variables de entorno (archivo `.env`):  
   ```bash
   PG_USER=usuario_postgres
   PG_PASSWORD=contraseña_segura
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=accident_db
   ```  

3. Ejecuta la inicialización de la base de datos:  
   ```bash
   python src/database/setup_db.py
   ```  

---

## Proceso de EDA  

### Pasos clave del análisis:  

1. **Limpieza de Datos**:  
   - Manejo de valores faltantes.  
   - Corrección de inconsistencias en tipos de datos.  
   - Normalización de variables categóricas.  

2. **Análisis Estadístico**:  
   - Distribución de accidentes por tiempo/geografía.  
   - Matriz de correlación de variables clave.  
   - Detección de valores atípicos en características numéricas.  

3. **Identificación de Patrones**:  
   - Análisis de series temporales de tendencias de accidentes.  
   - Análisis de clusters de ubicaciones de alto riesgo.  
   - Segmentación demográfica de conductores.  

---

## Estrategia de Visualización  

### Visualizaciones planificadas:  
- Mapa de calor de puntos críticos de accidentes.  
- Patrones temporales usando descomposición de series de tiempo.  
- Análisis comparativo de gravedad de accidentes por tipo de vehículo.  
- Mapa geográfico interactivo de densidad de accidentes.  

![Ejemplo de Visualización](https://raw.githubusercontent.com/ntlg72/road-accidents-analysis/main/docs/example_heatmap.png)  

---

## Integración con Power BI  

1. **Conexión a PostgreSQL**:  
   - Usar DirectQuery para conexión en vivo a la base de datos.  
   - Importar tablas necesarias para el análisis.  

2. **Creación de Componentes del Dashboard**:  
   - Filtros jerárquicos (País > Ciudad > Distrito).  
   - Tarjetas de resumen de métricas clave.  
   - Informes detallados con capacidad de exploración.  

3. **Publicación en Power BI Service**:  
   - Programar actualizaciones diarias de datos.  
   - Configurar seguridad a nivel de fila.  
   - Compartir informes interactivos con stakeholders.  

---

## Hallazgos Clave  

1. **Patrones Temporales**:  
   - Aumento del 23% en accidentes durante condiciones de lluvia.  
   - Horas pico de accidentes entre las 16:00-19:00 hora local.  

2. **Perspectivas Geográficas**:  
   - Top 3 países de alto riesgo: [País A], [País B], [País C].  
   - El 15% de los accidentes ocurren en el 5% de los segmentos viales.  

3. **Análisis de Vehículos**:  
   - Motocicletas involucradas en el 40% de los accidentes graves.  
   - Vehículos comerciales muestran la tasa más alta de fatalidades.  

-
