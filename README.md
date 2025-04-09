
# ETL - Global Road Accidents

Por Michel Burgos Santos, Juan David Daza Rivera y Natalia López Gallego.

Este proyecto analiza el conjunto de datos **Global Road Accidents Dataset** de Kaggle. Utiliza Apache Airflow para orquestar flujos ETL, PostgreSQL como base de datos y Jupyter/Power BI para el análisis y visualización de datos.

---

## 🚀 Pasos de instalación (WSL2)

### 1. Instalar PostgreSQL (última versión)

```bash
sudo apt update
sudo apt install wget ca-certificates -y
wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install postgresql -y
```

Inicia el servicio:

```bash
sudo service postgresql start
```

Crea el usuario y las bases de datos:

```bash
sudo -u postgres psql
```

Dentro de `psql`:

```sql
CREATE USER postgres WITH PASSWORD 'pg';
ALTER USER postgres CREATEDB;
\q
```

Luego crea las bases de datos necesarias:

```bash
createdb -U postgres -h localhost gra
createdb -U postgres -h localhost gra_dimensional
```

---

### 2. Crear entorno virtual (fuera de la carpeta del repositorio)

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Instalar Apache Airflow

```bash
pip install apache-airflow
```

---

### 4. Clonar el repositorio dentro de `~/airflow/dags`

```bash
mkdir -p ~/airflow/dags
cd ~/airflow/dags
git clone https://github.com/ntlg72/ETL-Global-Road-Accidents.git
```

---

### 5. Crear archivo `.env` en el directorio clonado

Dentro de `~/airflow/dags/ETL-Global-Road-Accidents/`, crea el archivo `.env`:

```env
PG_USER=postgres
PG_PASSWORD=pg
PG_HOST=<resultado de hostname -I>
PG_PORT=5432
PG_DATABASE=gra
PG_DATABASE_DIMENSIONAL=gra_dimensional
```

Para obtener la IP de tu máquina WSL2:

```bash
hostname -I
```

---

### 6. Instalar requerimientos del proyecto

Desde el entorno virtual activo:

```bash
pip install -r ~/airflow/dags/ETL-Global-Road-Accidents/requirements.txt
```

---

### 7. Ejecutar los notebooks (antes de iniciar Airflow)

Los notebooks cargan y transforman los datos necesarios en las bases de datos. Es **obligatorio ejecutarlos antes de iniciar Airflow**.

```bash
jupyter lab
```

Corre los siguientes notebooks, en orden:

1. `001_e.ipynb` – Exploración inicial  
2. `001_eda.ipynb` – Análisis exploratorio  
3. `001_tl.ipynb` – Transformación y carga a PostgreSQL

---

### 8. Inicializar Airflow

```bash
airflow standalone
```

Esto creará la carpeta `~/airflow` y levantará el servidor web.

Abre [http://localhost:8080](http://localhost:8080) y usa las credenciales generadas en la terminal para acceder.

---

## ✅ Ejecutar los DAGs

Desde el panel web de Airflow:

- Habilita y ejecuta los DAGs disponibles.
- Asegúrate de que el entorno virtual esté activo si haces pruebas en consola.

---

## 📊 Integración con Power BI

1. Abre Power BI Desktop.
2. Selecciona _Obtener datos → PostgreSQL_.
3. Conecta usando:
   - **Servidor**: IP obtenida con `hostname -I`
   - **Base de datos**: `gra` o `gra_dimensional`
   - **Autenticación**: `postgres / pg`

---

## 📁 Notebooks

Puedes encontrar los notebooks en la carpeta `notebooks/` del repositorio clonado. Asegúrate de ejecutarlos en el orden correcto.

---

## 📌 Requisitos

- Python 3.12+
- PostgreSQL (última versión disponible en WSL2)
- Apache Airflow
- Power BI Desktop
- Visual Studio Code o editor de tu elección

---

## 🧠 Créditos

Proyecto desarrollado por:

- Michel Burgos Santos  
- Juan David Daza Rivera  
- Natalia López Gallego
