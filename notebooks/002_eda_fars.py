#!/usr/bin/env python
# coding: utf-8

# # EDA API

# ### Importación de librerías

# In[85]:


import sys
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, time
import seaborn as sns
import matplotlib.pyplot as plt
import math
sys.path.append(os.path.abspath('../source'))


# ### Descarga de Datos de Accidentes FARS desde la API de NHTSA

# In[4]:


'''
base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"

# Encabezados
headers = {
    "Accept": "text/csv", 
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)


for year in range(2017, 2023):  

    url = f"{base_url}?dataset=Accident&FromYear={year}&ToYear={year}&State=*&format=csv"
    print(f"Descargando datos para el año {year}...")

    try:
        # Realizar la solicitud al API
        response = requests.get(url, headers=headers, timeout=600, stream=True)  # Timeout de 10 minutos

        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Guardar los datos en el archivo CSV
            output_file = os.path.join(output_dir, f"FARS_data_{year}.csv")
            with open(output_file, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            print(f"Datos del año {year} guardados exitosamente en {output_file}")
        else:
            print(f"Error al obtener los datos para el año {year}: Código HTTP {response.status_code}")
            print(response.text)

    except requests.exceptions.Timeout:
        print(f"La solicitud para el año {year} excedió el tiempo límite.")
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud para el año {year}: {e}")
'''


# In[5]:


'''
base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"

# Encabezados
headers = {
    "Accept": "text/csv", 
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)

for year in range(2017, 2023):  

    url = f"{base_url}?dataset=Person&FromYear={year}&ToYear={year}&State=*&format=csv"
    print(f"Descargando datos para el año {year}...")

    try:
        # Realizar la solicitud al API
        response = requests.get(url, headers=headers, timeout=600, stream=True)  # Timeout de 10 minutos

        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Guardar los datos en el archivo CSV
            output_file = os.path.join(output_dir, f"FARS_person_{year}.csv")
            with open(output_file, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            print(f"Datos del año {year} guardados exitosamente en {output_file}")
        else:
            print(f"Error al obtener los datos para el año {year}: Código HTTP {response.status_code}")
            print(response.text)

    except requests.exceptions.Timeout:
        print(f"La solicitud para el año {year} excedió el tiempo límite.")
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud para el año {year}: {e}")
'''


# #### Carga de Datos de Accidentes FARS desde Archivos CSV

# In[6]:


# Definir los años a procesar
years = range(2017, 2023)  # Desde 2017 hasta 2022

# Lista para almacenar DataFrames
dataframes = []

# Cargar archivos en un ciclo
for year in years:
    file_path = f'../data/FARS_data_{year}.csv'  # Ajusta el path si es necesario
    df = pd.read_csv(file_path)
    df["Year"] = year  # Agregar la columna de año para referencia
    dataframes.append(df)

# Concatenar todos los DataFrames en uno solo
accidents = pd.concat(dataframes, ignore_index=True)

# Ver los primeros registros
accidents.head()


# In[7]:


# Años a procesar
years = range(2017, 2023)  # Hasta 2022 

# Lista para almacenar DataFrames
dataframes = []

# Cargar archivos en un ciclo
for year in years:
    file_path = f'../data/FARS_person_{year}.csv'  # Ajusta el path si es necesario
    df = pd.read_csv(file_path)
    df["Year"] = year  # Agregar la columna de año para referencia
    dataframes.append(df)

# Concatenar todos los DataFrames en uno solo
persons = pd.concat(dataframes, ignore_index=True)


# #### Unión de Datos de Accidentes y Persons 

# In[8]:


print(accidents.columns)  # Muestra todas las columnas disponibles


# In[9]:


print(persons.columns)  


# In[10]:


df = accidents.merge(persons[['st_case', 'age', 'sex', 'alc_res']], on='st_case', how='left')


# In[11]:


df_copy = df.copy()


# In[12]:


df.head()


# ### Generalidades

# In[13]:


df.shape


# #### Tipos de Datos

# In[14]:


df.info()


# In[15]:


df.columns


# ### Limpieza

# In[16]:


columns = [
    've_total', 'fatals', 'peds','arr_hour', 'arr_min',
    'year', 'monthname', 'day_weekname','hour','minute',
    'state', 'statename', 'rur_urbname', 'func_sysname', 
    'weathername', 'lgt_condname','harm_evname',
    'age', 'sex', 'alc_res'
]

df = df[columns]


# In[17]:


df.head()


# In[18]:


df.dtypes


# #### Manejo de datos de faltantes

# In[19]:


df.isnull().sum()


# #### Manejo de datos duplicados

# In[20]:


df.duplicated().sum()


# In[21]:


df[df.duplicated()]


# In[22]:


df = df.drop_duplicates()


# In[23]:


df.shape


# In[24]:


df['rur_urbname'].value_counts()    


# In[25]:


df = df.loc[~df["rur_urbname"].isin(["Not Reported", "Trafficway Not in State Inventory", "Unknown"])].copy()


# In[26]:


df['weathername'].value_counts()  


# In[27]:


df = df.loc[~df["weathername"].isin([
    "Not Reported", "Other", "Unknown", "Reported as Unknown"
])].copy()


# In[28]:


df.shape


# In[ ]:


df['lgt_condname'].value_counts()  


# In[ ]:


df = df.loc[~df['lgt_condname'].isin([
    "Not Reported", "Other", "Unknown", "Reported as Unknown"
])].copy()


# In[29]:


# Diccionario con códigos especiales por columna
special_codes = {
    'arr_hour': [88, 99],
    'arr_min': [88, 97, 98, 99],
}

for col, codes in special_codes.items():
    df[col] = df[col].replace({code: -1 for code in codes})  # -1 como marcador en lugar de NaN


# In[30]:


df = df.loc[~df[['arr_hour', 'arr_min']].isin([-1]).any(axis=1)].copy()


# 
# 
# Las siguientes variables de tiempo del dataset FARS fueron procesadas para eliminar códigos especiales o valores fuera del rango válido:
# 
# - `arr_hour`: hora de llegada
# - `arr_min`: minuto de llegada
# 
# 
# Estas variables contenían códigos utilizados por el sistema FARS para representar valores especiales:
# 
# | Código | Significado común en FARS |
# |--------|----------------------------|
# | `88`   | Not Applicable             |
# | `96`   | Anómalo (fuera de rango)   |
# | `97`   | Imputed                    |
# | `98`   | Estimated                  |
# | `99`   | Unknown / Missing          |
# 
# Estos códigos fueron reemplazados por `-1` y eliminados dado que no eran muchos.
# 
# Por ejemplo:
# - `arr_min` contenía valores como `97`, `98`, `99`, que no representan minutos válidos.

# ### Manejo de la variable `sex`

# In[31]:


df["sex"].value_counts()[[8, 9]]


# In[32]:


df = df.loc[~df["sex"].isin(["Not Reported", "Unknown"])].copy()


# In[33]:


sex_mapping = {
    1: "Male",
    2: "Female",
    3: "Other",
}

df["sex"] = df["sex"].map(sex_mapping)


# ### Manejo de la variable `func_sysname`

# In[62]:


df['func_sysname'].unique()


# In[64]:


# Diccionario con códigos especiales para la columna 'func_sysname'
special_codes = {
    'func_sysname': ['Not Reported', 'Unknown']  
}

# Reemplazar valores especiales por -1 como marcador en lugar de. NaN
for col, codes in special_codes.items():
    df[col] = df[col].replace({code: -1 for code in codes})


# In[65]:


# Filtrar filas donde 'func_sysname' no contenga valores especiales (-1)
df = df.loc[~df['func_sysname'].isin([-1])].copy()


# In[66]:


df['func_sysname'].unique()


# ## Estadísticas descriptivas

# In[34]:


df.describe(include="object")


# In[35]:


df.describe().T


# In[36]:


numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns

outlier_log = []

for col in numerical_cols:
    print(f"\n--- Analyzing Outliers for '{col}' ---")

    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_limit = Q1 - 1.5 * IQR
    upper_limit = Q3 + 1.5 * IQR

    outliers = df[(df[col] < lower_limit) | (df[col] > upper_limit)]
    num_outliers = len(outliers)

    print(f"Number of Outliers: {num_outliers}")
    print(f"Lower Limit: {lower_limit:.2f}, Upper Limit: {upper_limit:.2f}")

    outlier_log.append({
        'Variable': col,
        'Outliers': num_outliers,
        'Lower Limit': lower_limit,
        'Upper Limit': upper_limit,
        'Porcentaje de Outliers': round(num_outliers / len(df) * 100, 2)
    })

print("\nResumen de Outliers:")
print(outlier_log)


# # Transformations for the dimensional modeling

# In[37]:


df.columns


# In[38]:


df = df.drop(columns=['statename', 'state'])


# In[57]:


df["lgt_condname"].unique()


# In[40]:


visibility_mapping = {
    'Daylight': 'Alta',
    'Dawn': 'Moderada',
    'Dusk': 'Moderada',
    'Dark - Lighted': 'Baja',
    'Dark - Not Lighted': 'Muy Baja',
    'Dark - Unknown Lighting': 'Muy Baja',
    'Unknown': 'Unknown',
    'Not Reported': 'Unknown',
    'Reported as Unknown': 'Unknown',
    'Other': 'Unknown'
}

df["lgt_condname"] = df["lgt_condname"].map(visibility_mapping)


# In[41]:


print(df['alc_res'].unique())


# In[42]:


# Convertir el BAC a su valor real en g/dL
df['alc_res'] = df['alc_res'] / 1000

# Función de clasificación
def clasificar_bac(bac):
    if bac <= 0.03:
        return "Bajo"
    elif bac <= 0.08:
        return "Moderado"
    elif bac <= 0.20:
        return "Alto"
    elif bac <= 0.40:
        return "Peligroso"
    else:
        return "Letal"

# Aplicar la función
df['alc_res'] = df['alc_res'].apply(clasificar_bac)

# Mostrar el resultado
print(df.head())


# In[43]:


df["weathername"].unique()


# In[44]:


weather_map = {
    'Clear': 'Clear',
    'Rain': 'Rainy',
    'Cloudy': 'Windy',
    'Fog, Smog, Smoke': 'Foggy',
    'Snow': 'Snowy',
    'Sleet or Hail': 'Snowy',
    'Freezing Rain or Drizzle': 'Rainy',
    'Blowing Snow': 'Snowy',
    'Blowing Sand, Soil, Dirt': 'Windy',
    'Severe Crosswinds': 'Windy',
    'Clear': 'Clear',
    'Not Reported': 'Unknown',
    'Other': 'Unknown',
    'Reported as Unknown': 'Unknown'
    # 'Unknown' se mantiene igual
}

# Aplicamos el reemplazo
df['weathername'] = df['weathername'].replace(weather_map)


# In[45]:


accidents['arr_hour'].unique()


# In[46]:


df[df["arr_hour"] == 99]["arr_hour"].nunique()


# In[47]:


df[df["arr_hour"] == 88]["arr_hour"].nunique()


# In[48]:


df[df["arr_min"].isin([88, 97, 98, 99])]["arr_hour"].nunique()


# In[49]:


df['arr_min'].unique()


# In[54]:


from datetime import time

# Aseguramos que no haya NaN y convertimos
df['arr_hour'] = df['arr_hour'].fillna(0).astype(int)
df['arr_min'] = df['arr_min'].fillna(0).astype(int)

# Creamos la columna con objetos tipo datetime.time
df['arr_time'] = df.apply(lambda row: time(row['arr_hour'], row['arr_min']), axis=1)

print(df[['arr_hour', 'arr_min', 'arr_time']])


# In[72]:


df['hour'].unique()


# In[76]:


df['minute'].unique()


# In[ ]:


df = df[~df['hour'].isin([99, 98])]
df = df[~df['minute'].isin([99, 98])]

# Aseguramos que no haya NaN y convertimos
df['hour'] = df['hour'].fillna(0).astype(int)
df['minute'] = df['minute'].fillna(0).astype(int)

# Creamos la columna con objetos tipo datetime.time
df['time'] = df.apply(lambda row: time(row['hour'], row['minute']), axis=1)

print(df[['hour', 'minute', 'time']])


# In[ ]:


df = df[~df['hour'].isin([99, 98])]
df = df[~df['minute'].isin([99, 98])]


# Ejemplo de DataFrame (reemplaza con tu DataFrame real)
df = pd.DataFrame({
    'hour': [14, 14, 14, 4, 4, 23],
    'minute': [59, 59, 59, 57, 57, 0],
    'time': [time(14, 59), time(14, 59), time(14, 59), time(4, 57), time(4, 57), time(23, 0)],
    'arr_hour': [15, 15, 15, 5, 5, 2],
    'arr_min': [9, 9, 9, 25, 25, 0],
    'arr_time': [time(15, 9), time(15, 9), time(15, 9), time(5, 25), time(5, 25), time(2, 0)]
})

# Función para calcular la diferencia entre dos objetos time
def time_diff(t1, t2):
    # Convertir a datetime con una fecha ficticia
    dt1 = datetime(2023, 1, 1, t1.hour, t1.minute, t1.second)
    dt2 = datetime(2023, 1, 1, t2.hour, t2.minute, t2.second)

    # Calcular diferencia (arr_time - time)
    diff = dt2 - dt1

    # Si arr_time es menor que time, asumir que es del día siguiente
    if diff.total_seconds() < 0:
        dt2 = datetime(2023, 1, 2, t2.hour, t2.minute, t2.second)  # Día siguiente
        diff = dt2 - dt1

    # Convertir la diferencia a horas, minutos, segundos
    total_seconds = diff.total_seconds()
    hours = int(total_seconds // 3600) % 24  # Tomar módulo 24 para mantener horas en 0-23
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)

    return time(hours, minutes, seconds)

# Manejar valores nulos y calcular la diferencia
df['diff_time'] = df.apply(
    lambda row: time_diff(row['time'], row['arr_time']) 
    if pd.notnull(row['time']) and pd.notnull(row['arr_time']) 
    else None, 
    axis=1
)

# Mostrar el resultado
print(df[['time', 'arr_time', 'diff_time']])


# In[56]:


df.columns


# In[69]:


df['func_sysname'].unique()


# Tabla de correspondencias entre `func_sysname` y los valores únicos en `'road_type'` del modelo dimensional:
# 
# |  `func_sysname` | Correspondencia con `road_type` |
# |------------|--------------------------------|
# | Interstate | Highway                        |
# | Principal Arterial – Other Freeways and Expressways | Highway |
# | Principal Arterial – Other | Main Road |
# | Minor Arterial | Main Road |
# | Major Collector | Main Road |
# | Minor Collector | Street |
# | Local | Street |
# 

# In[77]:


# Diccionario de mapeo entre 'func_sysname' y 'road_type'
mapping = {
    'Interstate': 'Highway',
    'Principal Arterial - Other Freeways and Expressways': 'Highway',
    'Principal Arterial - Other': 'Main Road',
    'Minor Arterial': 'Main Road',
    'Major Collector': 'Main Road',
    'Minor Collector': 'Street',
    'Local': 'Street'
}

# Aplicar el mapeo en la columna 'func_sysname'
df['func_sysname'] = df['func_sysname'].map(mapping)


# In[79]:


df['func_sysname'].unique()


# In[63]:


df["age"].unique()


# In[67]:


# Elimina las filas con edades inválidas (998 y 999)
df = df[~df['age'].isin([998, 999])]

def map_driver_age(age):
    if age < 18:
        return '<18'
    elif 18 <= age <= 25:
        return '18-25'
    elif 26 <= age <= 40:
        return '26-40'
    elif 41 <= age <= 60:
        return '41-60'
    elif age > 60:
        return '61+'

df['age'] = df['age'].apply(map_driver_age)

print(df['age'])


# In[68]:


df.columns


# In[86]:


column_rename_map = {
    've_total': 'number_of_vehicles_involved',
    'fatals': 'number_of_fatalities',
    'peds': 'pedestrians_involved',
    'year': 'year',
    'monthname': 'month',
    'day_weekname': 'day_of_week',
    'rur_urbname': 'urban_rural',
    'func_sysname': 'road_type',
    'weathername': 'weather_conditions',
    'lgt_condname': 'visibility_level',
    'harm_evname': 'accident_cause',
    'age': 'driver_age_group',
    'sex': 'driver_gender',
    'alc_res': 'driver_alcohol_level',
    'diff_time': 'emergency_response_time',
}

df.rename(columns=column_rename_map, inplace=True)


# In[87]:


df.columns


# In[88]:


df = df.drop(columns=['hour','minute', 'arr_hour', 'arr_min', 'time', 'arr_time'])


# In[89]:


df.columns

