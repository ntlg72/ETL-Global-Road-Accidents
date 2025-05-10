#!/usr/bin/env python
# coding: utf-8

# # EDA API

# ### Importación de librerías

# In[6]:


import sys
import os
import requests
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import math
sys.path.append(os.path.abspath('../source'))


# ### Descarga de Datos de Accidentes FARS desde la API de NHTSA

# Primero, carga un par de herramientas: requests para conectarse a la API y os para manejar carpetas. Luego, define la dirección web base de la API y unos encabezados que hacen que parezca que la petición viene de un navegador. Crea una carpeta llamada ../data para guardar los archivos, y si ya existe, no hay problema.
# 
# Después, usa un bucle para recorrer los años de 2017 a 2022. Para cada año, arma una dirección web con los detalles necesarios (como el año y que queremos CSV) y hace la petición a la API. Si todo sale bien (código 200), guarda los datos en un archivo con nombre como FARS_data_año.csv. Si algo falla, muestra un mensaje con el problema, ya sea porque tardó mucho o por otro error.

# In[ ]:


base_url = "https://crashviewer.nhtsa.dot.gov/CrashAPI/FARSData/GetFARSData"

# Encabezados
headers = {
    "Accept": "application/json", #TODO: cambiar a "Accept": "text/csv"
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Crear la carpeta para almacenar los archivos CSV
output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)

# Automatizar la descarga por año
for year in range(2017, 2023):  # De 2015 a 2022
    # Construir la URL para cada año dinámicamente
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
            print(f"✅ Datos del año {year} guardados exitosamente en {output_file}")
        else:
            print(f"❌ Error al obtener los datos para el año {year}: Código HTTP {response.status_code}")
            print(response.text)

    except requests.exceptions.Timeout:
        print(f"⏱️ La solicitud para el año {year} excedió el tiempo límite.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error en la solicitud para el año {year}: {e}")


# #### Carga de Datos de Accidentes FARS desde Archivos CSV

# para leer cada archivo CSV que está en la carpeta ../data. Cada archivo se guarda en una variable distinta: data_2017, data_2018, y así hasta data_2022. Estas variables quedan listas como tablas en la memoria para que las podamos analizar o usar después.

# In[7]:


data_2017 = pd.read_csv('../data/FARS_data_2017.csv')
data_2018 = pd.read_csv('../data/FARS_data_2018.csv')
data_2019 = pd.read_csv('../data/FARS_data_2019.csv')
data_2020 = pd.read_csv('../data/FARS_data_2020.csv')
data_2021 = pd.read_csv('../data/FARS_data_2021.csv')
data_2022 = pd.read_csv('../data/FARS_data_2022.csv')


# #### Unión de Datos de Accidentes FARS desde Archivos CSV

# Después, con pd.concat() junta todas esas tablas en una sola, llamada df. El ignore_index=True hace que los números de las filas se reinicien y no se mezclen los índices viejos. Por último, df.head() muestra las primeras 5 filas de la tabla para que veamos cómo quedó.
# 
# Esto es para unir todos los datasets en uno solo. Así, en lugar de tener los datos de cada año por separado, los tenemos en una sola tabla grande, más fácil de analizar o usar para sacar información.

# In[8]:


df = pd.concat([data_2017, data_2018, data_2019, data_2020, data_2021, data_2022], ignore_index=True)
df.head()


# ### Gen

# In[9]:


df.shape


# #### Tipos de Dtaos

# In[10]:


df.info()


# In[11]:


df.columns


# ### Limpieza

# ### PERSONS
# 
# Data Element ID Data Element Name SAS Name 
# P5/NM5 Age  AGE 
# P6/NM6 Sex  SEX 
# P7/NM7 Person Type  PER_TYP 
# P8/NM8 Injury Severity  INJ_SEV 
# P9 Seating Position  SEAT_POS 
# P10A Restraint System Use  REST_USE
# P13 Ejection  EJECTION
# P17C/NM19C Alcohol Test Result  ALC_RES
# P19A/NM21A Drug Test Status  DSTATUS
# P21/NM23 Died at Scene/En Route  DOA 
# 
# 
# ### VEHICLES
# 
# V4 Number of Occupants  NUMOCCS
# V6 Hit-and-Run  HIT_RUN 
# V16 NCSA Body Type  BODY_TYP
# D5 Driver’s License State  L_STATE 
# D7A Non-CDL License Type  L_TYPE 
# D11 Compliance with License Restrictions  L_RESTRI
# D14 Previous Recorded Crashes  PREV_ACC 
# D17 Previous Speeding Convictions  PREV_SPD 
# D18 Previous Other Moving Violation Convictions  PREV_OTH 
# D15C Previous Recorded Other Suspensions, 
# Revocations, or Withdrawals  
# PREV_SUS3 

# In[12]:


columns = [
    've_total', 'pernotmvit', 'permvit', 'fatals', 'peds',
    'hosp_hr', 'hosp_mn', 'arr_hour', 'arr_min',
    'year', 'monthname', 'day_weekname', 'hourname',
    'state', 'statename', 'rur_urbname', 'func_sysname', 'rel_roadname',
    'weathername', 'lgt_condname',
    'drunk_dr',
    'man_collname', 'harm_evname'
]

df = df[columns]


# In[13]:


df


# In[14]:


df.dtypes


# #### Manejo de datos de faltantes

# In[ ]:


df.isnull().sum()


# In[ ]:


df = df.dropna()


# In[ ]:


null_columns = df.columns[df.isnull().any()]
null_columns


# In[ ]:


df.isnull().sum()[df.isnull().sum() > 0]


# In[ ]:


df.duplicated().sum()


# In[ ]:


df.shape


# #### Manejo de datos duplicados

# In[ ]:


df[df.duplicated()]


# In[ ]:


df = df.drop_duplicates()


# In[ ]:


df.shape


# ### estadística descriptiva 

# In[ ]:


df.describe(include="object")


# In[ ]:


df['hourname'].value_counts()    


# In[ ]:


df.drop(df[df['hourname'] == 'Unknown Hours'].index, inplace = True)


# In[ ]:


df.shape


# In[ ]:


df['rur_urbname'].value_counts()    


# In[ ]:


df.drop(df[df["rur_urbname"] == "Not Reported"].index, inplace=True)
df.drop(df[df["rur_urbname"] == "Trafficway Not in State Inventory"].index, inplace=True)
df.drop(df[df["rur_urbname"] == "Unknown"].index, inplace=True)


# In[ ]:


df.columns


# In[ ]:


df.shape


# In[ ]:


df['weathername'].value_counts()  


# In[ ]:


df.drop(df[df["weathername"] == "Not Reported"].index, inplace=True)
df.drop(df[df["weathername"] == "Other"].index, inplace=True)
df.drop(df[df["weathername"] == "Unknown"].index, inplace=True)
df.drop(df[df["weathername"] == "Reported as Unknown"].index, inplace=True)


# In[ ]:


df.shape


# In[ ]:


df.describe().T


# In[ ]:


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


# ## Visualizaciones

# Grafico de barras par las variables numericas 

# In[ ]:


col_cat = ["monthname", "day_weekname", "hourname", "statename", "rur_urbname",
            "func_sysname", "rel_roadname", "weathername", "lgt_condname", 
            "man_collname", "harm_evname"]


dtype: object
n_rows = len(col_cat)
fig, axes = plt.subplots(nrows=n_rows, ncols=1, figsize=(20, 6 * n_rows))
fig.subplots_adjust(hspace=0.6)

if n_rows == 1:
    axes = [axes]

for i, col in enumerate(col_cat):
    sns.countplot(x=col, data=df, ax=axes[i], order=df[col].value_counts().index)
    axes[i].set_title(col, fontsize=14)
    axes[i].set_xlabel("")  
    axes[i].set_ylabel("Count")
    axes[i].tick_params(axis='x', rotation=30)

plt.tight_layout()
plt.show()


# In[ ]:


col_num = df.select_dtypes(include=['int64', 'float64']).columns

n_cols = 3
n_rows = math.ceil(len(col_num) / n_cols)

fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(15, 12))
axes = axes.flatten()
fig.subplots_adjust(hspace=0.4, wspace=0.4)

for i, col in enumerate(col_num):
    nbins = 10 if col == 'age' else 50
    sns.histplot(x=col, data=df, ax=axes[i], bins=nbins, kde=True)
    axes[i].set_title(f"{col}", fontsize=12)

# Si sobran ejes (porque no llenamos el grid), los escondemos
for j in range(i + 1, len(axes)):
    axes[j].set_visible(False)

plt.tight_layout()
plt.show()


# In[ ]:


numerical_cols = df.select_dtypes(include=['int64', 'float64'])
for col in numerical_cols:
    plt.figure(figsize=(8, 6))
    sns.boxplot(x=df[col], color='skyblue')  
    plt.title(f'Boxplot de {col}', fontsize=14)
    plt.xlabel('Valores', fontsize=10)
    plt.grid(True, axis='x', linestyle='--', alpha=0.6)
    plt.xticks(fontsize=8)
    plt.tight_layout()
    plt.show()

    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]

    if not outliers.empty:
        print(f"Outliers en {col}:")
        print(outliers[[col]])
        print(f"Número de outliers: {len(outliers)}")
        print("-" * 30)
    else:
        print(f"No se encontraron outliers en {col}")
        print("-" * 30)


# In[ ]:


sns.set(style="whitegrid")

# Tamaño general de los gráficos
plt.figure(figsize=(12, 10))

# Histograma 1: arr_hour
plt.subplot(2, 2, 1)
sns.histplot(df['arr_hour'], bins=24, kde=False, color="skyblue")
plt.title('Arrival Hour Distribution')
plt.xlabel('Hour of Arrival')
plt.ylabel('Frequency')

# Histograma 2: arr_min
plt.subplot(2, 2, 2)
sns.histplot(df['arr_min'], bins=60, kde=False, color="salmon")
plt.title('Arrival Minute Distribution')
plt.xlabel('Minute of Arrival')
plt.ylabel('Frequency')

# Histograma 3: hosp_hr
plt.subplot(2, 2, 3)
sns.histplot(df['hosp_hr'], bins=24, kde=False, color="lightgreen")
plt.title('Hospitalization Hour Distribution')
plt.xlabel('Hour of Hospitalization')
plt.ylabel('Frequency')

# Histograma 4: hosp_mn
plt.subplot(2, 2, 4)
sns.histplot(df['hosp_mn'], bins=60, kde=False, color="plum")
plt.title('Hospitalization Minute Distribution')
plt.xlabel('Minute of Hospitalization')
plt.ylabel('Frequency')

# Ajuste final
plt.tight_layout()
plt.show()


# In[ ]:


# Histograma de drunk_dr
plt.figure(figsize=(16, 4))

plt.subplot(1, 3, 1)
sns.histplot(df['drunk_dr'].dropna(), bins=30, kde=True, color="dodgerblue")
plt.title('Drunk Drivers Distribution')
plt.xlabel('Number of Drunk Drivers')
plt.ylabel('Frequency')

# Boxplot de drunk_dr
plt.subplot(1, 3, 2)
sns.boxplot(x=df['drunk_dr'], color="tomato")
plt.title('Drunk Drivers Boxplot')
plt.xlabel('Number of Drunk Drivers')

# Scatter plot: drunk_dr vs fatals
plt.subplot(1, 3, 3)
sns.scatterplot(x='drunk_dr', y='fatals', data=df, alpha=0.6, color="seagreen")
plt.title('Drunk Drivers vs Fatalities')
plt.xlabel('Number of Drunk Drivers')
plt.ylabel('Number of Fatalities')

plt.tight_layout()
plt.show()


# In[ ]:


correlation_matrix = numerical_cols.corr()

plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', cbar=True, square=True)
plt.title('Correlation Mat' \
'rix of All Columns')
plt.show()

