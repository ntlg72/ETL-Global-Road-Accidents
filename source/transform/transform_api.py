import pandas as pd
from datetime import datetime, time

"""
Módulo para transformar datos de accidentes y personas descargados desde la API FARS (NHTSA).
"""

def clean_special_values(df):
    """
    Limpia valores especiales o no deseados en el DataFrame, eliminando filas con datos no reportados o desconocidos.

    Args:
        df (pd.DataFrame): DataFrame con los datos a limpiar.

    Returns:
        pd.DataFrame: DataFrame limpio con filas y columnas filtradas.
    """
    df = df.loc[~df["rur_urbname"].isin(["Not Reported", "Trafficway Not in State Inventory", "Unknown"])].copy()
    df = df.loc[~df["weathername"].isin(["Not Reported", "Other", "Unknown", "Reported as Unknown"])].copy()
    df = df.loc[~df["lgt_condname"].isin(["Not Reported", "Other", "Unknown", "Reported as Unknown"])].copy()
    df = df.loc[~df["sex"].isin(["Not Reported", "Unknown"])].copy()
    df = df.drop(columns=["statename", "state"])
    df = df[~df['hour'].isin([99, 98]) & ~df['minute'].isin([99, 98])]
    return df

def map_visibility(df):
    """
    Mapea los valores de visibilidad a categorías simplificadas.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'lgt_condname' a mapear.

    Returns:
        pd.DataFrame: DataFrame con la columna 'lgt_condname' transformada.
    """
    visibility_mapping = {
        'Daylight': 'Alta', 'Dawn': 'Moderada', 'Dusk': 'Moderada',
        'Dark - Lighted': 'Baja', 'Dark - Not Lighted': 'Muy Baja',
        'Dark - Unknown Lighting': 'Muy Baja',
        'Unknown': 'Unknown', 'Not Reported': 'Unknown',
        'Reported as Unknown': 'Unknown', 'Other': 'Unknown'
    }
    df["lgt_condname"] = df["lgt_condname"].map(visibility_mapping)
    return df

def map_weather(df):
    """
    Mapea los valores de condiciones climáticas a categorías simplificadas.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'weathername' a mapear.

    Returns:
        pd.DataFrame: DataFrame con la columna 'weathername' transformada.
    """
    weather_map = {
        'Clear': 'Clear', 'Rain': 'Rainy', 'Cloudy': 'Windy',
        'Fog, Smog, Smoke': 'Foggy', 'Snow': 'Snowy',
        'Sleet or Hail': 'Snowy', 'Freezing Rain or Drizzle': 'Rainy',
        'Blowing Snow': 'Snowy', 'Blowing Sand, Soil, Dirt': 'Windy',
        'Severe Crosswinds': 'Windy',
        'Not Reported': 'Unknown', 'Other': 'Unknown', 'Reported as Unknown': 'Unknown'
    }
    df['weathername'] = df['weathername'].replace(weather_map)
    return df

def map_sex(df):
    """
    Convierte códigos numéricos de sexo a etiquetas descriptivas.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'sex' a mapear.

    Returns:
        pd.DataFrame: DataFrame con la columna 'sex' transformada.
    """
    sex_mapping = {1: "Male", 2: "Female", 3: "Other"}
    df["sex"] = df["sex"].map(sex_mapping)
    return df

def clean_arrival_times(df):
    """
    Limpia valores especiales en las columnas de tiempo de llegada y filtra filas inválidas.

    Args:
        df (pd.DataFrame): DataFrame con las columnas 'arr_hour' y 'arr_min'.

    Returns:
        pd.DataFrame: DataFrame con tiempos de llegada limpios.
    """
    special_codes = {'arr_hour': [88, 99], 'arr_min': [88, 97, 98, 99]}
    for col, codes in special_codes.items():
        df[col] = df[col].replace({code: -1 for code in codes})
    df = df.loc[~df[['arr_hour', 'arr_min']].isin([-1]).any(axis=1)].copy()
    return df

def convert_arrival_time(df):
    """
    Convierte las columnas de hora y minuto de llegada en una columna de tiempo.

    Args:
        df (pd.DataFrame): DataFrame con las columnas 'arr_hour' y 'arr_min'.

    Returns:
        pd.DataFrame: DataFrame con nueva columna 'arr_time' tipo time.
    """
    df['arr_hour'] = df['arr_hour'].fillna(0).astype(int)
    df['arr_min'] = df['arr_min'].fillna(0).astype(int)
    df['arr_time'] = df.apply(lambda row: time(row['arr_hour'], row['arr_min']), axis=1)
    return df

def convert_crash_time(df):
    """
    Convierte las columnas de hora y minuto del accidente en una columna de tiempo.

    Args:
        df (pd.DataFrame): DataFrame con las columnas 'hour' y 'minute'.

    Returns:
        pd.DataFrame: DataFrame con nueva columna 'time' tipo time.
    """
    df['hour'] = df['hour'].fillna(0).astype(int)
    df['minute'] = df['minute'].fillna(0).astype(int)
    df['time'] = df.apply(lambda row: time(row['hour'], row['minute']), axis=1)
    return df

def compute_diff_time(df):
    """
    Calcula la diferencia de tiempo entre el accidente y la llegada de emergencias.

    Args:
        df (pd.DataFrame): DataFrame con las columnas 'time' y 'arr_time'.

    Returns:
        pd.DataFrame: DataFrame con nueva columna 'diff_time' tipo time.
    """
    def time_diff(t1, t2):
        dt1 = datetime(2023, 1, 1, t1.hour, t1.minute, t1.second)
        dt2 = datetime(2023, 1, 1, t2.hour, t2.minute, t2.second)
        if dt2 < dt1:
            dt2 = datetime(2023, 1, 2, t2.hour, t2.minute, t2.second)
        diff = dt2 - dt1
        return time(diff.seconds // 3600, (diff.seconds % 3600) // 60, diff.seconds % 60)
    df['diff_time'] = df.apply(lambda row: time_diff(row['time'], row['arr_time']) if pd.notnull(row['time']) and pd.notnull(row['arr_time']) else None, axis=1)
    return df

def classify_alcohol_level(df):
    """
    Clasifica el nivel de alcohol en sangre en categorías descriptivas.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'alc_res'.

    Returns:
        pd.DataFrame: DataFrame con la columna 'alc_res' transformada a categorías.
    """
    df['alc_res'] = df['alc_res'] / 1000
    def clasificar_bac(bac):
        if bac <= 0.03: return "Bajo"
        elif bac <= 0.08: return "Moderado"
        elif bac <= 0.20: return "Alto"
        elif bac <= 0.40: return "Peligroso"
        else: return "Letal"
    df['alc_res'] = df['alc_res'].apply(clasificar_bac)
    return df

def simplify_road_type(df):
    """
    Simplifica los tipos de carretera en categorías más generales.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'func_sysname'.

    Returns:
        pd.DataFrame: DataFrame con la columna 'func_sysname' transformada.
    """
    df['func_sysname'] = df['func_sysname'].replace({'Not Reported': -1, 'Unknown': -1})
    df = df[~df['func_sysname'].isin([-1])]
    mapping = {
        'Interstate': 'Highway',
        'Principal Arterial - Other Freeways and Expressways': 'Highway',
        'Principal Arterial - Other': 'Main Road',
        'Minor Arterial': 'Main Road',
        'Major Collector': 'Main Road',
        'Minor Collector': 'Street',
        'Local': 'Street'
    }
    df['func_sysname'] = df['func_sysname'].map(mapping)
    return df

def bucket_driver_age(df):
    """
    Agrupa las edades de los conductores en rangos categóricos.

    Args:
        df (pd.DataFrame): DataFrame con la columna 'age'.

    Returns:
        pd.DataFrame: DataFrame con la columna 'age' transformada a rangos.
    """
    df = df[~df['age'].isin([998, 999])]
    def map_driver_age(age):
        if age < 18: return '<18'
        elif 18 <= age <= 25: return '18-25'
        elif 26 <= age <= 40: return '26-40'
        elif 41 <= age <= 60: return '41-60'
        else: return '61+'
    df['age'] = df['age'].apply(map_driver_age)
    return df

def add_country_column(df):
    """
    Agrega una columna 'country' con el valor 'USA' para todas las filas.

    Args:
        df (pd.DataFrame): DataFrame al que se le agregará la columna.

    Returns:
        pd.DataFrame: DataFrame con la nueva columna 'country'.
    """
    df['country'] = 'USA'
    return df

def rename_columns(df):
    """
    Renombra las columnas del DataFrame para nombres más descriptivos.

    Args:
        df (pd.DataFrame): DataFrame con las columnas a renombrar.

    Returns:
        pd.DataFrame: DataFrame con columnas renombradas.
    """
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
    return df

def drop_auxiliary_columns(df):
    """
    Elimina columnas auxiliares que no son necesarias tras las transformaciones.

    Args:
        df (pd.DataFrame): DataFrame con columnas a eliminar.

    Returns:
        pd.DataFrame: DataFrame sin las columnas especificadas.
    """
    return df.drop(columns=['hour', 'minute', 'arr_hour', 'arr_min', 'time', 'arr_time'])

def transform_data(df):
    """
    Aplica todas las transformaciones al DataFrame de datos de accidentes y personas.

    Args:
        df (pd.DataFrame): DataFrame con los datos originales.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.

    Raises:
        Exception: Si ocurre un error durante alguna transformación.
    """
    try:
        df = clean_special_values(df)
        df = map_visibility(df)
        df = map_weather(df)
        df = map_sex(df)
        df = clean_arrival_times(df)
        df = convert_arrival_time(df)
        df = convert_crash_time(df)
        df = compute_diff_time(df)
        df = classify_alcohol_level(df)
        df = simplify_road_type(df)
        df = bucket_driver_age(df)
        df = add_country_column(df)
        df = rename_columns(df)
        df = drop_auxiliary_columns(df)
        return df
    except Exception as e:
        raise Exception(f"Error durante la transformación de datos: {e}")
