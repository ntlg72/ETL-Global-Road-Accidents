import pandas as pd
from datetime import datetime, time

def clean_special_values(df):
    """
    Removes invalid or special values from the dataset.
    """
    filters = {
        "rur_urbname": ["Not Reported", "Trafficway Not in State Inventory", "Unknown"],
        "sex": ["Not Reported", "Unknown"],
        "func_sysname": ["Not Reported", "Unknown"],
        "weathername": ["Not Reported", "Unknown", "Reported as Unknown", "Other"],
        "lgt_condname": ["Not Reported", "Unknown", "Reported as Unknown", "Other"],
        "hour": [98, 99],
        "minute": [98, 99],
    }

    for col, invalid_values in filters.items():
        df = df[~df[col].isin(invalid_values)]

    df = df.drop(columns=["statename", "state"])
    return df.copy()

def map_visibility(df):
    mapping = {
        'Daylight': 'Alta',
        'Dawn': 'Moderada',
        'Dusk': 'Moderada',
        'Dark - Lighted': 'Baja',
        'Dark - Not Lighted': 'Muy Baja',
        'Dark - Unknown Lighting': 'Muy Baja',
    }
    df["lgt_condname"] = df["lgt_condname"].map(mapping)
    return df

def map_weather(df):
    mapping = {
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
    }
    df['weathername'] = df['weathername'].map(mapping)
    return df

def map_sex(df):
    return df.assign(sex=df['sex'].map({1: "Male", 2: "Female", 3: "Other"}))

def clean_arrival_times(df):
    df['arr_hour'].replace([88, 99], -1, inplace=True)
    df['arr_min'].replace([88, 97, 98, 99], -1, inplace=True)
    return df.loc[~df[['arr_hour', 'arr_min']].isin([-1]).any(axis=1)]

def convert_arrival_time(df):
    df['arr_hour'] = df['arr_hour'].fillna(0).astype(int)
    df['arr_min'] = df['arr_min'].fillna(0).astype(int)
    df['arr_time'] = df.apply(lambda row: time(row['arr_hour'], row['arr_min']), axis=1)
    return df

def convert_crash_time(df):
    df['hour'] = df['hour'].fillna(0).astype(int)
    df['minute'] = df['minute'].fillna(0).astype(int)
    df['time'] = df.apply(lambda row: time(row['hour'], row['minute']), axis=1)
    return df

def compute_diff_time(df):
    def time_diff_str(t1, t2):
        dt1 = datetime(2023, 1, 1, t1.hour, t1.minute)
        dt2 = datetime(2023, 1, 1, t2.hour, t2.minute)
        if dt2 < dt1:
            dt2 = datetime(2023, 1, 2, t2.hour, t2.minute)
        diff = dt2 - dt1
        hours, remainder = divmod(diff.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    df['diff_time'] = df.apply(
        lambda row: time_diff_str(row['time'], row['arr_time'])
        if pd.notnull(row['time']) and pd.notnull(row['arr_time']) else None,
        axis=1
    )
    return df

def classify_alcohol_level(df):
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
    df = df[~df['age'].isin([998, 999])]
    def map_driver_age(age):
        if age < 18: return '<18'
        elif 18 <= age <= 25: return '18-25'
        elif 26 <= age <= 40: return '26-40'
        elif 41 <= age <= 60: return '41-60'
        else: return '61+'
    df['age'] = df['age'].apply(map_driver_age)
    return df

def categorize_time(df):
    def map_time_of_day(row):
        hour = row.hour
        if 5 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 17:
            return "Afternoon"
        elif 17 <= hour < 21:
            return "Evening"
        else:
            return "Night"
    df['time'] = df['time'].apply(map_time_of_day).astype("category")
    return df

def add_country_column(df):
    df['country'] = 'USA'
    return df

def drop_auxiliary_columns(df):
    return df.drop(columns=['hour', 'minute', 'arr_hour', 'arr_min', 'arr_time'])

def rename_columns(df):
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
        'time': 'time_of_day'
    }
    df.rename(columns=column_rename_map, inplace=True)
    columnas_finales = list(column_rename_map.values())
    if 'country' in df.columns:
        columnas_finales.append('country')
    df = df[columnas_finales]
    return df


def transform_data(df):
    """
    Ejecuta todo el pipeline de transformación de datos.
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
        df = categorize_time(df)
        df = add_country_column(df)
        df = drop_auxiliary_columns(df)
        df = rename_columns(df)
        
        return df
    except Exception as e:
        raise Exception(f"Error durante la transformación de datos: {e}")
    