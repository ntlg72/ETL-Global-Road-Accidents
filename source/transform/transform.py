import os
import sys
import pandas as pd
import logging
import tempfile

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Usar ruta temporal para evitar errores de permisos
ruta_salida = os.path.join(tempfile.gettempdir(), "data")


def transform_accidents_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones al DataFrame de accidentes:
    - Elimina columnas innecesarias
    - Redondea columnas float
    - Crea columnas categorizadas
    - Convierte fechas a categóricas ordenadas
    - Convierte columnas booleanas
    """
    try:
        df = df.drop(columns=["region", "insurance_claims", "medical_cost", "economic_loss"])

        # Redondear columnas float
        columnas_float = df.select_dtypes(include=['float'])
        for col in columnas_float.columns:
            max_decimales = columnas_float[col].astype(str).str.split('.').str[1].str.len().max()
            print(f"Número máximo de decimales en la columna '{col}': {max_decimales}")
        df[columnas_float.columns] = columnas_float.round(2)

        # Categorizar nivel de alcohol
        def categorizar_alcohol_level(level):
            if level < 0.03:
                return "Bajo"
            elif level < 0.08:
                return "Moderado"
            elif level < 0.20:
                return "Alto"
            elif level < 0.30:
                return "Peligroso"
            else:
                return "Letal"

        if "driver_alcohol_level" in df.columns:
            df["driver_alcohol_level"] = df["driver_alcohol_level"].apply(categorizar_alcohol_level)
            print("Columna 'driver_alcohol_level' categorizada.")

        # Categorizar visibilidad
        def categorize_visibility(level):
            if level < 200:
                return "Muy Baja"
            elif level < 300:
                return "Baja"
            elif level < 400:
                return "Moderada"
            else:
                return "Alta"

        if "visibility_level" in df.columns:
            df["visibility_level"] = df["visibility_level"].apply(categorize_visibility)
            print("Columna 'visibility_level' categorizada.")

        # Convertir columnas temporales a categóricas ordenadas
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        months_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        time_of_day_order = ["Morning", "Afternoon", "Evening", "Night"]

        if "day_of_week" in df.columns:
            df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=days_order, ordered=True)
        if "month" in df.columns:
            df["month"] = pd.Categorical(df["month"], categories=months_order, ordered=True)
        if "time_of_day" in df.columns:
            df["time_of_day"] = pd.Categorical(df["time_of_day"], categories=time_of_day_order, ordered=True)
        print("Columnas temporales convertidas a categóricas ordenadas.")

        # Convertir driver_fatigue a bool
        if "driver_fatigue" in df.columns:
            df["driver_fatigue"] = df["driver_fatigue"].astype(bool)
            print("Columna 'driver_fatigue' convertida a tipo booleano.")

        return df

    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise


def split_transformed_data(df: pd.DataFrame, ruta_salida: str = ruta_salida):
    """
    Prepara las tablas dimensionales y de hechos, y las guarda en CSV.
    """
    try:
        os.makedirs(ruta_salida, exist_ok=True)
        logging.info(f"📂 Directorio de salida creado: {ruta_salida}")

        # Crear tablas dimensionales
        dim_lugar = df[["country", "urban_rural", "road_type", "road_condition"]].drop_duplicates().reset_index(drop=True)
        dim_lugar["id_lugar"] = dim_lugar.index + 1
        dim_lugar.to_csv(os.path.join(ruta_salida, "dim_lugar.csv"), index=False)

        dim_fecha = df[["year", "month", "day_of_week", "time_of_day"]].drop_duplicates().reset_index(drop=True)
        dim_fecha["id_fecha"] = dim_fecha.index + 1
        dim_fecha.to_csv(os.path.join(ruta_salida, "dim_fecha.csv"), index=False)

        dim_condiciones = df[["weather_conditions", "visibility_level"]].drop_duplicates().reset_index(drop=True)
        dim_condiciones["id_condiciones"] = dim_condiciones.index + 1
        dim_condiciones.to_csv(os.path.join(ruta_salida, "dim_condiciones.csv"), index=False)

        dim_conductor = df[["driver_age_group", "driver_alcohol_level", "driver_fatigue"]].drop_duplicates().reset_index(drop=True)
        dim_conductor["id_conductor"] = dim_conductor.index + 1
        dim_conductor.to_csv(os.path.join(ruta_salida, "dim_conductor.csv"), index=False)

        dim_incidente = df[["accident_severity", "accident_cause"]].drop_duplicates().reset_index(drop=True)
        dim_incidente["id_incidente"] = dim_incidente.index + 1
        dim_incidente.to_csv(os.path.join(ruta_salida, "dim_incidente.csv"), index=False)

        dim_vehiculo = df[["vehicle_condition"]].drop_duplicates().reset_index(drop=True)
        dim_vehiculo["id_vehiculo"] = dim_vehiculo.index + 1
        dim_vehiculo.to_csv(os.path.join(ruta_salida, "dim_vehiculo.csv"), index=False)

        # Crear tabla de hechos
        df_hechos = df \
            .merge(dim_lugar, on=["country", "urban_rural", "road_type", "road_condition"]) \
            .merge(dim_fecha, on=["year", "month", "day_of_week", "time_of_day"]) \
            .merge(dim_condiciones, on=["weather_conditions", "visibility_level"]) \
            .merge(dim_conductor, on=["driver_age_group", "driver_alcohol_level", "driver_fatigue"]) \
            .merge(dim_incidente, on=["accident_severity", "accident_cause"]) \
            .merge(dim_vehiculo, on=["vehicle_condition"])

        hechos_accidentes = df_hechos[[
            "number_of_vehicles_involved", "speed_limit", "number_of_injuries", "number_of_fatalities",
            "emergency_response_time", "traffic_volume", "pedestrians_involved", "cyclists_involved", "population_density",
            "id_lugar", "id_fecha", "id_condiciones", "id_conductor", "id_incidente", "id_vehiculo"
        ]].copy()

        hechos_accidentes["id"] = hechos_accidentes.index + 1
        hechos_accidentes.to_csv(os.path.join(ruta_salida, "hechos_accidentes.csv"), index=False)
        logging.info("✅ Tabla 'hechos_accidentes' creada y guardada.")

    except Exception as e:
        logging.error(f"❌ Error al preparar las tablas: {e}")
        raise
