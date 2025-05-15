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

