import os
import pandas as pd
import logging
import tempfile

# Configuración global de logs
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Directorio de salida para archivos procesados
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')
os.makedirs(TRANSFORMED_DIR, exist_ok=True)

def completar_columnas(df_base: pd.DataFrame, df_incompleto: pd.DataFrame) -> pd.DataFrame:
    """
    Valida los DataFrames y añade las columnas faltantes en `df_incompleto` con valores `-1`.

    Parámetros:
    ----------
    df_base : pd.DataFrame
        DataFrame de referencia con todas las columnas necesarias.

    df_incompleto : pd.DataFrame
        DataFrame que puede tener columnas faltantes.

    Retorna:
    -------
    pd.DataFrame
        DataFrame `df_incompleto` actualizado con todas las columnas necesarias.

    Excepciones:
    -----------
    ValueError:
        Se lanza si `df_base` o `df_incompleto` están vacíos.
    """
    try:
        # Validar que los DataFrames no estén vacíos
        if df_base.empty:
            raise ValueError("⚠️ El DataFrame `df_base` está vacío. No se puede completar.")
        if df_incompleto.empty:
            raise ValueError("⚠️ El DataFrame `df_incompleto` está vacío. No se puede completar.")

        # Identificar columnas faltantes
        columnas_faltantes = set(df_base.columns) - set(df_incompleto.columns)

        # Agregar columnas faltantes con valores -1
        for col in columnas_faltantes:
            df_incompleto[col] = -1

        logging.info("✅ Validación completada y columnas agregadas correctamente.")
        return df_incompleto

    except ValueError as e:
        logging.error(f"❌ Error de validación: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Error inesperado al completar columnas: {e}")
        raise

def unir_dataframes(df_transformed: pd.DataFrame, df_transformed_api: pd.DataFrame) -> pd.DataFrame:
    """Une `df_transformed` y `df_transformed_api` en un solo DataFrame consolidado."""
    try:
        df_final = pd.concat([df_transformed, df_transformed_api], ignore_index=True)
        logging.info("✅ DataFrames fusionados correctamente.")
        return df_final
    except Exception as e:
        logging.error(f"❌ Error al unir DataFrames: {e}")
        raise

def guardar_csv(df_final: pd.DataFrame, ruta_salida: str) -> str:
    """Guarda el DataFrame final en un archivo CSV y retorna la ruta del archivo."""
    try:
        output_file = os.path.join(ruta_salida, "transformed_accidents_data.csv")
        df_final.to_csv(output_file, index=False)

        logging.info(f"✅ Archivo guardado en: {output_file}")
        return output_file

    except Exception as e:
        logging.error(f"❌ Error al guardar el archivo CSV: {e}")
        raise

def merge_transformed_data(df_transformed: pd.DataFrame, df_transformed_api: pd.DataFrame, ruta_salida: str = TRANSFORMED_DIR) -> tuple[pd.DataFrame, str]:
    """
    Fusiona los datos transformados desde PostgreSQL y la API externa, asegurando que el conjunto incompleto
    contenga todas las columnas necesarias y se guarde correctamente.

    Parámetros:
    ----------
    df_transformed : pd.DataFrame
        DataFrame con los datos transformados desde PostgreSQL.

    df_transformed_api : pd.DataFrame
        DataFrame con los datos transformados desde la API.

    ruta_salida : str, opcional
        Directorio donde se guardará el archivo final (por defecto, TRANSFORMED_DIR).

    Retorna:
    -------
    tuple[pd.DataFrame, str]
        - DataFrame fusionado con todas las columnas completas.
        - Ruta del archivo CSV guardado.
    """
    try:
        df_transformed_api = completar_columnas(df_transformed, df_transformed_api)  # Completar columnas
        df_final = unir_dataframes(df_transformed, df_transformed_api)  # Unión de DataFrames
        output_file = guardar_csv(df_final, ruta_salida)  # Guardar resultado

        return df_final, output_file

    except ValueError as e:
        logging.error(f"❌ Error de validación en `merge_transformed_data`: {e}")
        return None, None
    except Exception as e:
        logging.error(f"❌ Error inesperado en `merge_transformed_data`: {e}")
        return None, None