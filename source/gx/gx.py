import great_expectations as gx
import pandas as pd
import os
import json
from datetime import datetime

# --- Función auxiliar para guardar resultados ---
def guardar_resultado(nombre, resultado, results_dir):
    os.makedirs(results_dir, exist_ok=True)
    ruta = os.path.join(results_dir, f"{nombre}.json")

    def safe_to_dict(obj):
        if hasattr(obj, "to_json_dict"):
            return obj.to_json_dict()
        elif hasattr(obj, "to_dict"):
            return obj.to_dict()
        elif isinstance(obj, dict):
            return {k: safe_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [safe_to_dict(v) for v in obj]
        else:
            return obj

    with open(ruta, "w") as f:
        json.dump(safe_to_dict(resultado), f, indent=2)


# --- Cargar dataframe y crear contexto y batch ---
def cargar_batch(csv_path):
    df = pd.read_csv(csv_path)
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="accidentes_dataframe")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("accidentes_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    return df, context, batch

# --- Funciones por tipo de validación ---

def validar_categoricas(batch, timestamp, results_dir):
    categorical_columns = [
        "road_condition", "weather_conditions", "visibility_level", "accident_severity", 
        "accident_cause", "driver_age_group", "driver_alcohol_level", "driver_gender", 
        "vehicle_condition", "urban_rural", "country", "road_type", "time_of_day", 
        "day_of_week"
    ]
    for col in categorical_columns:
        expectation = gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="object")
        resultado = batch.validate(expectation)
        guardar_resultado(f"{timestamp}_type_{col}", resultado, results_dir)

def validar_no_negativos(batch, timestamp, results_dir):
    non_negative_columns = [
        "number_of_vehicles_involved", "number_of_fatalities", "pedestrians_involved"
    ]
    for col in non_negative_columns:
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
        resultado = batch.validate(expectation)
        guardar_resultado(f"{timestamp}_nonneg_{col}", resultado, results_dir)

def validar_valores_negativos_uno(batch, timestamp, results_dir):
    columns_allowing_negative_one = [
        "driver_fatigue", "speed_limit", "number_of_injuries", "cyclists_involved", "population_density",  "traffic_volume"
    ]
    for col in columns_allowing_negative_one:
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=-1)
        resultado = batch.validate(expectation)
        guardar_resultado(f"{timestamp}_neg1_{col}", resultado, results_dir)

def validar_enteros(batch, timestamp, results_dir):
    integer_columns = [
        "number_of_vehicles_involved", "number_of_injuries",
        "number_of_fatalities", "pedestrians_involved", "cyclists_involved"
    ]
    for col in integer_columns:
        expectation = gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="int64")
        resultado = batch.validate(expectation)
        guardar_resultado(f"{timestamp}_int_{col}", resultado, results_dir)

def validar_no_nulos(batch, timestamp, results_dir):
    nullable_columns = [
        "road_condition", "accident_severity", "speed_limit",
        "number_of_injuries", "traffic_volume", "cyclists_involved",
        "population_density", "driver_fatigue", "vehicle_condition"
    ]
    for col in nullable_columns:
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.8)
        resultado = batch.validate(expectation)
        guardar_resultado(f"{timestamp}_nullable_{col}", resultado, results_dir)

def validar_columnas_esperadas(batch, timestamp, results_dir):
    expected_columns = [
        "number_of_vehicles_involved", "speed_limit", "number_of_injuries", "number_of_fatalities",
        "emergency_response_time", "traffic_volume", "pedestrians_involved", "cyclists_involved",
        "population_density",
        "year", "month", "day_of_week", "time_of_day",
        "country", "urban_rural", "road_type", "road_condition",
        "weather_conditions", "visibility_level",
        "driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender",
        "accident_severity", "accident_cause",
        "vehicle_condition"
    ]
    expectation = gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected_columns)
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_expected_columns", resultado, results_dir)

# --- Función principal para ejecutar todas las validaciones ---
def ejecutar_validaciones(csv_path, results_dir):
    df, context, batch = cargar_batch(csv_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    validar_categoricas(batch, timestamp, results_dir)
    validar_no_negativos(batch, timestamp, results_dir)
    validar_valores_negativos_uno(batch, timestamp, results_dir)
    validar_enteros(batch, timestamp, results_dir)
    validar_no_nulos(batch, timestamp, results_dir)
    validar_columnas_esperadas(batch, timestamp, results_dir)

    print(f"✔️ Validaciones completadas. Resultados guardados en: {results_dir}")




