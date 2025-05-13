import great_expectations as gx
import pandas as pd
import os
import json
import tempfile
from datetime import datetime

# Ruta al archivo transformado
TRANSFORMED_DIR = os.path.join(tempfile.gettempdir(), 'data')
csv_path = os.path.join(TRANSFORMED_DIR, "transformed_accidents_data.csv")

# Leer archivo
df = pd.read_csv(csv_path)

# 2. Crear contexto y conectar datos

context = gx.get_context()
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="accidentes_dataframe")
batch_definition = data_asset.add_batch_definition_whole_dataframe("accidentes_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# 3. Carpeta para resultados
results_dir = "../gx/results"
os.makedirs(results_dir, exist_ok=True)

# Función auxiliar para guardar resultados
def guardar_resultado(nombre, resultado):
    ruta = os.path.join(results_dir, f"{nombre}.json")
    with open(ruta, "w") as f:
        json.dump(resultado.to_dict(), f, indent=2)

# Timestamp para nombre de archivo
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# 4. Expectativas a aplicar
resultados = {}

# 1. Categóricas
categorical_columns = [
    "road_condition", "weather_conditions", "visibility_level", "accident_severity", 
    "accident_cause", "driver_age_group", "driver_alcohol_level", "driver_gender", 
    "vehicle_condition", "urban_rural", "country", "road_type", "time_of_day", 
    "day_of_week"
]

for col in categorical_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="object")
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_type_{col}", resultado)

# 2. Enteros no negativos
non_negative_columns = [
    "number_of_vehicles_involved", "number_of_fatalities", "pedestrians_involved"
]

for col in non_negative_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_nonneg_{col}", resultado)

# 3. Columnas que aceptan -1
columns_allowing_negative_one = [
    "road_condition", "accident_severity", "driver_fatigue",
    "speed_limit", "number_of_injuries", "cyclists_involved", "population_density",  "traffic_volume"
]

for col in columns_allowing_negative_one:
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=-1)
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_neg1_{col}", resultado)

# 4. Enteros
integer_columns = [
    "number_of_vehicles_involved", "number_of_injuries",
    "number_of_fatalities", "pedestrians_involved", "cyclists_involved"
]

for col in integer_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="int64")
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_int_{col}", resultado)

# 5. Columnas que permiten nulos
nullable_columns = [
    "road_condition", "accident_severity", "speed_limit",
    "number_of_injuries", "traffic_volume", "cyclists_involved",
    "population_density", "driver_fatigue", "vehicle_condition"
]

for col in nullable_columns:
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.8)
    resultado = batch.validate(expectation)
    guardar_resultado(f"{timestamp}_nullable_{col}", resultado)

# 6. Columnas esperadas
expected_columns = [
    "number_of_vehicles_involved", "speed_limit", "number_of_injuries", "number_of_fatalities",
    "emergency_response_time", "traffic_volume", "pedestrians_involved", "cyclists_involved",
    "population_density", "fecha_id", "lugar_id", "condiciones_id", "conductor_id",
    "vehiculo_id", "incidente_id",
    "year", "month", "day_of_week", "time_of_day",
    "country", "urban_rural", "road_type", "road_condition",
    "weather_conditions", "visibility_level",
    "driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender",
    "accident_severity", "accident_cause",
    "vehicle_condition"
]

expectation = gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected_columns)
resultado = batch.validate(expectation)
guardar_resultado(f"{timestamp}_expected_columns", resultado)

print(f"✔️ Validaciones completadas. Resultados guardados en: {results_dir}")

