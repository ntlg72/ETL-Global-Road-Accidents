import great_expectations as ge

# Asumimos que df es un DataFrame de Pandas
# Si usas un Validator (recomendado en GE v3), reemplaza `df` por `validator`
df = ge.from_pandas(your_dataframe_here)

# 1. Categóricas
categorical_columns = [
    "road_condition", "weather_conditions", "visibility_level", "accident_severity", 
    "accident_cause", "driver_age_group", "driver_alcohol_level", "driver_gender", 
    "vehicle_condition", "urban_rural", "country", "road_type", "time_of_day", 
    "day_of_week"
]

for col in categorical_columns:
    df.expect_column_values_to_be_of_type(col, "object")

# 2. Enteros no negativos
non_negative_columns = [
    "number_of_vehicles_involved", "speed_limit", "number_of_injuries",
    "number_of_fatalities", "traffic_volume", "pedestrians_involved",
    "cyclists_involved", "population_density"
]

for col in non_negative_columns:
    df.expect_column_values_to_be_between(col, min_value=0)

# 3. Asegurar enteros (no floats)
integer_columns = [
    "number_of_vehicles_involved", "number_of_injuries",
    "number_of_fatalities", "pedestrians_involved", "cyclists_involved"
]

for col in integer_columns:
    df.expect_column_values_to_be_of_type(col, "int64")

# 4. Columnas que pueden tener nulos (mostly = 0.8)
nullable_columns = [
    "road_condition", "accident_severity", "speed_limit",
    "number_of_injuries", "traffic_volume", "cyclists_involved",
    "population_density", "driver_fatigue", "vehicle_condition"
]

for col in nullable_columns:
    df.expect_column_values_to_not_be_null(column=col, mostly=0.8)

# 5. Verificación de existencia de columnas
expected_columns = [
    # Hechos_Accidentes
    "number_of_vehicles_involved", "speed_limit", "number_of_injuries", "number_of_fatalities",
    "emergency_response_time", "traffic_volume", "pedestrians_involved", "cyclists_involved",
    "population_density", "fecha_id", "lugar_id", "condiciones_id", "conductor_id",
    "vehiculo_id", "incidente_id",

    # Dim_Fecha
    "year", "month", "day_of_week", "time_of_day",

    # Dim_Lugar
    "country", "urban_rural", "road_type", "road_condition",

    # Dim_Condiciones
    "weather_conditions", "visibility_level",

    # Dim_Conductor
    "driver_age_group", "driver_alcohol_level", "driver_fatigue", "driver_gender",

    # Dim_Incidente
    "accident_severity", "accident_cause",

    # Dim_Vehiculo
    "vehicle_condition"
]

df.expect_table_columns_to_match_set(expected_columns)

# (Opcional) Mostrar resultados
results = df.validate()
print(results)
