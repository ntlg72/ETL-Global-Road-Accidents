import pandas as pd
import pytest
import numpy as np
from unittest.mock import patch, MagicMock
from source.transform.transform import transform_accidents_data
import os
import logging
import warnings

# --- Fixture for Test DataFrame ---
@pytest.fixture
def test_dataframe():
    """Crea un DataFrame de prueba con todas las columnas relevantes."""
    data = {
        "region": ["North", "South", "East"],
        "insurance_claims": [1000, 2000, 1500],
        "medical_cost": [500.123456, 600.567890, 700.901234],
        "economic_loss": [10000, 20000, 15000],
        "driver_alcohol_level": [0.02, 0.10, 0.40],
        "visibility_level": [100, 300, 500],
        "day_of_week": ["Monday", "Friday", "Sunday"],
        "month": ["January", "June", "December"],
        "time_of_day": ["Morning", "Afternoon", "Night"],
        "driver_fatigue": [True, False, 1],
        "year": [2023, 2023, 2023],
        "country": ["USA", "USA", "USA"],
        "urban_rural": ["Urban", "Rural", "Urban"],
        "road_type": ["Highway", "Street", "Main Road"],
        "road_condition": ["Dry", "Wet", "Dry"],
        "weather_conditions": ["Clear", "Rainy", "Clear"],
        "driver_age_group": ["18-25", "26-40", "41-60"],
        "driver_gender": ["Male", "Female", "Other"],
        "accident_severity": ["Minor", "Major", "Fatal"],
        "accident_cause": ["Collision", "Rollover", "Pedestrian"],
        "vehicle_condition": ["Good", "Poor", "Good"],
        "number_of_vehicles_involved": [2, 1, 3],
        "speed_limit": [55, 65, 70],
        "number_of_injuries": [1, 2, 0],
        "number_of_fatalities": [0, 1, 2],
        "emergency_response_time": ["00:10:00", "00:15:00", "00:20:00"],
        "traffic_volume": [1000, 2000, 1500],
        "pedestrians_involved": [0, 1, 0],
        "cyclists_involved": [0, 0, 1],
        "population_density": [500.1234, 600.5678, 700.9012]
    }
    return pd.DataFrame(data)

# --- Tests for transform_accidents_data ---

def test_transform_accidents_data(test_dataframe):
    """Prueba todas las transformaciones de la función."""
    df = test_dataframe.copy()
    transformed_df = transform_accidents_data(df)

    # Verify dropped columns
    dropped_columns = ["region", "insurance_claims", "medical_cost", "economic_loss"]
    assert not any(col in transformed_df.columns for col in dropped_columns), \
        f"Columnas {dropped_columns} no fueron eliminadas"

    # Verify float columns rounded to 2 decimals
    float_columns = ["population_density"]
    for col in float_columns:
        assert transformed_df[col].round(2).equals(transformed_df[col]), \
            f"Columna '{col}' no está redondeada a 2 decimales"

    # Verify driver_alcohol_level categorization
    expected_alcohol = ["Bajo", "Alto", "Letal"]
    assert transformed_df["driver_alcohol_level"].tolist() == expected_alcohol, \
        f"Categorías de alcohol esperadas: {expected_alcohol}, obtenidas: {transformed_df['driver_alcohol_level'].tolist()}"

    # Verify visibility_level categorization
    expected_visibility = ["Muy Baja", "Moderada", "Alta"]
    assert transformed_df["visibility_level"].tolist() == expected_visibility, \
        f"Categorías de visibilidad esperadas: {expected_visibility}, obtenidas: {transformed_df['visibility_level'].tolist()}"

    # Verify categorical columns
    assert transformed_df["day_of_week"].dtype == "category", "day_of_week debería ser categórico"
    assert transformed_df["day_of_week"].cat.categories.tolist() == [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ], "Orden de días incorrecto"
    assert transformed_df["month"].dtype == "category", "month debería ser categórico"
    assert transformed_df["month"].cat.categories.tolist() == [
        "January", "February", "March", "April", "May", "June", "July",
        "August", "September", "October", "November", "December"
    ], "Orden de meses incorrecto"
    assert transformed_df["time_of_day"].dtype == "category", "time_of_day debería ser categórico"
    assert transformed_df["time_of_day"].cat.categories.tolist() == [
        "Morning", "Afternoon", "Evening", "Night"
    ], "Orden de time_of_day incorrecto"

    # Verify driver_fatigue boolean conversion
    assert transformed_df["driver_fatigue"].dtype == bool, "driver_fatigue debería ser booleano"
    assert transformed_df["driver_fatigue"].tolist() == [True, False, True], \
        f"Valores booleanos esperados: [True, False, True], obtenidas: {transformed_df['driver_fatigue'].tolist()}"

def test_transform_accidents_data_missing_columns():
    """Prueba la función con columnas faltantes."""
    df = pd.DataFrame({
        "number_of_vehicles_involved": [2, 1],
        "speed_limit": [55, 65]
    })
    with pytest.raises(KeyError, match="\\['region', 'insurance_claims', 'medical_cost', 'economic_loss'\\]"):
        transform_accidents_data(df)

def test_transform_accidents_data_empty_dataframe():
    """Prueba la función con un DataFrame vacío."""
    df = pd.DataFrame()
    with pytest.raises(KeyError, match="\\['region', 'insurance_claims', 'medical_cost', 'economic_loss'\\]"):
        transform_accidents_data(df)

def test_transform_accidents_data_invalid_data():
    """Prueba la función con datos inválidos."""
    df = pd.DataFrame({
        "region": ["North", "South"],
        "insurance_claims": [1000, 2000],
        "medical_cost": [500.1234, 600.5678],
        "economic_loss": [10000, 20000],
        "driver_alcohol_level": [np.nan, -0.01],
        "visibility_level": [np.nan, -100],
        "driver_fatigue": [None, "invalid"]
    })
    transformed_df = transform_accidents_data(df)
    expected_alcohol = ["Letal", "Bajo"]
    assert transformed_df["driver_alcohol_level"].tolist() == expected_alcohol, \
        f"Categorías de alcohol esperadas: {expected_alcohol}, obtenidas: {transformed_df['driver_alcohol_level'].tolist()}"
    expected_visibility = ["Alta", "Muy Baja"]
    assert transformed_df["visibility_level"].tolist() == expected_visibility, \
        f"Categorías de visibilidad esperadas: {expected_visibility}, obtenidas: {transformed_df['visibility_level'].tolist()}"
    assert transformed_df["driver_fatigue"].tolist() == [False, True], \
        "Valores inválidos en driver_fatigue no convertidos a False"

