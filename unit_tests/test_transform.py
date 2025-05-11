import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from source.transform.transform import transform_accidents_data, split_transformed_data

# --- Pruebas para transform_accidents_data ---

def create_test_dataframe():
    """Crea un DataFrame de prueba con todas las columnas relevantes para transform_accidents_data."""
    data = {
        "region": ["North", "South", "East"],
        "insurance_claims": [1000, 2000, 1500],
        "medical_cost": [500.1234, 600.5678, 700.9012],
        "economic_loss": [10000, 20000, 15000],
        "driver_alcohol_level": [0.02, 0.10, 0.40],
        "visibility_level": [100, 300, 500],
        "day_of_week": ["Monday", "Friday", "Sunday"],
        "month": ["January", "June", "December"],
        "time_of_day": ["Morning", "Afternoon", "Night"],
        "driver_fatigue": [True, False, 1],
        "speed": [55.1234, 66.5678, 77.9012]
    }
    return pd.DataFrame(data)


def test_transform_accidents_data():
    """Prueba todas las transformaciones de la función con un DataFrame completo."""
    df = create_test_dataframe()
    transformed_df = transform_accidents_data(df.copy())

    expected_columns = [
        "driver_alcohol_level", "visibility_level", "day_of_week",
        "month", "time_of_day", "driver_fatigue", "speed"
    ]
    assert list(transformed_df.columns) == expected_columns, \
        f"Columnas esperadas: {expected_columns}, obtenidas: {list(transformed_df.columns)}"

    assert transformed_df["speed"].round(2).equals(transformed_df["speed"]), \
        "La columna 'speed' no está redondeada a 2 decimales"

    expected_alcohol = ["Bajo", "Alto", "Letal"]
    assert transformed_df["driver_alcohol_level"].tolist() == expected_alcohol, \
        f"Categorías de alcohol esperadas: {expected_alcohol}, obtenidas: {transformed_df['driver_alcohol_level'].tolist()}"

    expected_visibility = ["Muy Baja", "Moderada", "Alta"]
    assert transformed_df["visibility_level"].tolist() == expected_visibility, \
        f"Categorías de visibilidad esperadas: {expected_visibility}, obtenidas: {transformed_df['visibility_level'].tolist()}"

    assert transformed_df["day_of_week"].dtype == "category", \
        "day_of_week debería ser categórico"
    assert transformed_df["day_of_week"].cat.categories.tolist() == [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ], "Orden de días incorrecto"
    assert transformed_df["month"].dtype == "category", \
        "month debería ser categórico"
    assert transformed_df["month"].cat.categories.tolist() == [
        "January", "February", "March", "April", "May", "June", "July",
        "August", "September", "October", "November", "December"
    ], "Orden de meses incorrecto"
    assert transformed_df["time_of_day"].dtype == "category", \
        "time_of_day debería ser categórico"
    assert transformed_df["time_of_day"].cat.categories.tolist() == [
        "Morning", "Afternoon", "Evening", "Night"
    ], "Orden de time_of_day incorrecto"

    assert transformed_df["driver_fatigue"].dtype == bool, \
        "driver_fatigue debería ser booleano"
    assert transformed_df["driver_fatigue"].tolist() == [True, False, True], \
        f"Valores booleanos esperados: [True, False, True], obtenidos: {transformed_df['driver_fatigue'].tolist()}"


def test_split_transformed_data():
    """Prueba la función split_transformed_data para asegurarse de que divide 
        correctamente el DataFrame."""
    df = create_test_dataframe()
    transformed_df = transform_accidents_data(df.copy())
  
    # Simulamos la función de división
    with patch('source.transform.transform.train_test_split', return_value=(transformed_df, transformed_df)):
        train_df, test_df = split_transformed_data(transformed_df)

        assert len(train_df) == len(transformed_df), \
            "El tamaño del DataFrame de entrenamiento no coincide con el original"
        assert len(test_df) == len(transformed_df), \
            "El tamaño del DataFrame de prueba no coincide con el original"
        assert train_df.equals(transformed_df), \
            "El DataFrame de entrenamiento no coincide con el original"
        assert test_df.equals(transformed_df), \
            "El DataFrame de prueba no coincide con el original"
# --- Pruebas para la función de carga ---  
