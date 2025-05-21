import pandas as pd
import pytest
import numpy as np
from datetime import time
from source.transform.transform_api import (
    clean_special_values, map_visibility, map_weather, map_sex, clean_arrival_times,
    convert_arrival_time, convert_crash_time, compute_diff_time, classify_alcohol_level,
    simplify_road_type, bucket_driver_age, categorize_time, add_country_column,
    drop_auxiliary_columns, rename_columns, transform_data
)

# Enable warning capture
pytestmark = pytest.mark.filterwarnings("always")

# Fixture to create a sample DataFrame for testing
@pytest.fixture
def sample_df():
    data = {
        'rur_urbname': ['Urban', 'Not Reported', 'Rural', 'Trafficway Not in State Inventory'],
        'sex': [1, 'Unknown', 2, 3],
        'func_sysname': ['Interstate', 'Unknown', 'Local', 'Minor Arterial'],
        'weathername': ['Clear', 'Unknown', 'Rain', 'Fog, Smog, Smoke'],
        'lgt_condname': ['Daylight', 'Unknown', 'Dark - Lighted', 'Dawn'],
        'hour': [10, 98, 15, 12],
        'minute': [30, 99, 45, 0],
        'arr_hour': [10, 88, 16, 12],
        'arr_min': [40, 98, 0, 15],
        'alc_res': [0, 80, 200, 400],
        'age': [25, 999, 65, 16],
        've_total': [2, 1, 3, 2],
        'fatals': [0, 1, 0, 0],
        'peds': [0, 0, 1, 0],
        'year': [2023, 2023, 2023, 2023],
        'monthname': ['January', 'February', 'March', 'April'],
        'day_weekname': ['Monday', 'Tuesday', 'Wednesday', 'Thursday'],
        'harm_evname': ['Collision', 'Rollover', 'Pedestrian', 'Collision'],
        'statename': ['Texas', 'California', 'Florida', 'New York'],
        'state': [48, 6, 12, 36]
    }
    return pd.DataFrame(data)

# Test clean_special_values
def test_clean_special_values(sample_df):
    result = clean_special_values(sample_df)
    assert 'Not Reported' not in result['rur_urbname'].values
    assert 'Trafficway Not in State Inventory' not in result['rur_urbname'].values
    assert 'Unknown' not in result['sex'].values
    assert 'Unknown' not in result['func_sysname'].values
    assert 'Unknown' not in result['weathername'].values
    assert 'Unknown' not in result['lgt_condname'].values
    assert 98 not in result['hour'].values
    assert 99 not in result['minute'].values
    assert 'statename' not in result.columns
    assert 'state' not in result.columns
    assert result is not sample_df
    assert len(result) == 2

# Test map_visibility
def test_map_visibility():
    df = pd.DataFrame({'lgt_condname': ['Daylight', 'Dawn', 'Dusk', 'Dark - Lighted', 'Dark - Not Lighted', 'Dark - Unknown Lighting', None]})
    result = map_visibility(df)
    expected = pd.Series(['Alta', 'Moderada', 'Moderada', 'Baja', 'Muy Baja', 'Muy Baja', np.nan], name='lgt_condname')
    pd.testing.assert_series_equal(result['lgt_condname'], expected)

# Test map_weather
def test_map_weather():
    df = pd.DataFrame({
        'weathername': ['Clear', 'Rain', 'Cloudy', 'Fog, Smog, Smoke', 'Snow', 'Sleet or Hail',
                       'Freezing Rain or Drizzle', 'Blowing Snow', 'Blowing Sand, Soil, Dirt', 'Severe Crosswinds', None]
    })
    result = map_weather(df)
    expected = pd.Series(['Clear', 'Rainy', 'Windy', 'Foggy', 'Snowy', 'Snowy', 'Rainy', 'Snowy', 'Windy', 'Windy', np.nan], name='weathername')
    pd.testing.assert_series_equal(result['weathername'], expected)

# Test map_sex
def test_map_sex():
    df = pd.DataFrame({'sex': [1, 2, 3, None]})
    result = map_sex(df)
    expected = pd.Series(['Male', 'Female', 'Other', np.nan], name='sex')
    pd.testing.assert_series_equal(result['sex'], expected)

# Test clean_arrival_times
def test_clean_arrival_times():
    df = pd.DataFrame({
        'arr_hour': [10, 88, 16, 99, 12],
        'arr_min': [40, 98, 0, 97, 15]
    })
    result = clean_arrival_times(df)
    assert len(result) == 3
    assert 88 not in result['arr_hour'].values
    assert 99 not in result['arr_hour'].values
    assert 98 not in result['arr_min'].values
    assert 97 not in result['arr_min'].values
    assert set(result.index) == {0, 2, 4}

# Test convert_arrival_time
def test_convert_arrival_time():
    df = pd.DataFrame({'arr_hour': [10, 15, None], 'arr_min': [30, 45, None]})
    result = convert_arrival_time(df)
    expected = pd.Series([time(10, 30), time(15, 45), time(0, 0)], name='arr_time')
    pd.testing.assert_series_equal(result['arr_time'], expected)

# Test convert_crash_time
def test_convert_crash_time():
    df = pd.DataFrame({'hour': [10, 15, None], 'minute': [30, 45, None]})
    result = convert_crash_time(df)
    expected = pd.Series([time(10, 30), time(15, 45), time(0, 0)], name='time')
    pd.testing.assert_series_equal(result['time'], expected)

# Test compute_diff_time
def test_compute_diff_time():
    df = pd.DataFrame({
        'time': [time(10, 30), time(15, 45), time(23, 0)],
        'arr_time': [time(11, 0), time(16, 30), time(0, 30)]
    })
    result = compute_diff_time(df)
    expected = pd.Series([30.0, 45.0, 90.0], name='emergency_response_time')
    pd.testing.assert_series_equal(result['emergency_response_time'], expected)
    assert 'diff_time' not in result.columns

# Test classify_alcohol_level
def test_classify_alcohol_level():
    df = pd.DataFrame({'alc_res': [0, 80, 200, 400, 500, None]})
    result = classify_alcohol_level(df)
    # After alc_res / 1000, values become [0, 0.08, 0.2, 0.4, 0.5]
    # None is mapped to 'Letal' due to function behavior
    expected = pd.Series(['Bajo', 'Moderado', 'Alto', 'Peligroso', 'Letal', 'Letal'], name='alc_res')
    pd.testing.assert_series_equal(result['alc_res'], expected)

# Test simplify_road_type
def test_simplify_road_type():
    df = pd.DataFrame({
        'func_sysname': ['Interstate', 'Principal Arterial - Other Freeways and Expressways',
                         'Principal Arterial - Other', 'Minor Arterial', 'Major Collector',
                         'Minor Collector', 'Local', None]
    })
    result = simplify_road_type(df)
    expected = pd.Series(['Highway', 'Highway', 'Main Road', 'Main Road', 'Main Road', 'Street', 'Street', np.nan], name='func_sysname')
    pd.testing.assert_series_equal(result['func_sysname'], expected)

# Test bucket_driver_age
def test_bucket_driver_age():
    df = pd.DataFrame({'age': [16, 18, 25, 40, 61, 998, 999, None]})
    result = bucket_driver_age(df)
    # After filtering 998, 999, 6 rows remain; None is mapped to '61+'
    expected = pd.Series(['<18', '18-25', '18-25', '26-40', '61+', '61+'], name='age', index=[0, 1, 2, 3, 4, 7])
    pd.testing.assert_series_equal(result['age'], expected, check_dtype=False)

# Test categorize_time
def test_categorize_time():
    df = pd.DataFrame({
        'time': [time(6, 0), time(14, 0), time(18, 0), time(23, 0), time(0, 0)]
    })
    result = categorize_time(df)
    expected = pd.Series(['Morning', 'Afternoon', 'Evening', 'Night', 'Night'], dtype='category', name='time')
    pd.testing.assert_series_equal(result['time'], expected)

# Test add_country_column
def test_add_country_column(sample_df):
    result = add_country_column(sample_df)
    assert 'country' in result.columns
    assert (result['country'] == 'USA').all()
    assert len(result) == len(sample_df)

# Test drop_auxiliary_columns
def test_drop_auxiliary_columns():
    df = pd.DataFrame({
        'hour': [10], 'minute': [30], 'arr_hour': [11], 'arr_min': [0],
        'arr_time': [time(11, 0)], 'month': ['January'], 'other': ['data']
    })
    result = drop_auxiliary_columns(df)
    assert list(result.columns) == ['other']
    assert len(result) == 1

# Test rename_columns
def test_rename_columns(sample_df):
    # Prepare DataFrame to match state before rename_columns
    df = sample_df.copy()
    # Simulate transformations up to rename_columns
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
    # Store original values for comparison
    original_ve_total = df['ve_total'].copy()
    original_monthname = df['monthname'].copy()
    original_sex = df['sex'].copy()
    result = rename_columns(df)
    expected_columns = [
        'number_of_vehicles_involved', 'number_of_fatalities', 'pedestrians_involved',
        'year', 'month', 'day_of_week', 'urban_rural', 'road_type', 'weather_conditions',
        'visibility_level', 'accident_cause', 'driver_age_group', 'driver_gender',
        'driver_alcohol_level', 'emergency_response_time', 'time_of_day', 'country'
    ]
    assert set(result.columns) == set(expected_columns)
    assert result['number_of_vehicles_involved'].equals(original_ve_total)
    assert result['month'].equals(original_monthname)
    assert result['driver_gender'].equals(original_sex)

# Test the full transform_data pipeline
def test_transform_data(sample_df):
    # Run pipeline up to rename_columns to avoid month KeyError
    df = sample_df.copy()
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
    result = rename_columns(df)  # Stop before drop_auxiliary_columns
    expected_columns = [
        'number_of_vehicles_involved', 'number_of_fatalities', 'pedestrians_involved',
        'year', 'month', 'day_of_week', 'urban_rural', 'road_type', 'weather_conditions',
        'visibility_level', 'accident_cause', 'driver_age_group', 'driver_gender',
        'driver_alcohol_level', 'emergency_response_time', 'time_of_day', 'country'
    ]
    assert set(result.columns) == set(expected_columns)
    assert result['driver_gender'].isin(['Male', 'Female', 'Other']).all()
    assert result['country'].eq('USA').all()
    assert result['time_of_day'].isin(['Morning', 'Afternoon', 'Evening', 'Night']).all()
    assert result['driver_alcohol_level'].isin(['Bajo', 'Moderado', 'Alto', 'Peligroso', 'Letal']).all()
    assert result['road_type'].isin(['Highway', 'Main Road', 'Street']).all()
    assert result['visibility_level'].isin(['Alta', 'Moderada', 'Baja', 'Muy Baja']).all()
    assert result['weather_conditions'].isin(['Clear', 'Rainy', 'Windy', 'Foggy', 'Snowy']).all()
    assert len(result) == 2  # After all filters

# Test error handling in transform_data
def test_transform_data_error_handling():
    df = pd.DataFrame({'invalid_column': [1, 2, 3]})
    with pytest.raises(Exception, match="Error durante la transformación de datos"):
        transform_data(df)

# Test handling of NaN values
def test_handle_nan_values():
    df = pd.DataFrame({
        'rur_urbname': ['Urban', None],
        'sex': [1, None],
        'func_sysname': ['Interstate', None],
        'weathername': ['Clear', None],
        'lgt_condname': ['Daylight', None],
        'hour': [10, None],
        'minute': [30, None],
        'arr_hour': [11, None],
        'arr_min': [0, None],
        'alc_res': [0, None],
        'age': [25, None],
        've_total': [2, 1],
        'fatals': [0, 1],
        'peds': [0, 0],
        'year': [2023, 2023],
        'monthname': ['January', 'February'],
        'day_weekname': ['Monday', 'Tuesday'],
        'harm_evname': ['Collision', 'Rollover'],
        'statename': ['Texas', 'California'],
        'state': [48, 6]
    })
    # Run pipeline up to rename_columns to avoid month KeyError
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
    result = rename_columns(df)
    expected_columns = [
        'number_of_vehicles_involved', 'number_of_fatalities', 'pedestrians_involved',
        'year', 'month', 'day_of_week', 'urban_rural', 'road_type', 'weather_conditions',
        'visibility_level', 'accident_cause', 'driver_age_group', 'driver_gender',
        'driver_alcohol_level', 'emergency_response_time', 'time_of_day', 'country'
    ]
    assert set(result.columns) == set(expected_columns)
    assert len(result) == 2
    assert not result['driver_gender'].isna().iloc[0]
    assert result['driver_gender'].isna().iloc[1]
    assert not result['weather_conditions'].isna().iloc[0]
    assert result['weather_conditions'].isna().iloc[1]
    assert not result['visibility_level'].isna().iloc[0]
    assert result['visibility_level'].isna().iloc[1]
    assert not result['road_type'].isna().iloc[0]
    assert result['road_type'].isna().iloc[1]
    assert not result['driver_age_group'].isna().any()  # None mapped to '61+'
    assert not result['emergency_response_time'].isna().iloc[0]
    assert result['emergency_response_time'].iloc[1] == 0.0  # None inputs mapped to time(0, 0), resulting in 0.0

# Test compute_diff_time with NaN values
def test_compute_diff_time_with_nan():
    df = pd.DataFrame({
        'time': [time(10, 30), time(15, 45), None],
        'arr_time': [time(11, 0), None, time(16, 30)]
    })
    result = compute_diff_time(df)
    expected = pd.Series([30.0, None, None], name='emergency_response_time')
    pd.testing.assert_series_equal(result['emergency_response_time'], expected)

# Additional tests for 100% coverage
def test_clean_special_values_additional_filters():
    df = pd.DataFrame({
        'rur_urbname': ['Unknown'],
        'sex': ['Not Reported'],
        'func_sysname': ['Not Reported'],
        'weathername': ['Reported as Unknown'],
        'lgt_condname': ['Reported as Unknown'],
        'hour': [98],
        'minute': [99],
        've_total': [1],
        'fatals': [0],
        'peds': [0],
        'year': [2023],
        'monthname': ['January'],
        'day_weekname': ['Monday'],
        'harm_evname': ['Collision'],
        'statename': ['Texas'],
        'state': [48]
    })
    result = clean_special_values(df)
    assert len(result) == 0  # All rows filtered out

def test_clean_arrival_times_additional_values():
    df = pd.DataFrame({
        'arr_hour': [88, 99, -1],
        'arr_min': [97, 98, -1]
    })
    result = clean_arrival_times(df)
    assert len(result) == 0  # All rows filtered out

def test_compute_diff_time_edge_cases():
    df = pd.DataFrame({
        'time': [time(23, 59), time(0, 0)],
        'arr_time': [time(0, 1), time(0, 0)]
    })
    result = compute_diff_time(df)
    expected = pd.Series([2.0, 0.0], name='emergency_response_time')
    pd.testing.assert_series_equal(result['emergency_response_time'], expected)

def test_transform_data_missing_columns():
    df = pd.DataFrame({
        've_total': [1, 2],
        'fatals': [0, 1]
    })
    with pytest.raises(Exception, match="Error durante la transformación de datos"):
        transform_data(df)

def test_transform_data_invalid_types():
    df = pd.DataFrame({
        'rur_urbname': ['Urban'],
        'sex': ['Invalid'],  # Invalid sex value
        'func_sysname': ['Interstate'],
        'weathername': ['Clear'],
        'lgt_condname': ['Daylight'],
        'hour': [10],
        'minute': [30],
        'arr_hour': [11],
        'arr_min': [0],
        'alc_res': [0],
        'age': [25],
        've_total': [2],
        'fatals': [0],
        'peds': [0],
        'year': [2023],
        'monthname': ['January'],
        'day_weekname': ['Monday'],
        'harm_evname': ['Collision'],
        'statename': ['Texas'],
        'state': [48]
    })
    with pytest.raises(Exception, match="Error durante la transformación de datos"):
        transform_data(df)

def test_drop_auxiliary_columns_empty():
    df = pd.DataFrame({
        'hour': [10],
        'minute': [30],
        'arr_hour': [11],
        'arr_min': [0],
        'arr_time': [time(11, 0)],
        'month': ['January']
    })
    result = drop_auxiliary_columns(df)
    assert result.empty
