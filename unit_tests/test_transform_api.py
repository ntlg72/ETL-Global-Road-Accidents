import pandas as pd
import pytest
import numpy as np
from datetime import time
from source.transform.transform_api import (
    clean_special_values, map_visibility, map_weather, map_sex, clean_arrival_times,
    convert_arrival_time, convert_crash_time, compute_diff_time, classify_alcohol_level,
    simplify_road_type, bucket_driver_age, categorize_time, add_country_column,
    rename_columns, drop_auxiliary_columns, transform_data
)

# Fixture to create a sample DataFrame for testing
@pytest.fixture
def sample_df():
    data = {
        'rur_urbname': ['Urban', 'Not Reported', 'Rural'],
        'sex': [1, 'Unknown', 2],
        'func_sysname': ['Interstate', 'Unknown', 'Local'],
        'weathername': ['Clear', 'Unknown', 'Rain'],
        'lgt_condname': ['Daylight', 'Unknown', 'Dark - Lighted'],
        'hour': [10, 98, 15],
        'minute': [30, 99, 45],
        'arr_hour': [10, 88, 16],
        'arr_min': [40, 98, 0],
        'alc_res': [0, 80, 200],
        'age': [25, 999, 65],
        've_total': [2, 1, 3],
        'fatals': [0, 1, 0],
        'peds': [0, 0, 1],
        'year': [2023, 2023, 2023],
        'monthname': ['January', 'February', 'March'],
        'day_weekname': ['Monday', 'Tuesday', 'Wednesday'],
        'harm_evname': ['Collision', 'Rollover', 'Pedestrian'],
        'statename': ['Texas', 'California', 'Florida'],
        'state': [48, 6, 12]
    }
    return pd.DataFrame(data)

# Test clean_special_values
def test_clean_special_values(sample_df):
    result = clean_special_values(sample_df)
    assert 'Not Reported' not in result['rur_urbname'].values
    assert 'Unknown' not in result['sex'].values
    assert 'statename' not in result.columns
    assert 'state' not in result.columns
    assert result is not sample_df

# Test map_visibility
def test_map_visibility():
    df = pd.DataFrame({'lgt_condname': ['Daylight', 'Dawn', 'Dark - Not Lighted', None]})
    result = map_visibility(df)
    expected = ['Alta', 'Moderada', 'Muy Baja', np.nan]
    pd.testing.assert_series_equal(result['lgt_condname'], pd.Series(expected, name='lgt_condname'))

# Test map_weather
def test_map_weather():
    df = pd.DataFrame({'weathername': ['Clear', 'Rain', 'Cloudy', None]})
    result = map_weather(df)
    expected = ['Clear', 'Rainy', 'Windy', np.nan]
    pd.testing.assert_series_equal(result['weathername'], pd.Series(expected, name='weathername'))

# Test map_sex
def test_map_sex():
    df = pd.DataFrame({'sex': [1, 2, 3, None]})
    result = map_sex(df)
    expected = ['Male', 'Female', 'Other', np.nan]
    pd.testing.assert_series_equal(result['sex'], pd.Series(expected, name='sex'))

# Test clean_arrival_times
def test_clean_arrival_times():
    df = pd.DataFrame({
        'arr_hour': [10, 88, 16],
        'arr_min': [40, 98, 0]
    })
    result = clean_arrival_times(df)
    assert len(result) == 2
    assert 88 not in result['arr_hour'].values
    assert 98 not in result['arr_min'].values

# Test convert_arrival_time
def test_convert_arrival_time():
    df = pd.DataFrame({'arr_hour': [10, 15], 'arr_min': [30, 45]})
    result = convert_arrival_time(df)
    expected = [time(10, 30), time(15, 45)]
    pd.testing.assert_series_equal(result['arr_time'], pd.Series(expected, name='arr_time'))

# Test convert_crash_time
def test_convert_crash_time():
    df = pd.DataFrame({'hour': [10, 15], 'minute': [30, 45]})
    result = convert_crash_time(df)
    expected = [time(10, 30), time(15, 45)]
    pd.testing.assert_series_equal(result['time'], pd.Series(expected, name='time'))

# Test compute_diff_time
def test_compute_diff_time():
    df = pd.DataFrame({
        'time': [time(10, 30), time(15, 45)],
        'arr_time': [time(11, 0), time(16, 30)]
    })
    result = compute_diff_time(df)
    expected = ['00:30:00', '00:45:00']
    pd.testing.assert_series_equal(result['diff_time'], pd.Series(expected, name='diff_time'))

# Test classify_alcohol_level
def test_classify_alcohol_level():
    df = pd.DataFrame({'alc_res': [0, 80, 200, 400, 500]})
    result = classify_alcohol_level(df)
    expected = ['Bajo', 'Moderado', 'Alto', 'Peligroso', 'Letal']
    pd.testing.assert_series_equal(result['alc_res'], pd.Series(expected, name='alc_res'))

# Test simplify_road_type
def test_simplify_road_type():
    df = pd.DataFrame({'func_sysname': ['Interstate', 'Local', 'Minor Arterial', None]})
    result = simplify_road_type(df)
    expected = ['Highway', 'Street', 'Main Road', np.nan]
    pd.testing.assert_series_equal(result['func_sysname'], pd.Series(expected, name='func_sysname'))

# Test bucket_driver_age
def test_bucket_driver_age():
    df = pd.DataFrame({'age': [16, 25, 40, 65]})
    result = bucket_driver_age(df)
    expected = ['<18', '18-25', '26-40', '61+']
    pd.testing.assert_series_equal(result['age'], pd.Series(expected, name='age'))

# Test categorize_time
def test_categorize_time():
    df = pd.DataFrame({'time': [time(6, 0), time(14, 0), time(18, 0), time(23, 0)]})
    result = categorize_time(df)
    expected = ['Morning', 'Afternoon', 'Evening', 'Night']
    pd.testing.assert_series_equal(result['time'], pd.Series(expected, dtype='category', name='time'))

# Test add_country_column
def test_add_country_column(sample_df):
    result = add_country_column(sample_df)
    assert 'country' in result.columns
    assert (result['country'] == 'USA').all()

# Test rename_columns
def test_rename_columns(sample_df):
    result = rename_columns(sample_df)
    expected_columns = [
        'number_of_vehicles_involved', 'number_of_fatalities', 'pedestrians_involved',
        'year', 'month', 'day_of_week', 'urban_rural', 'road_type', 'weather_conditions',
        'visibility_level', 'accident_cause', 'driver_age_group', 'driver_gender',
        'driver_alcohol_level', 'emergency_response_time', 'time_of_day',
        'rur_urbname', 'sex', 'func_sysname', 'weathername', 'lgt_condname', 'hour',
        'minute', 'arr_hour', 'arr_min', 'alc_res', 'age', 'statename', 'state'
    ]
    assert all(col in result.columns for col in expected_columns if col in sample_df.columns)

# Test drop_auxiliary_columns
def test_drop_auxiliary_columns():
    df = pd.DataFrame({
        'hour': [10], 'minute': [30], 'arr_hour': [11], 'arr_min': [0],
        'arr_time': [time(11, 0)], 'other': ['data']
    })
    result = drop_auxiliary_columns(df)
    assert list(result.columns) == ['other']

# Test the full transform_data pipeline
def test_transform_data(sample_df):
    result = transform_data(sample_df)
    expected_columns = [
        'number_of_vehicles_involved', 'number_of_fatalities', 'pedestrians_involved',
        'year', 'month', 'day_of_week', 'urban_rural', 'road_type', 'weather_conditions',
        'visibility_level', 'accident_cause', 'driver_age_group', 'driver_gender',
        'driver_alcohol_level', 'emergency_response_time', 'time_of_day', 'country'
    ]
    assert set(result.columns) == set(expected_columns)
    assert result['driver_gender'].isin(['Male', 'Female', 'Other', None]).all()
    assert result['country'].eq('USA').all()
    assert result['time_of_day'].isin(['Morning', 'Afternoon', 'Evening', 'Night']).all()

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
    result = transform_data(df)
    assert not result.empty
    assert pd.isna(result['driver_gender'].iloc[1])
    assert pd.isna(result['weather_conditions'].iloc[1])
    assert pd.isna(result['visibility_level'].iloc[1])
    assert pd.isna(result['road_type'].iloc[1])

