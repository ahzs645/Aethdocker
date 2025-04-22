import pandas as pd
import os
import numpy as np
from datetime import datetime, timezone
from app.utils.status_tracker import processing_status, processing_progress, processing_messages
import traceback

def standardize_column_names(df):
    """Standardize weather data column names"""
    column_patterns = {
        'temperature': ['temperature_c', 'temperature', 'temp_c', 'temp'],
        'humidity': ['relative_humidity_percent', 'humidity', 'rh', 'rel_humid'],
        'windSpeed': ['wind_speed_kmh', 'windspeed', 'wind_speed', 'wind'],
        'pressure': ['pressure_hpa', 'pressure', 'press', 'air_pressure']
    }
    
    # Keep track of how many times we've seen each standard name
    name_counts = {}
    renamed_columns = {}
    
    for col in df.columns:
        col_lower = col.lower()
        for standard_name, patterns in column_patterns.items():
            if any(pattern.lower() in col_lower for pattern in patterns):
                # If we've seen this standard name before, append a number
                if standard_name in name_counts:
                    name_counts[standard_name] += 1
                    actual_name = f"{standard_name}_{name_counts[standard_name]}"
                else:
                    name_counts[standard_name] = 1
                    actual_name = standard_name
                renamed_columns[col] = actual_name
                break
    
    print(f"[DEBUG] Column renaming map: {renamed_columns}")
    
    if renamed_columns:
        df = df.rename(columns=renamed_columns)
        print(f"[DEBUG] Columns after renaming: {df.columns.tolist()}")
    
    return df

def validate_weather_data(df):
    """Validate weather data columns and format"""
    print("[DEBUG] Starting weather data validation")
    print(f"[DEBUG] Input DataFrame shape: {df.shape}")
    print(f"[DEBUG] Input columns: {df.columns.tolist()}")
    print(f"[DEBUG] Input data types:\n{df.dtypes}")
    
    # Define base required columns
    base_required = {
        'temperature': False,
        'humidity': False,
        'windSpeed': False
    }
    found_columns = []
    
    # First standardize column names
    print("[DEBUG] Standardizing column names")
    df = standardize_column_names(df)
    print(f"[DEBUG] After standardization columns: {df.columns.tolist()}")
    
    # Convert columns to numeric and validate
    print("[DEBUG] Converting columns to numeric")
    for col in df.columns:
        # Check if this column is a required column or a numbered version of it
        base_col = col.split('_')[0] if '_' in col else col
        if base_col in base_required:
            print(f"[DEBUG] Converting {col} to numeric")
            print(f"[DEBUG] Column type before conversion: {df[col].dtype}")
            print(f"[DEBUG] Sample data before conversion:\n{df[col].head()}")
            
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"[DEBUG] Column type after conversion: {df[col].dtype}")
                print(f"[DEBUG] Sample data after conversion:\n{df[col].head()}")
                base_required[base_col] = True
                found_columns.append(col)
            except Exception as e:
                print(f"[DEBUG] Error converting {col} to numeric: {str(e)}")
                print(f"[DEBUG] Column data sample: {df[col].head()}")
                raise
    
    # Check if all required base columns were found
    missing_columns = [col for col, found in base_required.items() if not found]
    if missing_columns:
        print(f"[DEBUG] Missing base columns: {missing_columns}")
        raise ValueError(f"Missing required weather columns: {', '.join(missing_columns)}")
    
    print("[DEBUG] Weather data validation complete")
    print(f"[DEBUG] Final data shape: {df.shape}")
    print(f"[DEBUG] Final columns: {df.columns.tolist()}")
    print(f"[DEBUG] Final data types:\n{df.dtypes}")
    print(f"[DEBUG] Found columns: {found_columns}")
    
    return df

def ensure_tz_aware(timestamp_data):
    """Ensure a timestamp series is timezone aware"""
    try:
        print(f"[DEBUG] ensure_tz_aware input type: {type(timestamp_data)}")
        
        # Convert DataFrame to Series if needed
        if isinstance(timestamp_data, pd.DataFrame):
            print("[DEBUG] Converting DataFrame to Series")
            if len(timestamp_data.columns) != 1:
                raise ValueError("DataFrame must have exactly one column")
            timestamp_data = timestamp_data.iloc[:, 0]
        
        # Convert to Series if not already
        if not isinstance(timestamp_data, pd.Series):
            print("[DEBUG] Converting to Series")
            timestamp_data = pd.Series(timestamp_data)
        
        print(f"[DEBUG] Data type after Series conversion: {timestamp_data.dtype}")
        
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(timestamp_data):
            print("[DEBUG] Converting to datetime")
            timestamp_data = pd.to_datetime(timestamp_data)
            print(f"[DEBUG] Data type after datetime conversion: {timestamp_data.dtype}")
        
        # Add timezone if not present
        if timestamp_data.dt.tz is None:
            print("[DEBUG] Adding UTC timezone")
            return timestamp_data.dt.tz_localize('UTC')
        return timestamp_data
        
    except Exception as e:
        print(f"[DEBUG] Error in ensure_tz_aware: {str(e)}")
        print(f"[DEBUG] Input type: {type(timestamp_data)}")
        if hasattr(timestamp_data, 'head'):
            print(f"[DEBUG] Input sample: {timestamp_data.head()}")
        elif hasattr(timestamp_data, '__getitem__'):
            print(f"[DEBUG] Input sample: {timestamp_data[:5]}")
        else:
            print(f"[DEBUG] Input value: {timestamp_data}")
        raise

def process_weather_data(file_path, job_id=None):
    """Process weather data file with improved error handling and data validation"""
    try:
        if job_id:
            processing_messages[job_id] = "Processing weather data..."
            processing_status[job_id] = "Reading"
            processing_progress[job_id] = 10
        
        # Validate file
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Weather data file not found: {file_path}")
            
        if os.path.getsize(file_path) == 0:
            raise ValueError("Weather data file is empty")
            
        print(f"[DEBUG] Reading weather data from: {file_path}")
        print(f"[DEBUG] File size: {os.path.getsize(file_path)} bytes")
        
        # Read and validate CSV file
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                raise ValueError("No data found in weather file")
                
            print(f"[DEBUG] Original weather data shape: {df.shape}")
            print(f"[DEBUG] Original weather columns: {df.columns.tolist()}")
            print(f"[DEBUG] Data types of columns:\n{df.dtypes}")
            print(f"[DEBUG] First few rows of weather data:\n{df.head()}")
            print(f"[DEBUG] Any null values:\n{df.isnull().sum()}")
            
            # Basic data validation
            if df.shape[1] < 2:  # At least timestamp and one weather metric
                raise ValueError("Weather data file has insufficient columns")
                
        except pd.errors.EmptyDataError:
            raise ValueError("Weather data file is empty or malformed")
        except Exception as e:
            raise ValueError(f"Error reading weather data file: {str(e)}")
        
        # Standardize column names using weather-specific standardization
        print("[DEBUG] Starting column name standardization")
        print("[DEBUG] Original columns:")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head()}")
        
        df = standardize_column_names(df)
        
        print("[DEBUG] After standardize_column_names:")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Data types:\n{df.dtypes}")
        print(f"Sample data:\n{df.head()}")
        
        if job_id:
            processing_progress[job_id] = 30
            processing_messages[job_id] = "Validating timestamp data..."
        
        # Handle timestamp column
        timestamp_found = False
        timestamp_col = None
        
        # First, look for an existing timestamp column
        for col in df.columns:
            if 'timestamp' in col.lower():
                timestamp_col = col
                timestamp_found = True
                break
        
        # If no timestamp column found, look for date/time columns
        if not timestamp_found:
            date_col = None
            time_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if 'date' in col_lower:
                    date_col = col
                elif 'time' in col_lower:
                    time_col = col
            
            if date_col and time_col:
                print(f"[DEBUG] Creating timestamp from {date_col} and {time_col}")
                try:
                    print(f"[DEBUG] Creating timestamp from date and time columns")
                    print(f"Date column type: {type(df[date_col])}")
                    print(f"Date sample: {df[date_col].head()}")
                    print(f"Time column type: {type(df[time_col])}")
                    print(f"Time sample: {df[time_col].head()}")
                    
                    # Convert to string and combine
                    date_series = pd.Series(df[date_col]).astype(str)
                    time_series = pd.Series(df[time_col]).astype(str)
                    combined_series = date_series + ' ' + time_series
                    
                    print(f"[DEBUG] Combined datetime strings: {combined_series.head()}")
                    
                    # Convert to datetime
                    df['timestamp'] = pd.to_datetime(combined_series)
                    print(f"[DEBUG] Converted to datetime. Sample: {df['timestamp'].head()}")
                    timestamp_found = True
                except Exception as e:
                    print(f"[DEBUG] Error creating timestamp: {str(e)}")
                    print(f"[DEBUG] Date column info:\n{df[date_col].describe()}")
                    print(f"[DEBUG] Time column info:\n{df[time_col].describe()}")
            elif date_col:
                print(f"[DEBUG] Creating timestamp from {date_col}")
                try:
                    df['timestamp'] = pd.to_datetime(df[date_col])
                    timestamp_found = True
                except Exception as e:
                    print(f"[DEBUG] Error creating timestamp: {str(e)}")
        
        if not timestamp_found:
            raise ValueError("No valid timestamp column found in weather data")
        
        # Convert timestamp column
        if timestamp_found:
            try:
                if timestamp_col:
                    print(f"[DEBUG] Converting timestamp column: {timestamp_col}")
                    print(f"[DEBUG] Timestamp column info:")
                    print(f"Type: {type(df[timestamp_col])}")
                    print(f"Sample data: {df[timestamp_col].head()}")
                    
                    # First convert the column to a Series explicitly
                    timestamp_series = pd.Series(df[timestamp_col])
                    print(f"[DEBUG] Converted to Series. Type: {type(timestamp_series)}")
                    
                    # Convert to datetime
                    timestamp_series = pd.to_datetime(timestamp_series)
                    print(f"[DEBUG] Converted to datetime. Type: {type(timestamp_series)}")
                    print(f"[DEBUG] Sample after datetime conversion: {timestamp_series.head()}")
                    
                    # Make timezone aware
                    timestamp_series = ensure_tz_aware(timestamp_series)
                    print(f"[DEBUG] Made timezone aware. Type: {type(timestamp_series)}")
                    print(f"[DEBUG] Sample after timezone conversion: {timestamp_series.head()}")
                    
                    # Assign back to DataFrame
                    df['timestamp'] = timestamp_series
                    print(f"[DEBUG] Timestamp conversion successful. Final sample: {df['timestamp'].head()}")
            except Exception as e:
                print(f"[DEBUG] Error converting timestamp: {str(e)}")
                raise
        
        if job_id:
            processing_progress[job_id] = 50
            processing_messages[job_id] = "Validating and cleaning weather data..."
        
        # Validate and clean data
        df = validate_weather_data(df)
        print(f"[DEBUG] Final weather columns after validation: {df.columns.tolist()}")
        
        # Remove rows with invalid timestamps
        df = df.dropna(subset=['timestamp'])
        print(f"[DEBUG] Data shape after removing invalid timestamps: {df.shape}")
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['timestamp'], keep='first')
        print(f"[DEBUG] Final data shape: {df.shape}")
        
        if job_id:
            processing_progress[job_id] = 90
            processing_messages[job_id] = "Weather data processing complete"
            processing_status[job_id] = "Complete"
        
        return df
        
    except Exception as e:
        error_msg = f"Error processing weather data: {str(e)}"
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = error_msg
        print(error_msg)
        raise RuntimeError(error_msg)

def filter_weather_data_by_range(weather_df, aethalometer_df):
    """Filter weather data to match aethalometer data date range"""
    if 'timestamp' not in weather_df.columns or 'timestamp' not in aethalometer_df.columns:
        raise ValueError("Missing timestamp column in one or both datasets")

    # Get aethalometer date range
    aeth_min_date = aethalometer_df['timestamp'].min()
    aeth_max_date = aethalometer_df['timestamp'].max()
    
    print(f"[DEBUG] Aethalometer data range: {aeth_min_date} to {aeth_max_date}")
    print(f"[DEBUG] Weather data range before filtering: {weather_df['timestamp'].min()} to {weather_df['timestamp'].max()}")
    
    # Filter weather data to match aethalometer date range
    filtered_df = weather_df[
        (weather_df['timestamp'] >= aeth_min_date) &
        (weather_df['timestamp'] <= aeth_max_date)
    ]
    
    print(f"[DEBUG] Weather data range after filtering: {filtered_df['timestamp'].min() if not filtered_df.empty else 'No data'} to {filtered_df['timestamp'].max() if not filtered_df.empty else 'No data'}")
    print(f"[DEBUG] Filtered weather data shape: {filtered_df.shape}")
    
    if filtered_df.empty:
        raise ValueError("No weather data available for the aethalometer data time period")
        
    return filtered_df

def synchronize_data(aethalometer_df, weather_df, job_id=None):
    """Synchronize aethalometer and weather data by timestamp with improved handling"""
    try:
        # Input validation
        if not isinstance(aethalometer_df, pd.DataFrame) or not isinstance(weather_df, pd.DataFrame):
            raise ValueError("Both inputs must be pandas DataFrames")
            
        if aethalometer_df.empty or weather_df.empty:
            raise ValueError("Empty dataframe provided for synchronization")
        
        if job_id:
            processing_messages[job_id] = "Synchronizing aethalometer and weather data..."
            processing_progress[job_id] = 60
            
        # Filter weather data to match aethalometer date range
        try:
            weather_df = filter_weather_data_by_range(weather_df, aethalometer_df)
        except ValueError as e:
            raise ValueError(f"Date range mismatch: {str(e)}")
        
        print("[DEBUG] Starting data synchronization")
        print(f"[DEBUG] Aethalometer data shape: {aethalometer_df.shape}")
        print(f"[DEBUG] Weather data shape: {weather_df.shape}")
        print(f"[DEBUG] Aethalometer columns: {aethalometer_df.columns.tolist()}")
        print(f"[DEBUG] Weather columns: {weather_df.columns.tolist()}")
        print(f"[DEBUG] Aethalometer data types:\n{aethalometer_df.dtypes}")
        print(f"[DEBUG] Weather data types:\n{weather_df.dtypes}")
        
        # Ensure timestamp columns exist
        if 'timestamp' not in aethalometer_df.columns or 'timestamp' not in weather_df.columns:
            raise ValueError("Missing timestamp column in one or both datasets")
        
        # Convert timestamps to datetime if they're not already
        try:
            print("[DEBUG] Converting timestamps to datetime")
            print(f"[DEBUG] Aethalometer timestamp sample: {aethalometer_df['timestamp'].head()}")
            print(f"[DEBUG] Weather timestamp sample: {weather_df['timestamp'].head()}")
            
            aethalometer_df['timestamp'] = pd.to_datetime(aethalometer_df['timestamp'])
            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
            
            # Ensure timestamps are timezone aware
            print("[DEBUG] Making timestamps timezone aware")
            aethalometer_df['timestamp'] = ensure_tz_aware(aethalometer_df['timestamp'])
            weather_df['timestamp'] = ensure_tz_aware(weather_df['timestamp'])
            
            print("[DEBUG] Timestamps converted and timezone-aware")
            print(f"[DEBUG] Aethalometer timestamp sample after conversion: {aethalometer_df['timestamp'].head()}")
            print(f"[DEBUG] Weather timestamp sample after conversion: {weather_df['timestamp'].head()}")
        except Exception as e:
            print(f"[DEBUG] Error during timestamp conversion: {str(e)}")
            print(f"[DEBUG] Aethalometer timestamp type: {type(aethalometer_df['timestamp'])}")
            print(f"[DEBUG] Weather timestamp type: {type(weather_df['timestamp'])}")
            print(f"[DEBUG] Full traceback: {traceback.format_exc()}")
            raise
        
        # Handle multiple windSpeed columns if they exist
        wind_speed_cols = [col for col in weather_df.columns if col.startswith('windSpeed')]
        if len(wind_speed_cols) > 1:
            print(f"[DEBUG] Found multiple wind speed columns: {wind_speed_cols}")
            # Use the first non-null value from any windSpeed column
            weather_df['windSpeed'] = weather_df[wind_speed_cols].bfill(axis=1).iloc[:, 0]
            # Drop the numbered columns
            weather_df = weather_df.drop(columns=[col for col in wind_speed_cols if col != 'windSpeed'])
            print(f"[DEBUG] Combined wind speed columns into single column")
            print(f"[DEBUG] Updated weather columns: {weather_df.columns.tolist()}")
        
        # Convert both to UTC if they have different timezones
        if (aethalometer_df['timestamp'].dt.tz and
            weather_df['timestamp'].dt.tz and
            aethalometer_df['timestamp'].dt.tz != weather_df['timestamp'].dt.tz):
            print("[DEBUG] Converting timestamps to UTC")
            aethalometer_df['timestamp'] = aethalometer_df['timestamp'].dt.tz_convert('UTC')
            weather_df['timestamp'] = weather_df['timestamp'].dt.tz_convert('UTC')
        
        # Set timestamp as index for both dataframes
        aethalometer_df = aethalometer_df.set_index('timestamp')
        weather_df = weather_df.set_index('timestamp')
        
        print("[DEBUG] Calculating resampling frequency")
        # Calculate resampling frequency
        time_diffs = pd.Series(aethalometer_df.index[1:] - aethalometer_df.index[:-1])
        if len(time_diffs) > 0:
            min_time_diff = time_diffs.min()
            resample_freq = f"{max(1, int(min_time_diff.total_seconds()))}S"
            print(f"[DEBUG] Using resampling frequency: {resample_freq}")
        else:
            resample_freq = '1min'  # Default if can't determine
            print("[DEBUG] Using default resampling frequency: 1min")
        
        print("[DEBUG] Resampling and interpolating weather data")
        # For hourly weather data, use appropriate interpolation
        weather_resampled = weather_df.copy()
        
        # Fill missing values using appropriate methods for each type
        for col in weather_resampled.columns:
            if col.startswith(('temperature', 'humidity', 'pressure')):
                # Use cubic interpolation for continuous variables
                weather_resampled[col] = weather_resampled[col].interpolate(method='cubic')
            elif col.startswith('wind'):
                # Use linear interpolation for wind measurements
                weather_resampled[col] = weather_resampled[col].interpolate(method='linear')
        
        print(f"[DEBUG] Weather data shape after resampling: {weather_resampled.shape}")
        
        # Reset index for merge
        aethalometer_reset = aethalometer_df.reset_index()
        weather_reset = weather_resampled.reset_index()
        
        # Ensure timestamps are sorted
        aethalometer_reset = aethalometer_reset.sort_values('timestamp')
        weather_reset = weather_reset.sort_values('timestamp')
        
        print("[DEBUG] Merging dataframes")
        print(f"[DEBUG] Aethalometer columns before merge: {aethalometer_reset.columns.tolist()}")
        print(f"[DEBUG] Weather columns before merge: {weather_reset.columns.tolist()}")
        
        # Identify overlapping columns (except timestamp)
        overlapping_cols = [col for col in aethalometer_reset.columns if col in weather_reset.columns and col != 'timestamp']
        if overlapping_cols:
            print(f"[DEBUG] Found overlapping columns: {overlapping_cols}")
            # Rename overlapping columns in weather_reset
            weather_reset = weather_reset.rename(columns={col: f"weather_{col}" for col in overlapping_cols})
            print(f"[DEBUG] Weather columns after renaming: {weather_reset.columns.tolist()}")
        
        # Merge dataframes with improved handling
        combined = pd.merge_asof(
            aethalometer_reset,
            weather_reset,
            on='timestamp',
            direction='nearest',
            tolerance=pd.Timedelta('1h')  # Match within 1 hour for hourly weather data
        )

        # Add processedBC column if it exists in original data
        if 'processedBC' in aethalometer_df.columns:
            combined['processedBC'] = aethalometer_df['processedBC']
        
        print(f"[DEBUG] Combined columns after merge: {combined.columns.tolist()}")
        
        # Ensure 'processedBC' column is retained
        print("[DEBUG] Checking for processedBC column")
        print(f"[DEBUG] Columns in aethalometer_reset: {aethalometer_reset.columns.tolist()}")
        print(f"[DEBUG] processedBC in combined before: {combined.columns.tolist()}")
        
        if 'processedBC' not in combined.columns and 'processedBC' in aethalometer_reset.columns:
            print("[DEBUG] Restoring processedBC column")
            combined['processedBC'] = aethalometer_reset['processedBC']
            print(f"[DEBUG] processedBC stats after restoration: {combined['processedBC'].describe()}")
        
        print(f"[DEBUG] Combined data shape: {combined.shape}")
        print(f"[DEBUG] Combined columns: {combined.columns.tolist()}")
        print("[DEBUG] Sample of combined data:")
        print(combined.head())
        
        if job_id:
            processing_progress[job_id] = 100
            processing_messages[job_id] = "Data synchronization complete"
        
        return combined
        
    except Exception as e:
        error_msg = f"Error synchronizing data: {str(e)}"
        print(error_msg)
        print(f"[DEBUG] Full traceback: {traceback.format_exc()}")
        if job_id:
            processing_messages[job_id] = error_msg
        raise RuntimeError(error_msg)
