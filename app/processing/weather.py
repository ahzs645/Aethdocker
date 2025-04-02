import pandas as pd
import os
from app.utils.status_tracker import processing_status, processing_progress, processing_messages

def process_weather_data(file_path, job_id=None):
    """Process weather data file"""
    try:
        if job_id:
            processing_messages[job_id] = "Processing weather data..."
        
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Standardize column names
        from app.processing.aethalometer import map_field_names
        df = map_field_names(df)
        
        # Ensure timestamp column exists
        if 'timestamp' not in df.columns:
            # Try to find timestamp column
            for col in df.columns:
                if 'time' in col.lower() or 'date' in col.lower():
                    df['timestamp'] = pd.to_datetime(df[col])
                    break
            
            if 'timestamp' not in df.columns:
                # Create a timestamp column with row index as placeholder
                df['timestamp'] = pd.date_range(start='2023-01-01', periods=len(df), freq='1hour')
        
        return df
    except Exception as e:
        if job_id:
            processing_messages[job_id] = f"Warning: Error processing weather data: {str(e)}"
        print(f"Error processing weather data: {e}")
        return pd.DataFrame()

def synchronize_data(aethalometer_df, weather_df, job_id=None):
    """Synchronize aethalometer and weather data by timestamp"""
    if aethalometer_df.empty or weather_df.empty:
        return pd.DataFrame()
    
    if job_id:
        processing_messages[job_id] = "Synchronizing aethalometer and weather data..."
    
    # Ensure timestamp columns are datetime
    aethalometer_df['timestamp'] = pd.to_datetime(aethalometer_df['timestamp'])
    weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
    
    # Set timestamp as index for both dataframes
    aethalometer_df = aethalometer_df.set_index('timestamp')
    weather_df = weather_df.set_index('timestamp')
    
    # Resample weather data to match aethalometer data frequency
    weather_resampled = weather_df.resample('1min').ffill()
    
    # Merge dataframes
    combined = pd.merge_asof(
        aethalometer_df.reset_index().sort_values('timestamp'),
        weather_resampled.reset_index().sort_values('timestamp'),
        on='timestamp',
        direction='nearest'
    )
    
    return combined
