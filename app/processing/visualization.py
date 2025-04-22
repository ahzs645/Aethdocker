import os
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import gaussian_kde
import traceback
from typing import Optional, Dict, Any, List
from app.utils.status_tracker import processing_status, processing_progress, processing_messages

def downsample_data(df: pd.DataFrame, max_points: int = 10000) -> pd.DataFrame:
    """Downsample data intelligently to preserve important features"""
    if len(df) <= max_points:
        return df
    
    # Calculate optimal window size for downsampling
    window_size = len(df) // max_points
    
    # Use rolling window to capture local extremes
    df_downsampled = pd.DataFrame()
    
    # Keep timestamp column as is
    if 'timestamp' in df.columns:
        df_downsampled['timestamp'] = df['timestamp'].iloc[::window_size]
    
    # Process numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        # Calculate mean, min, and max for each window
        means = df[col].rolling(window=window_size, center=True).mean()[::window_size]
        mins = df[col].rolling(window=window_size, center=True).min()[::window_size]
        maxs = df[col].rolling(window=window_size, center=True).max()[::window_size]
        
        # Combine them intelligently
        df_downsampled[col] = means
        # Add significant extremes
        significant_diff = (maxs - mins) > means.std()
        df_downsampled.loc[significant_diff, col] = maxs[significant_diff]
    
    return df_downsampled

def calculate_correlations(data: pd.DataFrame, x: str, y: str) -> Dict[str, float]:
    """Calculate correlations with proper error handling"""
    try:
        valid_data = data[[x, y]].dropna()
        if len(valid_data) < 5:
            return None
        
        pearson_r, pearson_p = stats.pearsonr(valid_data[x], valid_data[y])
        spearman_r, spearman_p = stats.spearmanr(valid_data[x], valid_data[y])
        
        return {
            'pearson_r': pearson_r,
            'pearson_p': pearson_p,
            'spearman_r': spearman_r,
            'spearman_p': spearman_p
        }
    except Exception:
        return None

def identify_weather_columns(df: pd.DataFrame) -> List[str]:
    """Identify weather-related columns in the dataframe"""
    weather_patterns = {
        'temperature': ['temperature_c', 'temperature', 'temp_c', 'temp'],
        'humidity': ['relative_humidity_percent', 'humidity', 'rh', 'rel_humid'],
        'windSpeed': ['wind_speed_kmh', 'windspeed', 'wind_speed', 'wind'],
        'pressure': ['pressure_hpa', 'pressure', 'press', 'air_pressure']
    }
    
    weather_cols = []
    
    for col in df.columns:
        col_lower = col.lower()
        for category, patterns in weather_patterns.items():
            if any(pattern.lower() in col_lower for pattern in patterns):
                if pd.api.types.is_numeric_dtype(df[col]):
                    weather_cols.append(col)
                break
    
    return weather_cols

def prepare_visualization_data(original_df: pd.DataFrame, processed_df: pd.DataFrame,
                           combined_df: Optional[pd.DataFrame], wavelength: str,
                           job_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Prepare data for visualization with improved memory efficiency and error handling.
    
    Args:
        original_df: Original dataframe (not used, kept for backward compatibility)
        processed_df: Processed dataframe containing timestamp, rawBC, and processedBC columns
        combined_df: Optional combined dataframe with weather data
        wavelength: The wavelength being processed
        job_id: Optional job ID for status tracking
    
    Returns:
        Dictionary containing visualization data for time series, comparison, and weather correlation
    """
    """Prepare data for visualization with improved memory efficiency and error handling"""
    try:
        if job_id:
            processing_status[job_id] = "Preparing visualization data"
            processing_messages[job_id] = "Processing data for visualization..."
            processing_progress[job_id] = 85

        # Validate input data
        if processed_df is None or processed_df.empty:
            raise ValueError("Processed data is empty or None")
            
        if not isinstance(processed_df, pd.DataFrame):
            raise TypeError("Processed data must be a pandas DataFrame")
            
        # Ensure timestamp column exists and is datetime
        if 'timestamp' not in processed_df.columns:
            raise ValueError("Timestamp column missing from processed data")
            
        processed_df['timestamp'] = pd.to_datetime(processed_df['timestamp'])
        
        # Downsample data for visualization
        viz_df = downsample_data(processed_df, max_points=10000)
        
        # Initialize result dictionary with metadata
        result = {
            'time_series_data': None,
            'comparison_data': None,
            'weather_correlation_data': None,
            'metadata': {
                'original_rows': len(processed_df),
                'downsampled_rows': len(viz_df),
                'timestamp_range': {
                    'start': processed_df['timestamp'].min().isoformat(),
                    'end': processed_df['timestamp'].max().isoformat()
                }
            }
        }
        
        try:
            # Validate required columns and data types
            required_columns = {'timestamp', 'rawBC', 'processedBC'}
            if not all(col in viz_df.columns for col in required_columns):
                missing_cols = required_columns - set(viz_df.columns)
                raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

            # Ensure data is not empty
            if viz_df.empty:
                raise ValueError("Visualization DataFrame is empty")

            # Check for null values in critical columns
            null_counts = viz_df[list(required_columns)].isnull().sum()
            if null_counts.any():
                print(f"[WARNING] Null values found in columns: {null_counts[null_counts > 0].to_dict()}")

            # Time Series Data
            if job_id:
                processing_messages[job_id] = "Preparing time series data..."
            
            time_series_data = viz_df[['timestamp', 'rawBC', 'processedBC']].dropna()
            if time_series_data.empty:
                raise ValueError("No valid time series data available after removing null values")
            
            result['time_series_data'] = time_series_data.to_dict('records')
            
            # BC Comparison Data
            if job_id:
                processing_messages[job_id] = "Preparing comparison data..."
                processing_progress[job_id] = 90
            
            try:
                # Validate BC data columns
                if not {'rawBC', 'processedBC'}.issubset(viz_df.columns):
                    raise ValueError("Missing required BC columns for comparison")
                
                # Check data ranges
                if (viz_df['rawBC'] < 0).any() or (viz_df['processedBC'] < 0).any():
                    print("[WARNING] Negative BC values found in data")
                
                valid_bc_data = viz_df[['rawBC', 'processedBC']].dropna()
                if valid_bc_data.empty:
                    raise ValueError("No valid BC data available for comparison after removing null values")
                
                if len(valid_bc_data) < 5:
                    raise ValueError("Insufficient data points for meaningful comparison (minimum 5 required)")
                
                result['comparison_data'] = valid_bc_data.to_dict('records')
                
                # Add correlation statistics
                corr_stats = calculate_correlations(valid_bc_data, 'rawBC', 'processedBC')
                if corr_stats:
                    result['comparison_stats'] = corr_stats
                    # Add data quality metrics
                    result['comparison_stats']['data_points'] = len(valid_bc_data)
                    result['comparison_stats']['null_percentage'] = (
                        (1 - len(valid_bc_data) / len(viz_df)) * 100
                    )
                else:
                    print("[WARNING] Could not calculate correlation statistics")
            except Exception as e:
                print(f"[ERROR] BC comparison processing failed: {str(e)}")
                if job_id:
                    processing_messages[job_id] = f"Warning: BC comparison processing failed: {str(e)}"
            
            # Weather correlation data
            if combined_df is not None and not combined_df.empty:
                if job_id:
                    processing_messages[job_id] = "Preparing weather correlation data..."
                    processing_progress[job_id] = 95

                try:
                    # Validate timestamp column in combined data
                    if 'timestamp' not in combined_df.columns:
                        raise ValueError("Combined data missing timestamp column")

                    # Add processedBC to combined_df if not present
                    if 'processedBC' not in combined_df.columns:
                        if 'processedBC' not in processed_df.columns:
                            raise ValueError("ProcessedBC column not found in either dataset")
                        combined_df = combined_df.copy()
                        combined_df['processedBC'] = processed_df['processedBC']

                    # Ensure timestamps are properly aligned
                    combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                    if combined_df['timestamp'].dt.tz is None:
                        combined_df['timestamp'] = combined_df['timestamp'].dt.tz_localize('UTC')

                    # Get aethalometer data time range
                    aeth_start = processed_df['timestamp'].min()
                    aeth_end = processed_df['timestamp'].max()
                    
                    print(f"[INFO] Aethalometer data time range: {aeth_start} to {aeth_end}")
                    print(f"[INFO] Weather data time range before filtering: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
                    
                    # Filter weather data to match aethalometer time range
                    combined_df = combined_df[
                        (combined_df['timestamp'] >= aeth_start) &
                        (combined_df['timestamp'] <= aeth_end)
                    ].copy()
                    
                    if combined_df.empty:
                        raise ValueError("No overlapping time period between weather and aethalometer data")
                    
                    print(f"[INFO] Weather data time range after filtering: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
                    print(f"[INFO] Number of weather data points after filtering: {len(combined_df)}")
                    
                    # Update metadata with time overlap information
                    result['metadata']['weather_data'] = {
                        'original_timespan': {
                            'start': combined_df['timestamp'].min().isoformat(),
                            'end': combined_df['timestamp'].max().isoformat()
                        },
                        'overlap_timespan': {
                            'start': aeth_start.isoformat(),
                            'end': aeth_end.isoformat()
                        },
                        'points_in_overlap': len(combined_df)
                    }

                    # Downsample combined data
                    combined_viz_df = downsample_data(combined_df)
                    
                    # Identify weather columns
                    weather_cols = identify_weather_columns(combined_viz_df)
                    if not weather_cols:
                        raise ValueError("No valid weather columns identified")

                    if 'processedBC' not in combined_viz_df.columns:
                        raise ValueError("ProcessedBC column missing from combined visualization data")

                    weather_data = {}
                    valid_correlations = False

                    for weather_col in weather_cols:
                        valid_data = combined_viz_df[[weather_col, 'processedBC']].dropna()
                        if len(valid_data) > 0:
                            correlation = calculate_correlations(valid_data, weather_col, 'processedBC')
                            if correlation is not None:
                                weather_data[weather_col] = {
                                    'data': valid_data.to_dict('records'),
                                    'correlation': correlation
                                }
                                valid_correlations = True

                    if valid_correlations:
                        result['weather_correlation_data'] = weather_data
                    else:
                        print("[WARNING] No valid weather correlations found")
                        if job_id:
                            processing_messages[job_id] = "Warning: No valid weather correlations found"

                except Exception as e:
                    print(f"[ERROR] Weather correlation processing failed: {str(e)}")
                    if job_id:
                        processing_messages[job_id] = f"Warning: Weather correlation processing failed: {str(e)}"
        
        except Exception as e:
            print(f"Error in visualization data preparation: {e}")
            print(traceback.format_exc())
            if job_id:
                processing_messages[job_id] = f"Warning: Some visualization data could not be prepared: {str(e)}"
        
        if job_id:
            processing_messages[job_id] = "Visualization data prepared successfully"
            processing_progress[job_id] = 100
        
        return result
        
    except Exception as e:
        error_msg = f"Error preparing visualization data: {str(e)}"
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = error_msg
        print(error_msg)
        print(traceback.format_exc())
        return {
            'time_series_data': None,
            'comparison_data': None,
            'weather_correlation_data': None
        }
