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
    """Prepare data for visualization with improved memory efficiency and error handling"""
    try:
        if job_id:
            processing_status[job_id] = "Preparing visualization data"
            processing_messages[job_id] = "Processing data for visualization..."
            processing_progress[job_id] = 85
        
        # Downsample data for visualization
        viz_df = downsample_data(processed_df, max_points=10000)
        
        # Initialize result dictionary
        result = {
            'time_series_data': None,
            'comparison_data': None,
            'weather_correlation_data': None
        }
        
        try:
            # Time Series Data
            if job_id:
                processing_messages[job_id] = "Preparing time series data..."
            
            result['time_series_data'] = viz_df[['timestamp', 'rawBC', 'processedBC']].to_dict('records')
            
            # BC Comparison Data
            if job_id:
                processing_messages[job_id] = "Preparing comparison data..."
                processing_progress[job_id] = 90
            
            valid_bc_data = viz_df[['rawBC', 'processedBC']].dropna()
            if len(valid_bc_data) > 0:
                result['comparison_data'] = valid_bc_data.to_dict('records')
                
                # Add correlation statistics
                corr_stats = calculate_correlations(valid_bc_data, 'rawBC', 'processedBC')
                if corr_stats:
                    result['comparison_stats'] = corr_stats
            
            # Weather correlation data
            if combined_df is not None and not combined_df.empty:
                if job_id:
                    processing_messages[job_id] = "Preparing weather correlation data..."
                    processing_progress[job_id] = 95
                
                # Add processedBC to combined_df if not present
                if 'processedBC' not in combined_df.columns and 'processedBC' in processed_df.columns:
                    combined_df = combined_df.copy()
                    combined_df['processedBC'] = processed_df['processedBC']
                
                # Downsample combined data
                combined_viz_df = downsample_data(combined_df)
                
                # Identify weather columns
                weather_cols = identify_weather_columns(combined_viz_df)
                
                if weather_cols and 'processedBC' in combined_viz_df.columns:
                    weather_data = {}
                    
                    for weather_col in weather_cols:
                        valid_data = combined_viz_df[[weather_col, 'processedBC']].dropna()
                        if len(valid_data) > 0:
                            weather_data[weather_col] = {
                                'data': valid_data.to_dict('records'),
                                'correlation': calculate_correlations(valid_data, weather_col, 'processedBC')
                            }
                    
                    result['weather_correlation_data'] = weather_data
        
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
