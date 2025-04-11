import os
import plotly
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from scipy import stats
import traceback
from typing import Optional, Dict, Any, List
from app.utils.status_tracker import processing_status, processing_progress, processing_messages

def downsample_data(df: pd.DataFrame, max_points: int = 10000) -> pd.DataFrame:
    """Downsample data intelligently to preserve important features"""
    print(f"[DEBUG] Downsampling data. Input shape: {df.shape}")
    print(f"[DEBUG] Input columns: {df.columns.tolist()}")
    print(f"[DEBUG] processedBC stats before downsampling: {df['processedBC'].describe() if 'processedBC' in df.columns else 'Not found'}")
    
    if len(df) <= max_points:
        return df
    
    # Calculate optimal window size for downsampling
    window_size = len(df) // max_points
    print(f"[DEBUG] Calculated window size: {window_size}")
    
    # Use rolling window to capture local extremes
    df_downsampled = pd.DataFrame()
    
    # Keep timestamp column as is
    if 'timestamp' in df.columns:
        df_downsampled['timestamp'] = df['timestamp'].iloc[::window_size]
    
    # Process numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    print(f"[DEBUG] Processing numeric columns: {numeric_cols.tolist()}")
    
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
        
        if col == 'processedBC':
            print(f"[DEBUG] processedBC stats after downsampling: {df_downsampled['processedBC'].describe()}")
    
    print(f"[DEBUG] Output shape: {df_downsampled.shape}")
    print(f"[DEBUG] Output columns: {df_downsampled.columns.tolist()}")
    return df_downsampled

def create_time_series_plot(df: pd.DataFrame, x: str, y: List[str], title: str, 
                          y_label: str) -> go.Figure:
    """Create a time series plot with multiple traces"""
    fig = go.Figure()
    
    for y_col in y:
        # Handle missing or invalid data
        valid_data = df[[x, y_col]].dropna()
        if len(valid_data) > 0:
            fig.add_trace(go.Scatter(
                x=valid_data[x],
                y=valid_data[y_col],
                mode='lines',
                name=y_col
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title=y_label,
        template='plotly_white',
        showlegend=True
    )
    
    return fig

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
    print("[DEBUG] Looking for weather columns in DataFrame")
    print(f"[DEBUG] Available columns: {df.columns.tolist()}")
    
    for col in df.columns:
        col_lower = col.lower()
        for category, patterns in weather_patterns.items():
            if any(pattern.lower() in col_lower for pattern in patterns):
                if pd.api.types.is_numeric_dtype(df[col]):
                    weather_cols.append(col)
                    print(f"[DEBUG] Found weather column: {col} matching category {category}")
                else:
                    print(f"[DEBUG] Column {col} matches pattern but is not numeric. Type: {df[col].dtype}")
                break
    
    print(f"[DEBUG] Identified weather columns: {weather_cols}")
    return weather_cols

def create_visualizations(original_df: pd.DataFrame, processed_df: pd.DataFrame,
                        combined_df: Optional[pd.DataFrame], wavelength: str,
                        timestamp: str, job_id: Optional[str] = None) -> Dict[str, str]:
    """Create visualizations with improved memory efficiency and error handling"""
    try:
        print(f"[DEBUG] Starting create_visualizations")
        print(f"[DEBUG] Original DataFrame shape: {original_df.shape}")
        print(f"[DEBUG] Processed DataFrame shape: {processed_df.shape}")
        print(f"[DEBUG] Combined DataFrame shape: {combined_df.shape if combined_df is not None else 'None'}")
        print(f"[DEBUG] Processed DataFrame columns: {processed_df.columns.tolist()}")
        print(f"[DEBUG] processedBC stats in processed_df: {processed_df['processedBC'].describe() if 'processedBC' in processed_df.columns else 'Not found'}")
        
        if job_id:
            processing_status[job_id] = "Creating visualizations"
            processing_messages[job_id] = "Preparing data for visualization..."
            processing_progress[job_id] = 85
        
        # Create static directory
        static_folder = 'app/static'
        os.makedirs(static_folder, exist_ok=True)
        
        # Downsample data for visualization
        viz_df = downsample_data(processed_df, max_points=10000)
        
        # Initialize result dictionary
        result = {
            'bc_time_series': None,
            'atn_time_series': None,
            'bc_comparison': None,
            'weather_correlation': None,
            'time_aligned': None
        }
        
        try:
            # BC Time Series
            if job_id:
                processing_messages[job_id] = "Generating BC time series plot..."
            
            print(f"[DEBUG] Creating BC time series plot")
            print(f"[DEBUG] viz_df shape before time series: {viz_df.shape}")
            print(f"[DEBUG] viz_df columns: {viz_df.columns.tolist()}")
            print(f"[DEBUG] processedBC null values: {viz_df['processedBC'].isnull().sum() if 'processedBC' in viz_df.columns else 'Column not found'}")
            print(f"[DEBUG] processedBC stats in viz_df: {viz_df['processedBC'].describe() if 'processedBC' in viz_df.columns else 'Not found'}")
            
            bc_fig = create_time_series_plot(
                viz_df,
                'timestamp',
                ['rawBC', 'processedBC'],
                f'{wavelength} BC Time Series',
                'BC (ng/m³)'
            )
            bc_time_series_path = os.path.join(static_folder, f'bc_time_series_{timestamp}.html')
            bc_fig.write_html(bc_time_series_path)
            result['bc_time_series'] = f'/static/bc_time_series_{timestamp}.html'
            
            # ATN Time Series
            if job_id:
                processing_messages[job_id] = "Generating ATN time series plot..."
                processing_progress[job_id] = 90
            
            atn_fig = create_time_series_plot(
                viz_df,
                'timestamp',
                ['atn'],
                f'{wavelength} ATN Time Series',
                'ATN'
            )
            atn_time_series_path = os.path.join(static_folder, f'atn_time_series_{timestamp}.html')
            atn_fig.write_html(atn_time_series_path)
            result['atn_time_series'] = f'/static/atn_time_series_{timestamp}.html'
            
            # BC Comparison
            if job_id:
                processing_messages[job_id] = "Generating BC comparison plot..."
                processing_progress[job_id] = 95
            
            print(f"[DEBUG] Creating BC comparison plot")
            print(f"[DEBUG] viz_df shape before comparison: {viz_df.shape}")
            print(f"[DEBUG] Required columns present: rawBC={('rawBC' in viz_df.columns)}, processedBC={('processedBC' in viz_df.columns)}")
            
            valid_bc_data = viz_df[['rawBC', 'processedBC']].dropna()
            print(f"[DEBUG] Valid BC data shape after dropna: {valid_bc_data.shape}")
            print(f"[DEBUG] Rows dropped due to NaN: {len(viz_df) - len(valid_bc_data)}")
            print(f"[DEBUG] processedBC stats in valid_bc_data: {valid_bc_data['processedBC'].describe() if len(valid_bc_data) > 0 else 'Empty DataFrame'}")
            
            if len(valid_bc_data) > 0:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=valid_bc_data['rawBC'],
                    y=valid_bc_data['processedBC'],
                    mode='markers',
                    name='BC Comparison',
                    marker=dict(
                        color=valid_bc_data['processedBC'],
                        colorscale='Viridis',
                        showscale=True
                    )
                ))
                
                # Add 1:1 line
                min_val = min(valid_bc_data['rawBC'].min(), valid_bc_data['processedBC'].min())
                max_val = max(valid_bc_data['rawBC'].max(), valid_bc_data['processedBC'].max())
                fig.add_trace(go.Scatter(
                    x=[min_val, max_val],
                    y=[min_val, max_val],
                    mode='lines',
                    name='1:1 Line',
                    line=dict(dash='dash')
                ))
                
                # Add correlation statistics
                corr_stats = calculate_correlations(valid_bc_data, 'rawBC', 'processedBC')
                if corr_stats:
                    fig.add_annotation(
                        text=f"Pearson r: {corr_stats['pearson_r']:.3f}<br>Spearman ρ: {corr_stats['spearman_r']:.3f}",
                        xref="paper", yref="paper",
                        x=0.05, y=0.95,
                        showarrow=False,
                        bgcolor='rgba(255,255,255,0.8)'
                    )
                
                fig.update_layout(
                    title=f'{wavelength} BC: Raw vs Processed',
                    xaxis_title='Raw BC (ng/m³)',
                    yaxis_title='Processed BC (ng/m³)',
                    template='plotly_white'
                )
                
                bc_comparison_path = os.path.join(static_folder, f'bc_comparison_{timestamp}.html')
                fig.write_html(bc_comparison_path)
                result['bc_comparison'] = f'/static/bc_comparison_{timestamp}.html'
            
            # Weather correlation plots
            if combined_df is not None and not combined_df.empty:
                print("[DEBUG] Starting weather correlation visualization")
                print(f"[DEBUG] Combined DataFrame shape: {combined_df.shape}")
                print(f"[DEBUG] Combined DataFrame columns: {combined_df.columns.tolist()}")
                
                if job_id:
                    processing_messages[job_id] = "Generating weather correlation plots..."
                    processing_progress[job_id] = 98
                
                # Add processedBC to combined_df if not present
                if 'processedBC' not in combined_df.columns and 'processedBC' in processed_df.columns:
                    print("[DEBUG] Adding processedBC to combined data")
                    combined_df = combined_df.copy()
                    combined_df['processedBC'] = processed_df['processedBC']
                
                # Downsample combined data
                combined_viz_df = downsample_data(combined_df)
                
                # Identify weather columns with improved detection
                weather_cols = identify_weather_columns(combined_viz_df)

                print(
                    f"[DEBUG] Identified weather columns: {weather_cols}, "
                    f"Combined viz columns: {combined_viz_df.columns.tolist()}, "
                    f"ProcessedBC present: {'processedBC' in combined_viz_df.columns}"
                )

                if weather_cols and 'processedBC' in combined_viz_df.columns:
                    print("[DEBUG] Creating weather correlation subplots")
                    print(f"[DEBUG] ProcessedBC stats: {combined_viz_df['processedBC'].describe()}")
                    try:
                        # Create correlation plots with improved layout
                        fig = make_subplots(
                            rows=len(weather_cols),
                            cols=1,
                            subplot_titles=[f'{wavelength} BC vs {col}' for col in weather_cols],
                            vertical_spacing=0.2
                        )
                        
                        for i, weather_col in enumerate(weather_cols, 1):
                            print(f"[DEBUG] Creating subplot for {weather_col}")
                            
                            # Calculate correlations
                            corr_stats = calculate_correlations(combined_viz_df, 'processedBC', weather_col)
                            print(f"[DEBUG] Correlation stats for {weather_col}: {corr_stats}")
                            
                            # Add scatter plot
                            fig.add_trace(
                                go.Scatter(
                                    x=combined_viz_df[weather_col],
                                    y=combined_viz_df['processedBC'],
                                    mode='markers',
                                    marker=dict(
                                        size=8,
                                        color=combined_viz_df['processedBC'], # Color by BC values
                                        colorscale='Plasma', # Change color scale to 'Plasma'
                                        showscale=True if i == len(weather_cols) else False, # Show scale only for the last plot
                                        colorbar=dict(title='BC (ng/m³)') if i == 1 else None
                                    ),
                                    name=weather_col
                                ),
                                row=i, col=1
                            )
                            
                            if corr_stats:
                                fig.add_annotation(
                                    text=(f"Pearson r: {corr_stats['pearson_r']:.3f}<br>"
                                          f"Spearman ρ: {corr_stats['spearman_r']:.3f}"),
                                    xref=f"x{i}", yref=f"y{i}",
                                    x=0.95, y=0.95,
                                    showarrow=False,
                                    bgcolor='rgba(255,255,255,0.8)',
                                    xanchor='right'
                                )
                        
                        print("[DEBUG] Updating layout for weather correlation plot")
                        fig.update_layout(
                            height=300 * len(weather_cols),
                            width=800,
                            template='plotly_white',
                            showlegend=False
                        )
                        
                        # Update x and y axis labels
                        for i, weather_col in enumerate(weather_cols, 1):
                           fig.update_xaxes(
                                title_text=f'{weather_col} (Units)',
                                row=i, col=1
                            )
                           fig.update_yaxes(
                                title_text='Black Carbon (ng/m³)',
                                row=i, col=1
                            )
                        print("[DEBUG] Saving weather correlation plot")
                        weather_correlation_path = os.path.join(static_folder, f'weather_correlation_{timestamp}.html')
                        fig.write_html(weather_correlation_path)
                        result['weather_correlation'] = f'/static/weather_correlation_{timestamp}.html'
                        print("[DEBUG] Weather correlation plot saved successfully")
                    except Exception as e:
                        print(f"[DEBUG] Error creating weather correlation plot: {str(e)}")
                        print(traceback.format_exc())
                else:
                    print("[DEBUG] No weather columns identified for correlation plots")
            else:
                print("[DEBUG] No combined data available for weather correlation")
        
        except Exception as e:
            print(f"Error in visualization creation: {e}")
            print(traceback.format_exc())
            if job_id:
                processing_messages[job_id] = f"Warning: Some visualizations could not be created: {str(e)}"
        
        if job_id:
            processing_messages[job_id] = "Visualizations created successfully"
            processing_progress[job_id] = 100
        
        return result
        
    except Exception as e:
        error_msg = f"Error creating visualizations: {str(e)}"
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = error_msg
        print(error_msg)
        print(traceback.format_exc())
        return {
            'bc_time_series': None,
            'atn_time_series': None,
            'bc_comparison': None,
            'weather_correlation': None,
            'time_aligned': None
        }
