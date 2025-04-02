import os
import plotly
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
from app.utils.status_tracker import processing_status, processing_progress, processing_messages

def create_visualizations(original_df, processed_df, combined_df, wavelength, timestamp, job_id=None):
    """Create visualizations for the processed data"""
    if job_id:
        processing_status[job_id] = "Creating visualizations"
        processing_messages[job_id] = "Generating BC time series plot..."
        processing_progress[job_id] = 85
    
    try:
        # Create static directory if it doesn't exist
        static_folder = 'app/static'
        os.makedirs(static_folder, exist_ok=True)
        
        # For large datasets, downsample to prevent memory issues
        max_points = 10000  # Maximum number of points for visualization
        
        # Downsample if necessary
        if len(processed_df) > max_points:
            if job_id:
                processing_messages[job_id] = f"Downsampling data from {len(processed_df)} to {max_points} points for visualization..."
            
            # Calculate sampling interval
            sample_interval = len(processed_df) // max_points
            
            # Create downsampled dataframe for visualization
            viz_df = processed_df.iloc[::sample_interval].copy()
            if job_id:
                processing_messages[job_id] = f"Downsampled to {len(viz_df)} points for visualization"
        else:
            viz_df = processed_df.copy()
        
        # BC Time Series
        if job_id:
            processing_messages[job_id] = "Generating BC time series plot..."
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=viz_df['timestamp'],
            y=viz_df['rawBC'],
            mode='lines',
            name='Raw BC'
        ))
        fig.add_trace(go.Scatter(
            x=viz_df['timestamp'],
            y=viz_df['processedBC'],
            mode='lines',
            name='Processed BC (ONA)'
        ))
        fig.update_layout(
            title=f'{wavelength} BC Time Series',
            xaxis_title='Time',
            yaxis_title='BC (ng/m³)',
            template='plotly_white'
        )
        bc_time_series_path = os.path.join(static_folder, f'bc_time_series_{timestamp}.html')
        fig.write_html(bc_time_series_path)
        
        if job_id:
            processing_messages[job_id] = "Generating ATN time series plot..."
            processing_progress[job_id] = 90
        
        # ATN Time Series
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=viz_df['timestamp'],
            y=viz_df['atn'],
            mode='lines',
            name='ATN'
        ))
        fig.update_layout(
            title=f'{wavelength} ATN Time Series',
            xaxis_title='Time',
            yaxis_title='ATN',
            template='plotly_white'
        )
        atn_time_series_path = os.path.join(static_folder, f'atn_time_series_{timestamp}.html')
        fig.write_html(atn_time_series_path)
        
        if job_id:
            processing_messages[job_id] = "Generating BC comparison plot..."
            processing_progress[job_id] = 95
        
        # BC Comparison (Raw vs Processed)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=viz_df['rawBC'],
            y=viz_df['processedBC'],
            mode='markers',
            name='BC Comparison'
        ))
        
        # Calculate min and max for 1:1 line
        min_val = min(viz_df['rawBC'].min(), viz_df['processedBC'].min())
        max_val = max(viz_df['rawBC'].max(), viz_df['processedBC'].max())
        
        fig.add_trace(go.Scatter(
            x=[min_val, max_val],
            y=[min_val, max_val],
            mode='lines',
            name='1:1 Line',
            line=dict(dash='dash')
        ))
        fig.update_layout(
            title=f'{wavelength} BC: Raw vs Processed',
            xaxis_title='Raw BC (ng/m³)',
            yaxis_title='Processed BC (ng/m³)',
            template='plotly_white'
        )
        bc_comparison_path = os.path.join(static_folder, f'bc_comparison_{timestamp}.html')
        fig.write_html(bc_comparison_path)
        
        # Weather Correlation (if weather data available)
        weather_correlation_path = None
        if combined_df is not None and not combined_df.empty:
            if job_id:
                processing_messages[job_id] = "Generating weather correlation plots..."
                processing_progress[job_id] = 98
            
            # Downsample combined data if necessary
            if len(combined_df) > max_points:
                sample_interval = len(combined_df) // max_points
                combined_viz_df = combined_df.iloc[::sample_interval].copy()
            else:
                combined_viz_df = combined_df.copy()
            
            # Find weather columns
            weather_cols = []
            for col in combined_viz_df.columns:
                if col.lower() in ['temperaturec', 'temperature', 'relativehumiditypercent', 'humidity', 
                                'precipitationmm', 'precipitation', 'windspeedkmh', 'windspeed', 
                                'pressurehpa', 'pressure']:
                    weather_cols.append(col)
            
            if weather_cols and len(weather_cols) > 0:
                # Create a figure with subplots for each weather parameter
                num_weather_cols = len(weather_cols)
                
                # Create a figure with subplots
                fig = plotly.subplots.make_subplots(
                    rows=num_weather_cols, 
                    cols=1,
                    subplot_titles=[f'{wavelength} BC vs {col}' for col in weather_cols],
                    vertical_spacing=0.1
                )
                
                # Check if the expected BC column exists
                bc_col = f'{wavelength.lower()}BC1'
                if bc_col not in combined_viz_df.columns:
                    # Try to find an alternative BC column
                    bc_col = 'processedBC'
                
                if bc_col in combined_viz_df.columns:
                    # Add a scatter plot for each weather parameter
                    for i, weather_col in enumerate(weather_cols):
                        # Calculate correlation coefficient
                        valid_data = combined_viz_df[[bc_col, weather_col]].dropna()
                        
                        if len(valid_data) > 5:  # Need at least a few points for correlation
                            pearson_r, pearson_p = stats.pearsonr(valid_data[bc_col], valid_data[weather_col])
                            spearman_r, spearman_p = stats.spearmanr(valid_data[bc_col], valid_data[weather_col])
                            
                            # Format correlation text
                            corr_text = (
                                f"Pearson r: {pearson_r:.3f} (p: {pearson_p:.3f})<br>"
                                f"Spearman ρ: {spearman_r:.3f} (p: {spearman_p:.3f})"
                            )
                            
                            # Add scatter plot
                            fig.add_trace(
                                go.Scatter(
                                    x=combined_viz_df[weather_col],
                                    y=combined_viz_df[bc_col],
                                    mode='markers',
                                    name=f'BC vs {weather_col}',
                                    marker=dict(
                                        color=combined_viz_df[bc_col],
                                        colorscale='Viridis',
                                        showscale=True,
                                        colorbar=dict(title='BC (ng/m³)')
                                    )
                                ),
                                row=i+1, col=1
                            )
                            
                            # Add annotation with correlation coefficients
                            fig.add_annotation(
                                xref=f"x{i+1}", yref=f"y{i+1}",
                                x=0.05, y=0.95,
                                xanchor='left', yanchor='top',
                                text=corr_text,
                                showarrow=False,
                                bgcolor='rgba(255, 255, 255, 0.8)',
                                bordercolor='rgba(0, 0, 0, 0.3)',
                                borderwidth=1,
                                borderpad=4,
                                font=dict(size=10)
                            )
                    
                    # Update layout
                    fig.update_layout(
                        height=300 * num_weather_cols,
                        width=800,
                        template='plotly_white',
                        showlegend=False
                    )
                    
                    # Update x and y axis labels
                    for i, weather_col in enumerate(weather_cols):
                        fig.update_xaxes(title_text=weather_col, row=i+1, col=1)
                        fig.update_yaxes(title_text='BC (ng/m³)', row=i+1, col=1)
                    
                    # Save the figure
                    weather_correlation_path = os.path.join(static_folder, f'weather_correlation_{timestamp}.html')
                    fig.write_html(weather_correlation_path)
                    
                    # Create a time-aligned plot if we have timestamp data
                    if 'timestamp' in combined_viz_df.columns:
                        # Create a figure with subplots
                        time_fig = plotly.subplots.make_subplots(
                            rows=num_weather_cols + 1,  # +1 for BC
                            cols=1,
                            subplot_titles=[f'{wavelength} BC'] + weather_cols,
                            vertical_spacing=0.05,
                            shared_xaxes=True
                        )
                        
                        # Add BC time series
                        time_fig.add_trace(
                            go.Scatter(
                                x=combined_viz_df['timestamp'],
                                y=combined_viz_df[bc_col],
                                mode='lines',
                                name=f'{wavelength} BC'
                            ),
                            row=1, col=1
                        )
                        
                        # Add weather parameters
                        for i, weather_col in enumerate(weather_cols):
                            time_fig.add_trace(
                                go.Scatter(
                                    x=combined_viz_df['timestamp'],
                                    y=combined_viz_df[weather_col],
                                    mode='lines',
                                    name=weather_col
                                ),
                                row=i+2, col=1
                            )
                        
                        # Update layout
                        time_fig.update_layout(
                            height=200 * (num_weather_cols + 1),
                            width=800,
                            template='plotly_white',
                            title_text='Time-Aligned BC and Weather Parameters'
                        )
                        
                        # Update y-axis labels
                        time_fig.update_yaxes(title_text='BC (ng/m³)', row=1, col=1)
                        for i, weather_col in enumerate(weather_cols):
                            time_fig.update_yaxes(title_text=weather_col, row=i+2, col=1)
                        
                        # Update x-axis label only for the bottom subplot
                        time_fig.update_xaxes(title_text='Time', row=num_weather_cols+1, col=1)
                        
                        # Save the time-aligned figure
                        time_aligned_path = os.path.join(static_folder, f'time_aligned_{timestamp}.html')
                        time_fig.write_html(time_aligned_path)
        
        if job_id:
            processing_messages[job_id] = "Visualizations created successfully"
            processing_progress[job_id] = 100
        
        # Return paths to visualizations
        result = {
            'bc_time_series': f'/static/bc_time_series_{timestamp}.html',
            'atn_time_series': f'/static/atn_time_series_{timestamp}.html',
            'bc_comparison': f'/static/bc_comparison_{timestamp}.html',
            'weather_correlation': f'/static/weather_correlation_{timestamp}.html' if weather_correlation_path else None,
            'time_aligned': f'/static/time_aligned_{timestamp}.html' if 'time_aligned_path' in locals() else None
        }
        
        return result
    
    except Exception as e:
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = f"Error creating visualizations: {str(e)}"
            print(f"Error creating visualizations: {e}")
        return {
            'bc_time_series': None,
            'atn_time_series': None,
            'bc_comparison': None,
            'weather_correlation': None,
            'time_aligned': None
        }
