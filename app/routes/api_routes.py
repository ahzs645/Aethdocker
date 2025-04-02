from flask import Blueprint, request, jsonify, send_from_directory, Response
from werkzeug.utils import secure_filename
import os
import threading
import datetime
import traceback
import json
import numpy as np
import pandas as pd

from app.processing.aethalometer import process_aethalometer_data_in_chunks, apply_ona_algorithm
from app.processing.weather import process_weather_data, synchronize_data
from app.processing.visualization import create_visualizations
from app.utils.status_tracker import processing_status, processing_progress, processing_messages
from app.utils.json_encoder import NpEncoder, safe_json_dumps, clean_dict_for_json, ensure_json_serializable

api_bp = Blueprint('api', __name__)

@api_bp.route('/process', methods=['POST'])
def process_data():
    if 'aethalometer_file' not in request.files:
        return jsonify({'error': 'No aethalometer file provided'}), 400
    
    aethalometer_file = request.files['aethalometer_file']
    weather_file = request.files.get('weather_file')
    atn_min = float(request.form.get('atn_min', 0.01))
    wavelength = request.form.get('wavelength', 'Blue')
    
    # Generate a unique job ID
    job_id = f"job_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{hash(aethalometer_file.filename)}"
    
    # Save uploaded files
    upload_folder = 'app/data'
    os.makedirs(upload_folder, exist_ok=True)
    
    aethalometer_path = os.path.join(upload_folder, secure_filename(aethalometer_file.filename))
    aethalometer_file.save(aethalometer_path)
    
    weather_path = None
    if weather_file and weather_file.filename:
        weather_path = os.path.join(upload_folder, secure_filename(weather_file.filename))
        weather_file.save(weather_path)
    
    # Start processing in a background thread
    processing_thread = threading.Thread(
        target=process_data_async,
        args=(job_id, aethalometer_path, weather_path, atn_min, wavelength)
    )
    processing_thread.daemon = True
    processing_thread.start()
    
    # Return job ID for status polling
    return jsonify({
        'job_id': job_id,
        'status': 'Processing started',
        'message': 'Processing has started. Poll /api/status/{job_id} for updates.'
    })

def process_data_async(job_id, aethalometer_path, weather_path, atn_min, wavelength):
    """Process data asynchronously with progress tracking"""
    try:
        # Initialize progress tracking
        processing_status[job_id] = "Processing"
        processing_progress[job_id] = 0
        processing_messages[job_id] = "Starting data processing..."
        
        # Create results directory if it doesn't exist
        results_folder = 'app/data/results'
        os.makedirs(results_folder, exist_ok=True)
        
        # Create static directory if it doesn't exist
        static_folder = 'app/static'
        os.makedirs(static_folder, exist_ok=True)
        
        # Process aethalometer data in chunks for better performance
        processing_progress[job_id] = 5
        aethalometer_df = process_aethalometer_data_in_chunks(aethalometer_path, chunk_size=100000, job_id=job_id)
        
        if aethalometer_df.empty:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = "Invalid aethalometer data format"
            return
        
        # Apply ONA algorithm
        original_df, processed_df = apply_ona_algorithm(aethalometer_df, wavelength, atn_min, job_id=job_id)
        
        if processed_df.empty:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = f"Could not find {wavelength} ATN and BC columns"
            return
        
        # Process weather data if provided
        weather_df = None
        combined_df = None
        
        if weather_path:
            processing_messages[job_id] = "Processing weather data..."
            weather_df = process_weather_data(weather_path, job_id=job_id)
            
            if weather_df is not None and not weather_df.empty:
                processing_messages[job_id] = "Synchronizing data..."
                combined_df = synchronize_data(original_df, weather_df, job_id=job_id)
        
        # Save processed data
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        processed_path = os.path.join(results_folder, f'processed_{wavelength}_{timestamp}.csv')
        
        processing_messages[job_id] = "Saving processed data..."
        processed_df.to_csv(processed_path, index=False)
        
        # Create visualizations
        visualizations = create_visualizations(original_df, processed_df, combined_df, wavelength, timestamp, job_id=job_id)
        
        # Store results
        processing_status[job_id] = "Completed"
        processing_progress[job_id] = 100
        processing_messages[job_id] = "Processing completed successfully"
        
        try:
            # Convert DataFrames to dictionaries with limited rows for performance
            # Use a larger sample for small datasets, smaller sample for large datasets
            sample_size = min(1000, max(100, len(processed_df) // 10))  # Adaptive sample size
            
            # Handle NaN values and convert to records
            original_data = original_df.head(sample_size).replace({np.nan: None, "": None, "NA": None}).to_dict(orient='records')
            processed_data = processed_df.head(sample_size).replace({np.nan: None, "": None, "NA": None}).to_dict(orient='records')
            combined_data = []
            if combined_df is not None and not combined_df.empty:
                combined_data = combined_df.head(sample_size).replace({np.nan: None, "": None, "NA": None}).to_dict(orient='records')
            
            # Clean all data for JSON serialization
            original_data = clean_dict_for_json(original_data)
            processed_data = clean_dict_for_json(processed_data)
            combined_data = clean_dict_for_json(combined_data)
            visualizations = clean_dict_for_json(visualizations)
            
            # Store result paths for retrieval with error handling
            result_data = {
                'aethalometer_data': original_data,
                'processed_data': processed_data,
                'combined_data': combined_data,
                'wavelength': wavelength,
                'atn_min': atn_min,
                'visualizations': visualizations,
                'download_path': f'processed_{wavelength}_{timestamp}.csv'
            }
            
            # Ensure all data is JSON serializable before storing
            processing_status[job_id + "_results"] = ensure_json_serializable(result_data)
            
        except Exception as e:
            # If there's an error in data serialization, store minimal results
            print(f"Error serializing results: {e}")
            print(traceback.format_exc())
            
            # Store minimal results that will still allow visualization access
            processing_status[job_id + "_results"] = ensure_json_serializable({
                'processed_data': [],  # Empty data but still valid JSON
                'wavelength': wavelength,
                'atn_min': atn_min,
                'visualizations': visualizations,
                'download_path': f'processed_{wavelength}_{timestamp}.csv'
            })
        
    except Exception as e:
        processing_status[job_id] = "Error"
        processing_messages[job_id] = f"Error during processing: {str(e)}"
        processing_progress[job_id] = 0
        print(f"Error during processing: {e}")
        print(traceback.format_exc())

@api_bp.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    if job_id not in processing_status:
        return jsonify({'error': 'Job not found'}), 404
    
    status = processing_status.get(job_id, "Unknown")
    progress = processing_progress.get(job_id, 0)
    message = processing_messages.get(job_id, "")
    
    response = {
        'status': status,
        'progress': progress,
        'message': message
    }
    
    # If processing is complete, include results
    if status == "Completed" and job_id + "_results" in processing_status:
        response['results'] = processing_status[job_id + "_results"]
    
    return jsonify(response)

@api_bp.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    return send_from_directory('app/data/results', filename, as_attachment=True)
