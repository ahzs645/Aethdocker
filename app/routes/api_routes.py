from flask import Blueprint, request, jsonify, send_from_directory, Response
from werkzeug.utils import secure_filename
import os
import threading
import datetime
import traceback
import json
import numpy as np
import pandas as pd
import shutil
from typing import Optional, Dict, Any

from app.processing.aethalometer import process_aethalometer_data_in_chunks, apply_ona_algorithm
from app.processing.weather import process_weather_data, synchronize_data
from app.processing.visualization import prepare_visualization_data
from app.utils.status_tracker import processing_status, processing_progress, processing_messages
from app.utils.json_encoder import NpEncoder, safe_json_dumps, clean_dict_for_json, ensure_json_serializable

api_bp = Blueprint('api', __name__)

def validate_file(file, allowed_extensions=None) -> bool:
    """Validate file extension and content"""
    if not file or not file.filename:
        return False
    
    if allowed_extensions:
        return '.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in allowed_extensions
    return True

def cleanup_old_files(directory: str, max_age_hours: int = 24):
    """Clean up old temporary files"""
    try:
        current_time = datetime.datetime.now()
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                file_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                if (current_time - file_time).total_seconds() > max_age_hours * 3600:
                    os.remove(filepath)
    except Exception as e:
        print(f"Error during cleanup: {e}")

@api_bp.route('/process', methods=['POST'])
def process_data():
    """Handle data processing request with improved validation and error handling"""
    try:
        # Validate aethalometer file
        if 'aethalometer_file' not in request.files:
            return jsonify({'error': 'No aethalometer file provided'}), 400
        
        aethalometer_file = request.files['aethalometer_file']
        if not validate_file(aethalometer_file, {'csv'}):
            return jsonify({'error': 'Invalid aethalometer file format. Only CSV files are allowed.'}), 400
        
        # Validate weather file if provided
        weather_file = request.files.get('weather_file')
        if weather_file and not validate_file(weather_file, {'csv'}):
            return jsonify({'error': 'Invalid weather file format. Only CSV files are allowed.'}), 400
        
        # Validate parameters
        try:
            atn_min = float(request.form.get('atn_min', 0.01))
            if atn_min <= 0:
                return jsonify({'error': 'ATN min must be positive'}), 400
        except ValueError:
            return jsonify({'error': 'Invalid ATN min value'}), 400
        
        wavelength = request.form.get('wavelength', 'Blue')
        if wavelength not in ['Blue', 'Green', 'Red', 'UV', 'IR']:
            return jsonify({'error': 'Invalid wavelength specified'}), 400
        
        # Generate unique job ID
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        job_id = f"job_{timestamp}_{hash(aethalometer_file.filename)}"
        
        # Create necessary directories
        upload_folder = 'app/data'
        results_folder = 'app/data/results'
        os.makedirs(upload_folder, exist_ok=True)
        os.makedirs(results_folder, exist_ok=True)
        
        # Clean up old files
        cleanup_old_files(upload_folder)
        cleanup_old_files(results_folder)
        
        # Save uploaded files
        aethalometer_path = os.path.join(upload_folder, secure_filename(aethalometer_file.filename))
        aethalometer_file.save(aethalometer_path)
        
        weather_path = None
        if weather_file and weather_file.filename:
            weather_path = os.path.join(upload_folder, secure_filename(weather_file.filename))
            weather_file.save(weather_path)
        
        # Initialize processing status
        processing_status[job_id] = "Initializing"
        processing_progress[job_id] = 0
        processing_messages[job_id] = "Starting data processing..."
        
        # Start processing in background thread
        processing_thread = threading.Thread(
            target=process_data_async,
            args=(job_id, aethalometer_path, weather_path, atn_min, wavelength)
        )
        processing_thread.daemon = True
        processing_thread.start()
        
        return jsonify({
            'job_id': job_id,
            'status': 'Processing started',
            'message': 'Processing has started. Poll /api/status/{job_id} for updates.'
        })
        
    except Exception as e:
        print(f"Error in process_data: {e}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

def process_data_async(job_id: str, aethalometer_path: str, weather_path: Optional[str], 
                      atn_min: float, wavelength: str):
    """Process data asynchronously with improved error handling and memory management"""
    try:
        # Process aethalometer data
        aethalometer_df = process_aethalometer_data_in_chunks(aethalometer_path, job_id=job_id)
        if aethalometer_df.empty:
            raise ValueError("Invalid aethalometer data format")
        
        # Apply ONA algorithm
        original_df, processed_df = apply_ona_algorithm(aethalometer_df, wavelength, atn_min, job_id=job_id)
        if processed_df.empty:
            raise ValueError(f"Could not find {wavelength} ATN and BC columns")
        
        # Process weather data if provided
        weather_df = None
        combined_df = None
        if weather_path:
            try:
                weather_df = process_weather_data(weather_path, job_id=job_id)
                
                if weather_df is not None and not weather_df.empty:
                    combined_df = synchronize_data(original_df, weather_df, job_id=job_id)
            except Exception as e:
                error_msg = f"Warning: Weather data processing failed: {str(e)}"
                print(f"[DEBUG] {error_msg}")
                print(traceback.format_exc())
                processing_messages[job_id] = error_msg
        
        # Save results
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        results_folder = 'app/data/results'
        processed_path = os.path.join(results_folder, f'processed_{wavelength}_{timestamp}.csv')
        
        # Save processed data efficiently
        processed_df.to_csv(processed_path, index=False)
        
        # Prepare visualization data
        visualization_data = prepare_visualization_data(
            original_df, processed_df, combined_df, wavelength, job_id=job_id
        )
        
        # Prepare results data
        try:
            # Determine sample size based on data size
            total_rows = len(processed_df)
            sample_size = min(1000, total_rows)
            
            # Prepare data samples efficiently
            result_data = {
                'aethalometer_data': clean_dict_for_json(
                    original_df.head(sample_size).replace({np.nan: None}).to_dict(orient='records')
                ),
                'processed_data': clean_dict_for_json(
                    processed_df.head(sample_size).replace({np.nan: None}).to_dict(orient='records')
                ),
                'combined_data': [],
                'wavelength': wavelength,
                'atn_min': atn_min,
                'visualization_data': clean_dict_for_json(visualization_data),
                'download_path': f'processed_{wavelength}_{timestamp}.csv',
                'total_rows': total_rows,
                'sample_size': sample_size
            }
            
            if combined_df is not None and not combined_df.empty:
                result_data['combined_data'] = clean_dict_for_json(
                    combined_df.head(sample_size).replace({np.nan: None}).to_dict(orient='records')
                )
            
            # Store results
            processing_status[job_id] = "Completed"
            processing_progress[job_id] = 100
            processing_messages[job_id] = "Processing completed successfully"
            processing_status[job_id + "_results"] = ensure_json_serializable(result_data)
            
        except Exception as e:
            print(f"Error preparing results: {e}")
            print(traceback.format_exc())
            
            # Store minimal results
            processing_status[job_id + "_results"] = ensure_json_serializable({
                'wavelength': wavelength,
                'atn_min': atn_min,
                'visualization_data': visualization_data,
                'download_path': f'processed_{wavelength}_{timestamp}.csv',
                'error': 'Error preparing detailed results'
            })
        
        # Cleanup temporary files
        try:
            os.remove(aethalometer_path)
            if weather_path:
                os.remove(weather_path)
        except Exception as e:
            print(f"Error cleaning up temporary files: {e}")
        
    except Exception as e:
        error_msg = f"Error during processing: {str(e)}"
        processing_status[job_id] = "Error"
        processing_messages[job_id] = error_msg
        processing_progress[job_id] = 0
        print(error_msg)
        print(traceback.format_exc())

@api_bp.route('/status/<job_id>', methods=['GET'])
def get_status(job_id: str):
    """Get processing status with improved error handling"""
    try:
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
        
        if status == "Completed" and job_id + "_results" in processing_status:
            response['results'] = processing_status[job_id + "_results"]
        elif status == "Error":
            response['error'] = message
        
        return jsonify(response)
        
    except Exception as e:
        print(f"Error in get_status: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@api_bp.route('/download/<filename>', methods=['GET'])
def download_file(filename: str):
    """Download processed file with security checks"""
    try:
        if not filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        file_path = os.path.join('app/data/results', filename)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        
        return send_from_directory('app/data/results', filename, as_attachment=True)
        
    except Exception as e:
        print(f"Error in download_file: {e}")
        return jsonify({'error': 'Internal server error'}), 500
