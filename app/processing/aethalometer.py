import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from app.utils.status_tracker import processing_status, processing_progress, processing_messages
from app.processing.weather import ensure_tz_aware

def transform_header(header):
    """Transform header to camelCase format"""
    result = header.strip()
    result = re.sub(r'\s*\(.*?\)\s*', '', result)  # Remove parenthetical descriptions
    result = re.sub(r'\s+', ' ', result)  # Normalize whitespace
    words = result.split(' ')
    if len(words) > 1:
        result = words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    else:
        result = words[0].lower()
    result = result.replace('%', 'Percent')
    result = re.sub(r'[/\.-]', '', result)
    return result

def map_field_names(df):
    """Map field names to standardized format"""
    return df.rename(columns={col: transform_header(col) for col in df.columns})

def validate_aethalometer_data(df, wavelength):
    """Validate required columns and data format"""
    atn_pattern = re.compile(f"{wavelength}\\s*ATN1", re.IGNORECASE)
    bc_pattern = re.compile(f"{wavelength}\\s*BC1", re.IGNORECASE)
    
    atn_col = next((col for col in df.columns if atn_pattern.search(col)), None)
    bc_col = next((col for col in df.columns if bc_pattern.search(col)), None)
    
    print(f"[DEBUG] Found columns - ATN: {atn_col}, BC: {bc_col}")
    
    if not (atn_col and bc_col):
        raise ValueError(f"Required columns for {wavelength} wavelength not found")
    
    # Convert to numeric, replacing invalid values with NaN
    df[atn_col] = pd.to_numeric(df[atn_col], errors='coerce')
    df[bc_col] = pd.to_numeric(df[bc_col], errors='coerce')
    
    # Remove rows with invalid data
    df = df.dropna(subset=[atn_col, bc_col])
    
    return df, atn_col, bc_col

def process_aethalometer_data_in_chunks(file_path, chunk_size=50000, job_id=None):
    """Process aethalometer data file in chunks with improved memory efficiency"""
    try:
        if job_id:
            processing_status[job_id] = "Reading"
            processing_messages[job_id] = "Initializing data processing..."
            processing_progress[job_id] = 5
        
        # Get file size for progress tracking
        file_size = os.path.getsize(file_path)
        total_chunks = file_size // (chunk_size * 100) + 1  # Estimate total chunks
        
        # Initialize an empty list to store DataFrames
        processed_chunks = []
        
        # Process the file in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            if job_id:
                progress = min(60, 10 + int(chunk_num * 50 / total_chunks))
                processing_progress[job_id] = progress
                processing_messages[job_id] = f"Processing chunk {chunk_num+1}/{total_chunks}..."
            
            # Standardize column names
            chunk = map_field_names(chunk)
            
            # Handle timestamp
            timestamp_col = next((col for col in chunk.columns if any(x in col.lower() for x in ['timestamp', 'date', 'time'])), None)
            
            if timestamp_col:
                try:
                    # Convert to datetime without timezone first
                    chunk['timestamp'] = pd.to_datetime(chunk[timestamp_col])
                except:
                    # Try to find separate date and time columns
                    date_col = next((col for col in chunk.columns if 'date' in col.lower()), None)
                    time_col = next((col for col in chunk.columns if 'time' in col.lower()), None)
                    
                    if date_col and time_col:
                        chunk['timestamp'] = pd.to_datetime(chunk[date_col] + ' ' + chunk[time_col])
                    else:
                        raise ValueError("No valid timestamp information found")
            
            # Ensure timestamp is timezone aware
            chunk['timestamp'] = ensure_tz_aware(chunk['timestamp'])
            
            processed_chunks.append(chunk)
            
            # Free memory periodically
            if len(processed_chunks) >= 10:
                processed_chunks = [pd.concat(processed_chunks, ignore_index=True)]
        
        if job_id:
            processing_messages[job_id] = "Combining processed chunks..."
            processing_progress[job_id] = 65
        
        # Combine all chunks efficiently
        df = pd.concat(processed_chunks, ignore_index=True)
        df = df.sort_values('timestamp')
        
        if job_id:
            processing_messages[job_id] = "Data processing complete"
            processing_progress[job_id] = 70
        
        return df
        
    except Exception as e:
        error_msg = f"Error processing aethalometer data: {str(e)}"
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = error_msg
        print(error_msg)
        raise RuntimeError(error_msg)

def apply_ona_algorithm(df, wavelength="Blue", atn_min=0.01, job_id=None):
    """Apply optimized ONA algorithm with improved memory efficiency"""
    try:
        if job_id:
            processing_status[job_id] = "Applying ONA"
            processing_messages[job_id] = "Preparing data for ONA algorithm..."
            processing_progress[job_id] = 70
        
        # Validate data and get column names
        df, atn_col, bc_col = validate_aethalometer_data(df, wavelength)
        
        if job_id:
            processing_messages[job_id] = f"Processing data with columns: {atn_col} and {bc_col}"
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Initialize arrays for efficient processing
        timestamps = df['timestamp'].values
        atn_values = df[atn_col].values
        bc_values = df[bc_col].values
        n_points = len(df)
        
        # Pre-allocate arrays for results
        processed_bc = np.zeros(n_points)
        window_starts = []
        window_ends = []
        
        if job_id:
            processing_messages[job_id] = "Applying ONA algorithm..."
        
        # Process data in a more efficient way
        i = 0
        while i < n_points:
            start_atn = atn_values[i]
            j = i + 1
            
            # Find window end
            while j < n_points and (atn_values[j] - start_atn) < atn_min:
                j += 1
            
            # Process window
            if j < n_points:
                window_bc_avg = np.mean(bc_values[i:j+1])
                processed_bc[i:j+1] = window_bc_avg
                window_starts.append(i)
                window_ends.append(j)
                i = j + 1
            else:
                # Handle remaining points
                processed_bc[i:] = bc_values[i:]
                break
            
            # Update progress
            if job_id and len(window_starts) % 100 == 0:
                progress = 70 + int((i / n_points) * 25)
                processing_progress[job_id] = min(95, progress)
        
        # Create result DataFrame efficiently
        result = pd.DataFrame({
            'timestamp': timestamps,
            'rawBC': bc_values,
            'processedBC': processed_bc
        })
        # Preserve original ATN column name
        result[atn_col] = atn_values
        print(f"[DEBUG] Result DataFrame columns: {result.columns.tolist()}")
        
        # Add window information
        result['windowStart'] = False
        result['windowEnd'] = False
        if window_starts:
            result.iloc[window_starts, result.columns.get_loc('windowStart')] = True
            result.iloc[window_ends, result.columns.get_loc('windowEnd')] = True
        
        if job_id:
            processing_messages[job_id] = "ONA algorithm completed successfully"
            processing_progress[job_id] = 95
        
        return df, result
        
    except Exception as e:
        error_msg = f"Error in ONA algorithm: {str(e)}"
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = error_msg
        print(error_msg)
        raise RuntimeError(error_msg)

def process_ona_chunk(df, atn_col, bc_col, atn_min):
    """Process a chunk of data with the ONA algorithm - kept for compatibility"""
    # This function is maintained for backward compatibility
    # but we recommend using the optimized apply_ona_algorithm instead
    return apply_ona_algorithm(df, wavelength=atn_col.replace('ATN1', ''), atn_min=atn_min)[1]
