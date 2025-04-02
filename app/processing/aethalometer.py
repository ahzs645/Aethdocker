import pandas as pd
import numpy as np
import os
import re
from app.utils.status_tracker import processing_status, processing_progress, processing_messages

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
    result = result.replace('/', '')
    result = result.replace('.', '')
    result = result.replace('-', '')
    return result

def map_field_names(df):
    """Map field names to standardized format"""
    renamed_columns = {}
    for col in df.columns:
        renamed_columns[col] = transform_header(col)
    return df.rename(columns=renamed_columns)

def process_aethalometer_data_in_chunks(file_path, chunk_size=100000, job_id=None):
    """Process aethalometer data file in chunks for better performance with large files"""
    try:
        if job_id:
            processing_status[job_id] = "Reading data"
            processing_messages[job_id] = "Determining file size..."
            processing_progress[job_id] = 5
        
        # Get file size for progress tracking
        file_size = os.path.getsize(file_path)
        bytes_processed = 0
        
        # Initialize an empty DataFrame to store the results
        df_chunks = []
        
        if job_id:
            processing_messages[job_id] = "Reading CSV file in chunks..."
            processing_progress[job_id] = 10
        
        # Process the file in chunks
        chunk_count = 0
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            chunk_count += 1
            if job_id:
                # Update progress based on chunks processed
                progress = min(60, 10 + int(chunk_num * 50 / (file_size / (chunk_size * 100))))
                processing_progress[job_id] = progress
                processing_messages[job_id] = f"Processing chunk {chunk_num+1}..."
            
            # Standardize column names
            chunk = map_field_names(chunk)
            
            # Convert timestamp if present
            if 'timestamp' not in chunk.columns:
                # Try to find date and time columns
                date_col = None
                time_col = None
                
                for col in chunk.columns:
                    if 'date' in col.lower():
                        date_col = col
                    if 'time' in col.lower() and 'local' in col.lower():
                        time_col = col
                
                if date_col and time_col:
                    chunk['timestamp'] = pd.to_datetime(chunk[date_col] + ' ' + chunk[time_col])
                else:
                    # Create a timestamp column with row index as placeholder
                    chunk['timestamp'] = pd.date_range(
                        start='2023-01-01', 
                        periods=len(chunk), 
                        freq='1min'
                    )
            
            df_chunks.append(chunk)
        
        if job_id:
            processing_messages[job_id] = "Combining data chunks..."
            processing_progress[job_id] = 65
        
        # Combine all chunks
        df = pd.concat(df_chunks, ignore_index=True)
        
        if job_id:
            processing_messages[job_id] = "Data processing complete"
            processing_progress[job_id] = 70
        
        return df
    except Exception as e:
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = f"Error processing aethalometer data: {str(e)}"
            print(f"Error processing aethalometer data: {e}")
        return pd.DataFrame()

def apply_ona_algorithm(df, wavelength="Blue", atn_min=0.01, job_id=None):
    """Apply ONA algorithm to reduce noise while preserving high time resolution"""
    # Update status
    if job_id:
        processing_status[job_id] = "Applying ONA algorithm"
        processing_messages[job_id] = "Identifying wavelength columns..."
        processing_progress[job_id] = 70  # Start ONA at 70%
    
    # Identify ATN column for the selected wavelength
    atn_col = f"{wavelength.lower()}ATN1"
    bc_col = f"{wavelength.lower()}BC1"
    
    if atn_col not in df.columns or bc_col not in df.columns:
        # Try alternative column names
        for col in df.columns:
            if wavelength.lower() in col.lower() and "atn" in col.lower() and "1" in col:
                atn_col = col
            if wavelength.lower() in col.lower() and "bc" in col.lower() and "1" in col:
                bc_col = col
    
    if job_id:
        processing_messages[job_id] = f"Using columns: {atn_col} and {bc_col}"
    
    if atn_col not in df.columns or bc_col not in df.columns:
        if job_id:
            processing_status[job_id] = "Error"
            processing_messages[job_id] = f"Could not find {wavelength} ATN and BC columns"
        return df, pd.DataFrame()  # Return original if columns not found
    
    # Sort by time
    if job_id:
        processing_messages[job_id] = "Sorting data by timestamp..."
    df = df.sort_values('timestamp')
    
    # Initialize result dataframe
    result = pd.DataFrame(columns=['timestamp', 'rawBC', 'processedBC', 'atn'])
    
    # Apply ONA algorithm
    if job_id:
        processing_messages[job_id] = "Applying ONA algorithm to data..."
    
    # For large datasets, use a more efficient approach
    if len(df) > 10000:
        # Process in chunks to avoid memory issues
        chunk_size = min(5000, max(100, len(df) // 20))  # Adjust chunk size based on data size
        chunks = [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
        
        for chunk_idx, chunk in enumerate(chunks):
            if job_id:
                # Update progress for each chunk
                progress = 70 + int((chunk_idx / len(chunks)) * 15)  # ONA goes from 70% to 85%
                processing_progress[job_id] = progress
                processing_messages[job_id] = f"Processing chunk {chunk_idx+1}/{len(chunks)} of ONA algorithm..."
            
            # Process this chunk
            chunk_result = process_ona_chunk(chunk, atn_col, bc_col, atn_min)
            result = pd.concat([result, chunk_result], ignore_index=True)
    else:
        # For smaller datasets, process all at once
        result = process_ona_chunk(df, atn_col, bc_col, atn_min)
        if job_id:
            processing_progress[job_id] = 85
    
    if job_id:
        processing_messages[job_id] = "ONA algorithm applied successfully"
    
    return df, result

def process_ona_chunk(df, atn_col, bc_col, atn_min):
    """Process a chunk of data with the ONA algorithm"""
    result = pd.DataFrame(columns=['timestamp', 'rawBC', 'processedBC', 'atn'])
    
    i = 0
    while i < len(df) - 1:
        start_idx = i
        start_atn = df.iloc[i][atn_col]
        
        # Find end of window where ATN change >= atn_min
        j = i + 1
        while j < len(df) and df.iloc[j][atn_col] - start_atn < atn_min:
            j += 1
        
        if j < len(df):
            # Calculate average BC for this window
            window = df.iloc[i:j+1]
            avg_bc = window[bc_col].mean()
            
            # Add to result more efficiently
            window_result = pd.DataFrame({
                'timestamp': df.iloc[i:j+1]['timestamp'].values,
                'rawBC': df.iloc[i:j+1][bc_col].values,
                'processedBC': [avg_bc] * (j-i+1),
                'atn': df.iloc[i:j+1][atn_col].values
            })
            result = pd.concat([result, window_result], ignore_index=True)
            
            i = j + 1
        else:
            # Add remaining points
            remaining_result = pd.DataFrame({
                'timestamp': df.iloc[i:]['timestamp'].values,
                'rawBC': df.iloc[i:][bc_col].values,
                'processedBC': df.iloc[i:][bc_col].values,  # No processing for last points
                'atn': df.iloc[i:][atn_col].values
            })
            result = pd.concat([result, remaining_result], ignore_index=True)
            break
    
    return result
