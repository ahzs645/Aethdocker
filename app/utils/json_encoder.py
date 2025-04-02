import json
import numpy as np
import pandas as pd

class NpEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles NumPy types and NaN values"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            # Replace NaN with None (null in JSON)
            return None if np.isnan(obj) else float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if pd.isna(obj):
            return None
        return super(NpEncoder, self).default(obj)

def clean_dict_for_json(data):
    """
    Recursively clean a dictionary to ensure all values are JSON serializable.
    Handles NaN, empty strings, and 'NA' values.
    """
    if isinstance(data, dict):
        return {k: clean_dict_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_dict_for_json(item) for item in data]
    elif isinstance(data, (float, np.float64, np.float32)) and (np.isnan(data) or np.isinf(data)):
        return None
    elif data == "" or data == "NA" or data == "NaN" or (isinstance(data, str) and data.lower() == "nan"):
        return None
    elif pd.isna(data):
        return None
    else:
        return data

def safe_json_dumps(data):
    """
    Safely convert data to JSON string, handling all types of problematic values.
    """
    # First clean the data to handle empty strings, 'NA', etc.
    cleaned_data = clean_dict_for_json(data)
    
    # Then use the custom encoder to handle NumPy types and any remaining NaN values
    return json.dumps(cleaned_data, cls=NpEncoder)

def ensure_json_serializable(obj):
    """
    Ensure an object is JSON serializable by first converting to JSON and back.
    This catches any serialization issues before they reach the client.
    """
    try:
        # Convert to JSON string using our safe encoder
        json_str = safe_json_dumps(obj)
        # Parse back to Python to verify it's valid JSON
        parsed = json.loads(json_str)
        return parsed
    except Exception as e:
        print(f"JSON serialization error: {e}")
        # If there's an error, return a sanitized version
        if isinstance(obj, dict):
            return {k: ensure_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ensure_json_serializable(item) for item in obj]
        elif isinstance(obj, (float, np.float64, np.float32)) and (np.isnan(obj) or np.isinf(obj)):
            return None
        elif obj == "" or obj == "NA" or obj == "NaN" or (isinstance(obj, str) and obj.lower() == "nan"):
            return None
        elif pd.isna(obj):
            return None
        else:
            # If we can't serialize it, convert to string
            try:
                return str(obj)
            except:
                return None
