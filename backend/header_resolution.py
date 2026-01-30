import pandas as pd
import re
from typing import Dict, List, Tuple, Optional

def detect_header_case(df: pd.DataFrame) -> Tuple[str, Dict]:
    """
    Detect which header case applies.
    
    Returns:
        (case_type, metadata)
        
    case_type:
        - 'valid': Headers look good
        - 'missing': Unnamed columns detected
        - 'suspicious': First row looks like data
    
    metadata:
        - For 'missing': list of unnamed column indices
        - For 'suspicious': analysis of why headers are suspicious
    """
    
    headers = list(df.columns)
    
    # Case 2: Check for unnamed/missing headers
    unnamed_pattern = re.compile(r'^unnamed[_:]?\d+$', re.IGNORECASE)
    numeric_pattern = re.compile(r'^\d+$')
    
    unnamed_indices = []
    for idx, col in enumerate(headers):
        col_str = str(col).strip().lower()
        if (
            col_str == '' or 
            col_str == 'nan' or
            unnamed_pattern.match(col_str) or
            numeric_pattern.match(col_str)
        ):
            unnamed_indices.append(idx)
    
    if unnamed_indices:
        return 'missing', {
            'unnamed_indices': unnamed_indices,
            'unnamed_columns': [headers[i] for i in unnamed_indices]
        }
    
    # Case 3: Check if headers look like data
    suspicion_score = 0
    suspicion_reasons = []
    
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    phone_pattern = re.compile(r'^[\d\s\(\)\-\+]{7,}$')
    
    for col in headers:
        col_str = str(col).strip()
        
        # All numeric
        if col_str.replace('.', '', 1).replace('-', '', 1).isdigit():
            suspicion_score += 2
            suspicion_reasons.append(f"Numeric value: {col_str}")
        
        # Email-like
        if email_pattern.match(col_str):
            suspicion_score += 3
            suspicion_reasons.append(f"Email-like: {col_str}")
        
        # Phone-like
        if phone_pattern.match(col_str):
            suspicion_score += 3
            suspicion_reasons.append(f"Phone-like: {col_str}")
    
    # If more than 30% of headers are suspicious, flag it
    if suspicion_score >= len(headers) * 0.3:
        return 'suspicious', {
            'suspicion_score': suspicion_score,
            'reasons': suspicion_reasons[:5]  # Limit to 5 examples
        }
    
    # Case 1: Valid headers
    return 'valid', {}


def get_column_samples(df: pd.DataFrame, n_samples: int = 5) -> List[Dict]:
    """
    Get sample values for each column.
    
    Returns:
        List of {column_index, column_name, samples}
    """
    samples = []
    
    for idx, col in enumerate(df.columns):
        col_samples = df[col].dropna().head(n_samples).tolist()
        
        samples.append({
            'column_index': idx,
            'column_name': str(col),
            'samples': [str(s) for s in col_samples]
        })
    
    return samples


def apply_user_headers(
    df: pd.DataFrame,
    user_mapping: Dict[int, str],
    treat_first_row_as_data: bool = False
) -> pd.DataFrame:
    """
    Apply user-provided header names.
    
    Args:
        df: Original dataframe
        user_mapping: {column_index: new_name}
        treat_first_row_as_data: If True, re-read file treating all rows as data
    
    Returns:
        DataFrame with corrected headers
    """
    
    if treat_first_row_as_data:
        # Convert first row to data, generate new headers
        new_headers = [f'unnamed_{i}' for i in range(len(df.columns))]
        first_row_dict = df.iloc[0].to_dict()
        
        df.columns = new_headers
        df = pd.concat([pd.DataFrame([first_row_dict]), df], ignore_index=True)
    
    # Apply user-provided names
    new_columns = []
    for idx, col in enumerate(df.columns):
        if idx in user_mapping and user_mapping[idx].strip():
            new_columns.append(user_mapping[idx].strip().lower().replace(' ', '_'))
        else:
            # Keep system-generated name if user didn't provide one
            new_columns.append(str(col))
    
    df.columns = new_columns
    
    return df


def normalize_header_name(name: str) -> str:
    """
    Normalize a header name (lowercase, underscores, etc.)
    """
    return name.strip().lower().replace(' ', '_').replace('-', '_')