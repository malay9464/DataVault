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
            'reasons': suspicion_reasons[:10]  # Limit to 5 examples
        }
    
    # Case 1: Valid headers
    return 'valid', {}

def get_column_samples(df: pd.DataFrame, n_samples: int = 10) -> List[Dict]:
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
        # CRITICAL FIX: Capture first row VALUES before column manipulation
        # This prevents data values from leaking into column names
        first_row_values = df.iloc[0].values.tolist()
        
        # Generate system headers
        new_headers = [f'unnamed_{i}' for i in range(len(df.columns))]
        df.columns = new_headers
        
        # Reconstruct first row dict with correct column names
        first_row_dict = {new_headers[i]: first_row_values[i] for i in range(len(new_headers))}
        
        # Add first row back as data
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

def infer_semantic_role(column_name: str) -> Optional[str]:
    """
    Infer semantic role from user-provided column name.
    Uses fuzzy matching with separator removal.
    
    Returns:
        'email', 'phone', 'name', or None
    """
    if not column_name:
        return None
    
    # Remove all common separators for matching
    normalized = column_name.lower()
    cleaned = normalized.replace('-', '').replace('_', '').replace(' ', '').replace('.', '')
    
    # Email patterns (check most specific first)
    email_patterns = [
        'emailaddress', 'emailid', 'email', 'mail', 'e-mail', 'e_mail'
    ]
    if any(pattern in cleaned for pattern in email_patterns):
        return 'email'
    
    # Phone patterns (check most specific first)
    phone_patterns = [
        'phonenumber', 'phoneno', 'mobilenumber', 'mobileno', 
        'contactnumber', 'contactno', 'cellnumber', 'cellno',
        'phone', 'mobile', 'contact', 'cell'
    ]
    if any(pattern in cleaned for pattern in phone_patterns):
        return 'phone'
    
    # Name patterns
    name_patterns = [
        'fullname', 'firstname', 'lastname', 'customername', 
        'clientname', 'username', 'name'
    ]
    if any(pattern in cleaned for pattern in name_patterns):
        return 'name'
    
    return None


def build_semantic_roles(user_mapping: Dict[int, str]) -> Dict[int, str]:
    """
    Build semantic role mapping from user-provided column names.
    
    Args:
        user_mapping: {column_index: column_name}
        
    Returns:
        {column_index: semantic_role}
        
    Example:
        Input: {0: "Full Name", 2: "E-mail", 5: "Phone_No"}
        Output: {0: "name", 2: "email", 5: "phone"}
    """
    semantic_roles = {}
    
    for idx, col_name in user_mapping.items():
        role = infer_semantic_role(col_name)
        if role:
            semantic_roles[idx] = role
    
    return semantic_roles


def normalize_column_name(name: str) -> str:
    """
    Normalize column name for storage.
    Converts to lowercase and replaces separators with underscores.
    
    Examples:
        "E-mail" → "e_mail"
        "Phone No" → "phone_no"
        "Full.Name" → "full_name"
    """
    if not name:
        return name
    
    normalized = name.strip().lower()
    # Replace all common separators with underscore
    for sep in ['-', '.', ' ']:
        normalized = normalized.replace(sep, '_')
    
    # Remove consecutive underscores
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    return normalized