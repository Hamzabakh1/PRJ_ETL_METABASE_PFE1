import os
import sys

# Ensure project root is on sys.path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.errors import ProgrammingError
from infra.config import get_snowflake_conn, load_config
from infra.constants import TABLE_KEYS, DATE_COLS

def generate_merge_sql(schema, target, temp, keys, cols):
    cond = ' AND '.join(f"t.{k}=s.{k}" for k in keys)
    upd = ', '.join(f"{col}=s.{col}" for col in cols if col not in keys)
    cols_list = ','.join(cols)
    vals = ','.join(f's.{col}' for col in cols)
    return f"""
MERGE INTO {schema}.{target} t
USING {schema}.{temp} s
ON {cond}
WHEN MATCHED THEN UPDATE SET {upd}
WHEN NOT MATCHED THEN INSERT ({cols_list}) VALUES ({vals});
"""

def convert_dates_to_snowflake_format(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Convert various date formats to Snowflake-compatible YYYY-MM-DD format.
    Handles large timestamp numbers and various date formats.
    """
    # Get configured date columns for this table
    date_cols = DATE_COLS.get(table_name.upper(), [])
    if isinstance(date_cols, str):
        date_cols = [date_cols]
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Find all potential date columns
    potential_date_cols = set()
    
    # Add configured date columns
    for col in date_cols:
        if col in df_copy.columns:
            potential_date_cols.add(col)
    
    # Add columns that look like dates
    for col in df_copy.columns:
        col_lower = col.lower()
        if (col_lower.startswith('date') or 
            'date' in col_lower or 
            col_lower.endswith('_date') or
            'embauche' in col_lower or
            'sortie' in col_lower or
            'debut' in col_lower or
            'fin' in col_lower):
            potential_date_cols.add(col)
    
    print(f"   🗓️  Converting date columns for {table_name}: {list(potential_date_cols)}")
    
    for col in potential_date_cols:
        if col not in df_copy.columns:
            continue
            
        print(f"      🔍 Processing column: {col}")
        
        try:
            # Get sample value to understand format
            non_null_values = df_copy[col].dropna()
            if non_null_values.empty:
                print(f"      ⚠️  Column {col} is empty, skipping")
                df_copy[col] = None
                continue
                
            sample_value = non_null_values.iloc[0]
            print(f"      📊 Sample value: {sample_value} (type: {type(sample_value)})")
            
            # Additional cleaning for any remaining nan strings
            df_copy[col] = df_copy[col].replace(['nan', 'NaN', 'NaT', 'nat', 'None', 'null'], None)
            
            # Convert based on the format detected
            if pd.api.types.is_numeric_dtype(df_copy[col]):
                # Handle numeric dates
                if isinstance(sample_value, (int, float)) and sample_value > 0:
                    if sample_value > 1e15:  # Very large timestamp (nanoseconds or weird format)
                        print(f"      🔢 Very large timestamp detected, trying different conversions...")
                        # These huge numbers might be in a special format, try different approaches
                        try:
                            # Try treating as nanoseconds since epoch
                            df_copy[col] = pd.to_datetime(df_copy[col], unit='ns', errors='coerce')
                        except:
                            try:
                                # Try dividing by large factor
                                df_copy[col] = pd.to_datetime(df_copy[col] / 1000000, unit='s', errors='coerce')
                            except:
                                # Last resort: treat as string and convert
                                df_copy[col] = pd.to_datetime(df_copy[col].astype(str), errors='coerce')
                    elif sample_value > 1e12:  # Milliseconds
                        print(f"      ⏰ Milliseconds timestamp")
                        df_copy[col] = pd.to_datetime(df_copy[col], unit='ms', errors='coerce')
                    elif sample_value > 1e9:  # Seconds
                        print(f"      ⏰ Seconds timestamp")
                        df_copy[col] = pd.to_datetime(df_copy[col], unit='s', errors='coerce')
                    elif 19000000 <= sample_value <= 21001231:  # YYYYMMDD format
                        print(f"      📅 YYYYMMDD format")
                        df_copy[col] = pd.to_datetime(df_copy[col].astype(str), format='%Y%m%d', errors='coerce')
                    else:
                        # Try generic conversion
                        df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                else:
                    # Zero or negative, set to null
                    df_copy[col] = pd.NaT
            else:
                # String or other format
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
            
            # Convert to YYYY-MM-DD string format for Snowflake
            # Important: ensure it's definitely a string, not datetime
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d').astype(str)
            
            # Replace all possible NaT representations with None for SQL NULL
            df_copy[col] = df_copy[col].replace(['NaT', 'NaTT', 'nat', 'nan', 'NaN', 'None', 'null'], None)
            
            # Additional safety check - replace any remaining nan strings
            mask = df_copy[col].astype(str).str.lower().str.contains('nan|nat|none|null', na=False)
            df_copy.loc[mask, col] = None
            
            # Count successful conversions
            valid_dates = df_copy[col].notna().sum()
            print(f"      ✅ Successfully converted {valid_dates}/{len(df_copy)} values in {col}")
            
        except Exception as e:
            print(f"      ❌ Error converting {col}: {e}")
            # If conversion fails completely, leave original values
            continue
    
    return df_copy

def load_data(df: pd.DataFrame, client: str, mode: str='full'):
    """
    Load DataFrame into Snowflake table.
    
    Args:
        df: DataFrame to load
        client: Client name for configuration
        mode: 'full' or 'incremental'
    """
    if df.empty:
        print(f"❗ Empty DataFrame: skipping {client}")
        return

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    
    # AGGRESSIVE NaN cleaning for Snowflake compatibility
    print(f"   🧹 AGGRESSIVE NaN cleaning for Snowflake...")
    
    original_shape = df.shape
    
    # Create a proper copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Step 1: Convert DataFrame to object type temporarily to catch all NaN representations
    df_temp = df.astype(str)
    
    # Step 2: Replace all possible NaN string representations
    nan_patterns = [
        'nan', 'NaN', 'NAN', 'Nan',
        'None', 'none', 'NONE', 
        'null', 'NULL', 'Null',
        'na', 'NA', 'N/A', 'n/a',
        'NaT', 'nat', '<NA>', 'NaTT',
        'inf', 'Inf', 'INF', '-inf', '-Inf', '-INF',
        '', ' ', '  ', '   '
    ]
    
    # Apply replacement multiple times to catch nested issues
    for pattern in nan_patterns:
        df_temp = df_temp.replace(pattern, None)
    
    # Additional pass for string representations
    for col in df_temp.columns:
        if df_temp[col].dtype == 'object':  # String columns
            # Replace any cell that contains only variations of nan/null
            mask = df_temp[col].astype(str).str.lower().str.match(r'^(nan|nat|none|null|na|n/a)$', na=False)
            df_temp.loc[mask, col] = None
    
    # Replace string patterns
    for pattern in nan_patterns:
        df_temp = df_temp.replace(pattern, None)
    
    # Step 3: Convert back to appropriate types, ensuring NaN becomes None
    for col in df.columns:
        try:
            original_dtype = df[col].dtype
            
            if 'float' in str(original_dtype) or 'int' in str(original_dtype):
                # For numeric columns
                df.loc[:, col] = pd.to_numeric(df_temp[col], errors='coerce')
                # Convert any remaining NaN to None (using numpy.isfinite instead of pandas)
                df.loc[:, col] = df[col].where(pd.notnull(df[col]) & np.isfinite(df[col]), None)
            else:
                # For non-numeric columns - convert to object first to avoid dtype incompatibility
                if df[col].dtype != 'object':
                    df[col] = df[col].astype('object')
                df.loc[:, col] = df_temp[col]
                df.loc[:, col] = df[col].where(pd.notnull(df[col]), None)
                
            # Count nulls after cleaning
            null_count = df[col].isnull().sum()
            if null_count > 0:
                print(f"      🧹 {col}: {null_count} nulls after cleaning")
                
        except Exception as e:
            print(f"      ⚠️ Warning cleaning {col}: {e}")
            # If cleaning fails, ensure at least no NaN strings
            df.loc[:, col] = df[col].astype(str).replace(nan_patterns, None)
    
    # Step 4: Final safety check - replace any remaining pandas NA with None
    df = df.where(pd.notnull(df), None)
    
    # Verify no inf or nan values remain
    total_nulls = df.isnull().sum().sum()
    print(f"   ✅ AGGRESSIVE cleaning complete: {original_shape} -> {df.shape}")
    print(f"      📊 Total null values: {total_nulls}")
    
    # Additional check for problematic values
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            inf_check = np.isinf(df[col].fillna(0)).any()
            if inf_check:
                print(f"      ⚠️ Still has inf values in {col}, replacing...")
                df[col] = df[col].replace([np.inf, -np.inf], None)

    # Get table name and convert dates
    tbl = df.attrs.get('table')
    if not tbl:
        raise ValueError("DataFrame must have 'table' attribute set")
        
    print(f"   📊 Processing table: {tbl}")
    
    # Debug: Print column information for DIM_PARCELLE
    if tbl.upper() == 'DIM_PARCELLE':
        print(f"   🔍 DEBUG - DataFrame columns ({len(df.columns)}): {list(df.columns)}")
        print(f"   🔍 DEBUG - DataFrame shape: {df.shape}")
    
    df = convert_dates_to_snowflake_format(df, tbl)
    
    tbl_u = tbl.upper()
    
    # Load configuration
    cfg = load_config(client)
    sf_cfg = cfg['snowflake']
    schema = sf_cfg['schema']
    create_replace = cfg.get('etl', {}).get('create_or_replace', False)

    conn = get_snowflake_conn(client)
    cur = conn.cursor()

    temp = f"TEMP_{tbl_u}"

    # Stage data - ensure index is not included as extra column
    df_to_stage = df.reset_index(drop=True)
    write_pandas(conn, df_to_stage, temp, schema=schema, overwrite=True)

    # Load logic
    if mode == 'full':
        if create_replace:
            write_pandas(conn, df, tbl_u, schema=schema, overwrite=True)
        else:
            try:
                # Always try to truncate and insert into existing table
                cur.execute(f"TRUNCATE TABLE {schema}.{tbl_u}")
                cur.execute(f"INSERT INTO {schema}.{tbl_u} SELECT * FROM {schema}.{temp}")
            except ProgrammingError as e:
                if "does not exist" in str(e):
                    raise ProgrammingError(f"Table {schema}.{tbl_u} doesn't exist. Please create the table first using the provided SQL schema.")
                else:
                    raise e
    else:
        keys = TABLE_KEYS.get(tbl_u)
        if not keys:
            raise KeyError(f"No key for {tbl_u}")
        try:
            sql = generate_merge_sql(schema, tbl_u, temp, keys, list(df.columns))
            cur.execute(sql)
        except ProgrammingError:
            write_pandas(conn, df, tbl_u, schema=schema, overwrite=True)

    # Cleanup
    cur.execute(f"DROP TABLE IF EXISTS {schema}.{temp}")
    cur.close()
    conn.close()
    print(f"✅ Loaded {tbl_u} ({mode})")
