from services.llm_service import LLMService
from services.s3_service import upload_to_s3
import streamlit as st
import pandas as pd
import os
from io import BytesIO

def create_pivot(df, rows, values=None, filter=None, aggfunc='sum'):
    try:
        # Create a copy to avoid modifying the original
        df_filtered = df.copy()
        
        # Handle filters - simple approach treating all columns the same
        if filter:
            for column, value in filter.items():
                if isinstance(value, list):
                    # For multiselect filters - convert everything to string for comparison
                    df_filtered = df_filtered[df_filtered[column].astype(str).isin([str(v) for v in value])]
                else:
                    # For single value filters
                    df_filtered = df_filtered[df_filtered[column] == value]

        # Create pivot table - no special handling for any column types
        pivot_table = pd.pivot_table(
            df_filtered,
            values=values,
            index=rows,
            columns=[],
            aggfunc=aggfunc,
            fill_value=0
        )
    except Exception as e:
        print(f'pivot creation error {e}')
        
    return pivot_table.reset_index()

def read_excel_file(file_buffer, sheet_name=None):
    read_options = {
        'dtype': object,  
    }

    if sheet_name:
        return pd.read_excel(file_buffer, sheet_name=sheet_name, **read_options)
    
    # First, read sheet names
    xls = pd.ExcelFile(file_buffer)
    sheet_names = xls.sheet_names
    
    if len(sheet_names) == 1:
        df=  pd.read_excel(file_buffer, sheet_name=sheet_names[0], **read_options)
    
    else:
        selected_sheet = st.selectbox("Select sheet:", sheet_names)
        df= pd.read_excel(file_buffer, sheet_name=selected_sheet, **read_options)

    return _coerce_column_types(df)


def _coerce_column_types(df):
    """Convert columns to appropriate data types where possible."""
    df_processed = df.copy()
    
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df_processed[col] = df[col].astype(str)
            continue
        # Skip columns that are already non-object types
        if df[col].dtype != 'object':
            continue
            
        # Try to convert to numeric if it makes sense
        try:
            # Check if all non-null values can be converted to numeric
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            # If we didn't lose any non-null values, convert it
            if numeric_series.notna().sum() == df[col].notna().sum():
                df_processed[col] = numeric_series
            else:
                # Has non-numeric values, keep as string
                df_processed[col] = df[col].astype(str)
        except:
            # Any error, keep as string
            df_processed[col] = df[col].astype(str)
            
    return df_processed


def handle_file_upload(uploaded_file):
    """Orchestrates file upload and processing."""
    if uploaded_file is None:
        return False

    # Check if this is a new upload attempt
    is_new_upload = _check_if_new_upload(uploaded_file.name)
    if not is_new_upload:
        return True  # Already processed

    # Upload to S3 first (all file types)
    if not _upload_file_to_s3(uploaded_file):
        return False

    # Process based on file type
    file_ext = os.path.splitext(uploaded_file.name)[1].lower()
    if file_ext in [".xls", ".xlsx"]:
        success = _process_excel_file(uploaded_file)
    else:
        success = _process_non_excel_file(uploaded_file)

    return success


# --- Smaller Helper Functions ---

def _upload_file_to_s3(uploaded_file):
    """Handles S3 upload with progress tracking."""
    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text("Uploading file to storage...")

    upload_success = upload_to_s3(uploaded_file)  # Your existing S3 function

    progress_bar.empty()
    status_text.empty()
    return upload_success


def _check_if_new_upload(filename):
    """Checks if this is a new file upload attempt."""
    if "last_uploaded_file" not in st.session_state:
        st.session_state.last_uploaded_file = None

    is_new_upload = st.session_state.last_uploaded_file != filename
    st.session_state.last_uploaded_file = filename

    # Skip if file already exists (unless it's a new upload attempt)
    if filename in st.session_state.datasets:
        if is_new_upload:
            st.info(f"File '{filename}' already uploaded")
        return False  # Not a "new" upload for processing
    return True

def _process_single_sheet_excel(uploaded_file, file_buffer, sheet_name):
    """Process Excel files with single sheet"""
    try:
        df = pd.read_excel(file_buffer, sheet_name=sheet_name)
        
        # Create dataset entry
        dataset_key = uploaded_file.name
        st.session_state.datasets[dataset_key] = {
            'df': df,
            'filename': uploaded_file.name,
            'sheet_name': sheet_name,
            'columns': list(df.columns),
            'numeric_columns': df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        }
        
        # Generate description
        try:
            st.session_state.datasets[dataset_key]['description'] = generate_data_description(df)
        except Exception:
            st.session_state.datasets[dataset_key]['description'] = "Dataset information unavailable."
        
        st.session_state.active_dataset = dataset_key
        return True
        
    except Exception as e:
        st.error(f"Error processing Excel sheet: {str(e)}")
        return False

def _process_excel_file(uploaded_file):
    """Handles Excel files (single/multi-sheet)."""
    file_buffer = BytesIO(uploaded_file.getvalue())
    xls = pd.ExcelFile(file_buffer)
    sheet_names = xls.sheet_names

    if len(sheet_names) == 1:
        success = _process_single_sheet_excel(uploaded_file, file_buffer, sheet_names[0])
        if success:
            st.success(f"Excel file '{uploaded_file.name}' processed successfully!")
        return success
    else:
        if uploaded_file.name not in st.session_state.datasets:
            st.session_state.datasets[uploaded_file.name] = {
                'filename': uploaded_file.name,
                'sheet_names': sheet_names,
                'file_buffer': uploaded_file.getvalue(),
                'processed_sheets': [],
                'pending_sheet_selection': True
            }
        
        st.success(f"Excel file uploaded with {len(sheet_names)} sheets. Please select a sheet.")
        return True



def _process_non_excel_file(uploaded_file):
    """Handles CSV/other non-Excel files."""
    file_buffer = BytesIO(uploaded_file.getvalue())
    return _process_file_data(uploaded_file, file_buffer)

def _process_uploaded_file(uploaded_file, file_buffer=None):
    """Initial file processing and S3 upload"""
    if 'original_df' not in st.session_state:
        # Only read from uploaded file and store to S3 on initial upload
        try:
            if file_buffer is None:
                file_buffer = BytesIO(uploaded_file.getvalue())
            
            file_ext = os.path.splitext(uploaded_file.name)[1].lower()
            
            if file_ext == ".csv":
                df = pd.read_csv(file_buffer, thousands=",", low_memory=False, dtype=object)
                df = _coerce_column_types(df)
            elif file_ext in [".xls", ".xlsx"]:
                df = read_excel_file(file_buffer)
            else:
                raise ValueError("Unsupported file format")
            
            # Store in session state
            st.session_state.original_df = df
            st.session_state.file_name = uploaded_file.name
            st.session_state.columns = list(df.columns)
            st.session_state.numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            
            return df, None
            
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            return None, None
    
    return st.session_state.original_df, None


def _process_file_data(uploaded_file, file_buffer):
    """Processes file data and stores results."""
    with st.spinner("Processing data..."):
        df, _ = _process_uploaded_file(uploaded_file, file_buffer)
        if df is None:
            st.error("Failed to process file data")
            return False

        _finalise_dataset_storage(uploaded_file.name, df)
        st.success(f"File {uploaded_file.name} uploaded successfully!")
        return True


def _finalise_dataset_storage(filename, df):
    """Stores processed data in session state."""
    st.session_state.datasets[filename] = {
        'df': df,
        'filename': filename,
        'columns': list(df.columns),
        'numeric_columns': df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    }
    st.session_state.active_dataset = filename

    # Optional: Generate AI description if LLM is available

    try:
        st.session_state.datasets[filename]['description'] = generate_data_description(df)
    except Exception:
        st.session_state.datasets[filename]['description'] = "Dataset information unavailable."


def generate_data_description(df):
    """Generate a simple description of the dataframe using LLM"""
    try:
        
        # Get descriptive statistics for numeric columns
        describe_str = df.describe().to_string()
        
        # Sample data
        sample_str = df.head(5).to_string()
        
        # Construct prompt
        prompt = f"""
        You're analyzing a financial dataset with the following information:
        
        DataFrame Info:
        {df.info()}
        
        Statistical Summary (numeric columns):
        {describe_str}
        
        Sample Data (first 5 rows):
        {sample_str}
        
        Please provide a concise title for this dataset, followed by a brief description 
        of what this dataset contains. Include what appear to be the main metrics, columns, information. 

        Do not make any judgements/observations of your own. Stay objective. No need for insights.
        
        Keep your response under 250 words and make it helpful for a financial analyst.
        """
        
        llm = LLMService.get_instance()
        response = llm(prompt)
        return response.strip()
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in generate_data_description: {error_details}")
        return f"Error generating description. Please try again or manually enter a description."
    

def join_datasets(datasets_dict, dataset_keys):
    """
    Join multiple datasets by concatenating them (union)
    
    """
    if not dataset_keys or len(dataset_keys) < 2:
        return None, None, "Please select at least two datasets to join"
    
    # Get dataframes
    dfs = []
    first_dataset = None
    
    for key in dataset_keys:
        if key not in datasets_dict:
            return None, None, f"Dataset {key} not found"
        
        dataset = datasets_dict[key]
        if 'df' not in dataset:
            return None, None, f"Dataset {key} does not contain dataframe data"
        
        if first_dataset is None:
            first_dataset = dataset
        
        df = dataset['df']
        dfs.append(df)
    
    # Check if all dataframes have the same columns
    first_columns = set(dfs[0].columns)
    for i, df in enumerate(dfs[1:], 1):
        df_columns = set(df.columns)
        if df_columns != first_columns:
            missing = first_columns - df_columns
            extra = df_columns - first_columns
            
            message = f"Column mismatch in dataset {dataset_keys[i]}."
            if missing:
                message += f" Missing columns: {', '.join(str(c) for c in missing)}."
            if extra:
                message += f" Extra columns: {', '.join(str(c) for c in extra)}."
                
            return None, None, message

    
    # All columns match, perform union
    try:
        joined_df = pd.concat(dfs, ignore_index=True)
        return joined_df, first_dataset, "Datasets joined successfully"
    except Exception as e:
        return None, None, f"Error joining datasets: {str(e)}"