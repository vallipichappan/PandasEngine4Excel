from io import BytesIO
import re
import time
import pandas as pd
import streamlit as st
import logging
from services.data_service import DataService
from ui.data_ui import DataProcessingUI
from services.query_service import QueryService
from services.llm_service import LLMService
from ui.query_ui import QueryUI
from session_management import check_session_timeout, clean_up_session, initialise_session, session_info_widget, update_session_activity
from utils import initialise_extended_session_state
from auth import authenticate_user
from data_processing import (
    generate_data_description,
    create_pivot, 
    handle_file_upload
)

from llama_index.core import Settings
from telemetry.setup_telemetry import setupLogging, traceFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

setupLogging()

from dotenv import load_dotenv

# Load the .env file
load_dotenv()

@traceFunction(st.session_state.get("username", ""))

def main():
    st.set_page_config(page_title="Excelsior", layout="wide")
    initialise_session()
    session_timeout_minutes = 60  # Adjust as needed
    if check_session_timeout(timeout_minutes=session_timeout_minutes):
        st.warning("Your previous session expired due to inactivity. Starting a new session.")
        time.sleep(2)
        st.rerun()
    
    # Update activity timestamp on each page load
    update_session_activity()
    
    initialise_extended_session_state()
    
    # Top navigation bar with login/logout in top right
    col_title, col_login = st.columns([5, 1])
    
    with col_title:
        st.title("Excelsior")
    
    with col_login:
        if st.session_state.authenticated:
            st.write(f"👤 {st.session_state.username}")
            if st.button("Logout"):
                st.session_state.authenticated = False
                st.session_state.token = None
                st.session_state.username = None
                clean_up_session()
                st.rerun()
    
    if not st.session_state.authenticated:
        # Center the login form
        _, login_col, _ = st.columns([1, 2, 1])
        with login_col:
            handle_authentication()
    else:
        session_info_widget()
        # Use sidebar for data management
        with st.sidebar:
            st.header("Data Management")
            data_tab1, data_tab2, data_tab3 = st.tabs(["Upload Data", "Join Datasets", "Customise Your Data"])
            
            with data_tab1:
                show_upload_page()

            with data_tab2:
                show_join_page()
            
            with data_tab3:
                show_pivot_page()
        
        # Main area for query
        st.header("Query Data")
        query_service = QueryService(st.session_state)
        query_ui = QueryUI(query_service)
        query_ui.show_query_page()

    
def handle_authentication():
    st.subheader("Login")
    with st.form(key="login_form"):
        user_id = st.text_input("User ID")
        password = st.text_input("Password", type="password")
        submit_button = st.form_submit_button("Login")
        
        if submit_button:
    
            token = authenticate_user(user_id, password)
            if token:
                st.success("Login successful!")
                st.session_state.authenticated = True
                st.session_state.token = token
                st.session_state.username = user_id

                LLMService.initialize()
                st.rerun()
            else:
                st.error("Login failed. Please try again.")


def show_upload_page():
    data_service = DataService(st.session_state)
    data_ui = DataProcessingUI(data_service)
    data_ui.show_upload_page()


def show_join_page():
    if not st.session_state.datasets:
        st.info("Please upload data files first in the Data Upload section")
        return
    
    st.subheader("Join Datasets")
    st.write("Join multiple datasets with matching column structures")
    
    data_service = DataService(st.session_state)
    dataset_options = {k: v['filename'] for k, v in data_service.get_processed_datasets().items()}
    
    if not dataset_options:
        st.info("No processed datasets available. Please select sheets for your Excel files or upload CSV files.")
        return
    
    # Select datasets to join
    selected_datasets = st.multiselect(
        "Select datasets to join",
        options=list(dataset_options.keys()),
        format_func=lambda x: dataset_options[x],
        key="join_dataset_selector"
    )
    
    if len(selected_datasets) < 2:
        st.info("Please select at least two datasets to join")
        return
    
    # Preview the datasets
    st.markdown("### Selected Datasets Preview")
    for dataset_key in selected_datasets:
        with st.expander(f"Preview: {dataset_options[dataset_key]}"):
            df = st.session_state.datasets[dataset_key]['df']
            st.dataframe(df.head(5), use_container_width=True)
    
    # Join name
    join_name = st.text_input("Joined dataset name", value="Joined Dataset")
    
    if st.button("Join Datasets", type="primary"):
        with st.spinner("Joining datasets..."):
            
            join_key, message = data_service.join_datasets(selected_datasets, join_name)
            
            if join_key is None:
                st.warning(message)
            else:
                # Create a new dataset entry for the joined data
                joined_df = st.session_state.datasets[join_key]['df']
                st.success(f"✅ {message}. Created joined dataset '{join_name}' with {len(joined_df)} rows.")

                st.subheader("Joined Dataset Preview")
                st.dataframe(joined_df.head(10), use_container_width=True)
                
                

def is_date_string(s):
    """Checks if string can be parsed as date. Returns True if parseable."""
    try:
        pd.to_datetime(s, errors='raise')
        return True
    except Exception:
        return False
    
def contains_month_name(s):
    if not re.search(r'[a-zA-Z]', s):
        return True
    
    months = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", 
        "aug", "sep", "oct", "nov", "dec",
        "january", "february", "march", "april", "june", 
        "july", "august", "september", "october", "november", "december"
    ]
    pattern = r'(' + '|'.join(months) + r')'
    return bool(re.search(pattern, s, re.IGNORECASE))


def possible_date_columns(df):
    patterns = re.compile(r'(date|time|year|yr|day|month|dt)', re.I)
    possible = []
    for col in df.columns:
        name_flag = bool(patterns.search(col))
        value_flag = False
        month_flag = False
        sample_values = df[col].dropna().astype(str).head(5)
        # Try parsing a sample of values, including those like "10 March 2024" or "10 mar"
        parse_success = sample_values.apply(is_date_string).sum() 
        month_flag = sample_values.apply(contains_month_name).any()
        if parse_success >= 3:
            value_flag = True
        if (name_flag and month_flag) or value_flag:
            possible.append(col)
    return possible

def show_pivot_page():
    if not st.session_state.datasets:
        st.info("Please upload data files first in the Data Upload section")
        return
    
    
    data_service = DataService(st.session_state)
    dataset_options = {k: v['filename'] for k, v in data_service.get_processed_datasets().items()}
    
    

    if not dataset_options:
        st.info("No processed datasets available. Please select sheets for your Excel files or upload CSV files.")
        return
        
    selected_dataset = st.selectbox(
        "Select dataset to pivot",
        options=list(dataset_options.keys()),
        format_func=lambda x: dataset_options[x],
        key="pivot_dataset_selector"
    )
    
    # Check if dataset selection has changed
    dataset_changed = st.session_state.get('active_dataset') != selected_dataset
    st.session_state.active_dataset = selected_dataset    
    
    dataset = st.session_state.datasets[selected_dataset]
    if 'df' not in dataset:
        st.warning("Dataset structure is incomplete. Please reload or re-upload the file.")
        return
    
    df = dataset['df']

    possible_date_cols = possible_date_columns(df)


    all_columns = [str(col).strip() for col in dataset['columns'] if col is not None and str(col).strip()]
    date_columns = possible_date_cols
    numeric_columns = [str(col).strip() for col in dataset['numeric_columns'] if col is not None and str(col).strip()]
    numeric_columns = [col for col in numeric_columns if col not in date_columns]

    non_numeric_columns = [col for col in all_columns if col not in numeric_columns and col not in date_columns]


    default_rows = ['Work Type', 'Remapped country', 'Platform Unit', 'Platform Index', 'GL Groups']
    available_default_rows = [col for col in default_rows if col in non_numeric_columns]
    
    # Reset temp pivot selections if dataset changed
    if dataset_changed:
        st.session_state.temp_pivot_rows = available_default_rows
        st.session_state.temp_pivot_date_rows = []
        st.session_state.temp_pivot_vals = numeric_columns
        st.session_state.temp_pivot_aggfunc = 'sum'
        st.session_state.temp_pivot_filters = {}
        
        # Force a rerun to update the UI with new selections
        st.rerun()
    else:
        # Initialize if not present (first time)
        if 'temp_pivot_rows' not in st.session_state:
            st.session_state.temp_pivot_rows = available_default_rows
        if 'temp_pivot_date_rows' not in st.session_state:
            st.session_state.temp_pivot_date_rows = []
        if 'temp_pivot_vals' not in st.session_state:
            st.session_state.temp_pivot_vals = numeric_columns
        if 'temp_pivot_aggfunc' not in st.session_state:
            st.session_state.temp_pivot_aggfunc = 'sum'
        if 'temp_pivot_filters' not in st.session_state:
            st.session_state.temp_pivot_filters = {}
    
    # --- Select Rows ---
    with st.expander("Select rows (Group By)", expanded=False):
        possible_row_fields = non_numeric_columns
        all_rows_selected = set(non_numeric_columns) <= set(st.session_state.temp_pivot_rows)
        if st.button("Unselect All Rows" if all_rows_selected else "Select All Rows", key="toggle_rows"):
            if all_rows_selected:
                st.session_state.temp_pivot_rows = []
            else:
                st.session_state.temp_pivot_rows = possible_row_fields.copy()
            st.rerun()

        
        selected_rows = []
        for i, col in enumerate(possible_row_fields):
            # Asegurar que la etiqueta sea válida
            safe_label = str(col) if col is not None else f"Column_{i}"
            if len(safe_label.strip()) == 0:
                safe_label = f"Column_{i}"
                
            try:
                checked = col in st.session_state.temp_pivot_rows
                if st.checkbox(safe_label, value=checked, key=f"row_{i}_{col}"):
                    selected_rows.append(col)
            except Exception as e:
                st.warning(f"Skipping column with invalid name: {repr(col)}")
                continue
        
        # Update session state on change
        if selected_rows != st.session_state.temp_pivot_rows:
            st.session_state.temp_pivot_rows = selected_rows

     # --- Select Date Rows (New Section) ---
    with st.expander("Select Date/Time Rows (Group By)", expanded=False):
        all_date_rows_selected = set(date_columns) <= set(st.session_state.temp_pivot_date_rows)
        if st.button("Unselect All Date Rows" if all_date_rows_selected else "Select All Date Rows", key="toggle_date_rows"):
            if all_date_rows_selected:
                st.session_state.temp_pivot_date_rows = []
            else:
                st.session_state.temp_pivot_date_rows = date_columns.copy()
            st.rerun()

        selected_date_rows = []
        for i, col in enumerate(date_columns):
            safe_label = str(col) if col is not None else f"DateColumn_{i}"
            if len(safe_label.strip()) == 0:
                safe_label = f"DateColumn_{i}"
                
            try:
                checked = col in st.session_state.temp_pivot_date_rows
                if st.checkbox(safe_label, value=checked, key=f"date_row_{i}_{col}"):
                    selected_date_rows.append(col)
            except Exception as e:
                st.warning(f"Skipping date column with invalid name: {repr(col)}")
                continue
        
        # Update session state on change
        if selected_date_rows != st.session_state.temp_pivot_date_rows:
            st.session_state.temp_pivot_date_rows = selected_date_rows
        


    # --- Select Numeric Columns ---  
    all_selected_rows = st.session_state.temp_pivot_rows + st.session_state.temp_pivot_date_rows
    available_value_fields = [col for col in numeric_columns if col not in all_selected_rows]
    
    with st.expander("Select Numeric Columns (Aggregate)", expanded=False):
        all_vals_selected = set(available_value_fields) <= set(st.session_state.temp_pivot_vals)
        if st.button("Unselect All Numeric Columns" if all_vals_selected else "Select All Numeric Columns", key="toggle_vals"):
            if all_vals_selected:
                st.session_state.temp_pivot_vals = []
            else:
                st.session_state.temp_pivot_vals = available_value_fields.copy()
            st.rerun()
        selected_values = []
        for i, col in enumerate(available_value_fields):
            # Asegurar que la etiqueta sea válida
            safe_label = str(col) if col else f"Numeric_Column_{i}"
            if len(safe_label.strip()) == 0:
                safe_label = f"Numeric_Column_{i}"
                
            try:
                checked = col in st.session_state.temp_pivot_vals
                if st.checkbox(safe_label, value=checked, key=f"val_{i}_{col}"):
                    selected_values.append(col)
            except Exception as e:
                st.warning(f"Skipping numeric column with invalid name: {repr(col)}")
                continue
        
        # Update session state
        if set(selected_values) != set(st.session_state.temp_pivot_vals):
            st.session_state.temp_pivot_vals = selected_values
    
        
    aggfunc_options = ['sum', 'min', 'max', 'mean', 'median', 'count', 'std', 'var']
    aggfunc = st.selectbox(
        "Aggregation function", 
        aggfunc_options, 
        index=aggfunc_options.index(st.session_state.temp_pivot_aggfunc),
        key="aggfunc_select",
        on_change=lambda: setattr(st.session_state, 'temp_pivot_aggfunc', st.session_state.aggfunc_select)
    )
    

    st.markdown("### Filters (optional)")
    filters = {}
    valid_cols = non_numeric_columns + date_columns
    valid_filter_columns = [col for col in valid_cols if col is not None and len(str(col).strip()) > 0]
    for col in valid_filter_columns:
        print(col)
    selected_filter_columns = st.multiselect("Select filter columns", valid_filter_columns)

    for filter_col in selected_filter_columns:
        try:            
            unique_vals = df[filter_col].dropna().unique().tolist()
            unique_vals = [str(val) for val in unique_vals if val is not None]
            
            default_vals = st.session_state.temp_pivot_filters.get(filter_col, [])
            selected_vals = st.multiselect(
                f"Filter values for {filter_col}", 
                unique_vals,
                default=default_vals,
                key=f"filter_vals_{filter_col}"
            )
            if selected_vals:
                filters[filter_col] = selected_vals
                st.session_state.temp_pivot_filters[filter_col] = selected_vals
        except Exception as e:
            st.warning(f"Error processing filter for column {filter_col}: {str(e)}")


    
    # Show live preview of pivot configuration
    st.subheader("Pivot Preview")
    
    if all_selected_rows:
        try:
            with st.spinner("Generating preview..."):
                preview_pivot = create_pivot(df, all_selected_rows, selected_values, filters, aggfunc)
                st.dataframe(preview_pivot.head(10), use_container_width=True)
        except Exception as e:
            # st.error(f"Preview error: {str(e)}")
            print(f"Preview error: {str(e)}")
            st.info("Adjust your selections to create a valid pivot table")
    else:
        st.info("Select at least one row to see the preview")
    
    # Create the actual pivot table
    st.divider()
    pivot_name = st.text_input("Pivot table name", value=f"Pivot {len(st.session_state.pivot_tables) + 1}")

    create_button = st.button("Create Pivot Table", type="primary", use_container_width=True)

    if create_button:
        if not all_selected_rows:
            st.warning("Please select at least one row or column")
        else:
            with st.spinner("Creating pivot table..."):
                try:
                    pivot_result = create_pivot(df, all_selected_rows, selected_values, filters, aggfunc)
                    
                    # Create unique key for this pivot
                    pivot_key = f"{st.session_state.active_dataset}_{pivot_name}"
                    
                    # Store pivot table
                    st.session_state.pivot_tables[pivot_key] = {
                        'result': pivot_result,
                        'name': pivot_name,
                        'source_dataset': st.session_state.active_dataset,
                        'config': {
                            'rows': selected_rows,
                            'date_rows': selected_date_rows,
                            'values': selected_values,
                            'filter': filters,
                            'aggfunc': aggfunc
                        }
                    }
                    
                    st.session_state.active_pivot = pivot_key
                    st.success(f"✅ Pivot table '{pivot_name}' created successfully!")
                except Exception as e:
                    st.error(f"Error creating pivot table: {str(e)}")
    
    # Show existing pivot tables
    st.subheader("Existing Pivot Tables")
    
    # Filter pivots for current dataset
    current_dataset_pivots = {k: v for k, v in st.session_state.pivot_tables.items() 
                             if v['source_dataset'] == st.session_state.active_dataset}
    
    if not current_dataset_pivots:
        st.info("No pivot tables created for this dataset yet")
    else:
        for pivot_key, pivot_data in current_dataset_pivots.items():
            # Create a layout with title and delete button side by side
            col1, col2 = st.columns([5, 1])
            
            with col1:
                st.write(f"**{pivot_data['name']}**")
            
            with col2:
                # Move delete button to the right
                if st.button("🗑️ Delete", key=f"delete_{pivot_key}"):
                    del st.session_state.pivot_tables[pivot_key]
                    st.rerun()
            
            # Display the pivot table
            # st.dataframe(pivot_data['result'], use_container_width=True)
            with st.expander("View data"):
                st.dataframe(pivot_data['result'], use_container_width=True)
                    
                    # Show configuration details in a cleaner format
            with st.expander("Configuration Details"):
                config = pivot_data['config']
                
                # Format rows and columns more cleanly
                if config['rows']:
                    st.write("**Rows:** " + ", ".join(config['rows']))
                
                if config['date_rows']:
                    st.write("**Date/Time Rows:** " + ", ".join(config['date_rows']))
                
                if config['values']:
                    st.write("**Values:** " + ", ".join(config['values']))
                
                st.write(f"**Aggregation:** {config['aggfunc']}")
                
                # Format filters more cleanly
                if config['filter']:
                    filter_details = []
                    for k, v in config['filter'].items():
                        if v:  # Check if filter value is not None
                            filter_details.append(f"{k} = {v}")
                    
                    if filter_details:
                        st.write("**Filters:**")
                        for detail in filter_details:
                            st.write(f"- {detail}")

    

if __name__ == "__main__":
    main()