from io import BytesIO
import pandas as pd
import streamlit as st
from data_processing import _coerce_column_types, generate_data_description, handle_file_upload
from data_processing import join_datasets as process_join
class DataService:
    def __init__(self, session_state):
        self.session_state = session_state
    
    def upload_file(self, uploaded_file):
        """Handle file upload with proper validation"""
        try: 
            return handle_file_upload(uploaded_file)
        except Exception as e:
            print(f'Error in upload_file {e}')

    
    def get_excel_files_needing_processing(self):
        """Get Excel files that need sheet processing"""
        excel_files = []
        try:
            for key, dataset in self.session_state.datasets.items():
                if ('sheet_names' in dataset and 
                    (dataset.get('pending_sheet_selection', False) or 
                    len(dataset.get('processed_sheets', [])) < len(dataset.get('sheet_names', [])))):
                    excel_files.append(key)
            return excel_files
        except Exception as e:
            print(f'Error in get_excel_files_needing_processing {e}')
    
    def process_excel_sheet(self, excel_key, sheet_name):
        """Process a specific sheet from an Excel file"""
        try:
            dataset = self.session_state.datasets[excel_key]
            file_buffer = BytesIO(dataset['file_buffer'])
            df = pd.read_excel(file_buffer, sheet_name=sheet_name, dtype=object)
            df = _coerce_column_types(df)
            
            # Create new dataset entry for this sheet
            sheet_key = f"{excel_key}_{sheet_name}"
            self.session_state.datasets[sheet_key] = {
                'df': df,
                'filename': f"{dataset['filename']} (Sheet: {sheet_name})",
                'source_file': excel_key,
                'sheet_name': sheet_name,
                'columns': list(df.columns),
                'numeric_columns': df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            }
            
            # Update processed sheets
            if 'processed_sheets' not in dataset:
                dataset['processed_sheets'] = []
            dataset['processed_sheets'].append(sheet_name)
            
            # Remove pending flag if all sheets processed
            if len(dataset['processed_sheets']) == len(dataset['sheet_names']):
                dataset['pending_sheet_selection'] = False
            
            # Generate description only once
            if 'description' not in self.session_state.datasets[sheet_key]:
                try:
                    self.session_state.datasets[sheet_key]['description'] = generate_data_description(df)
                except Exception:
                    self.session_state.datasets[sheet_key]['description'] = "Dataset information unavailable."
            
            self.session_state.active_dataset = sheet_key
            return sheet_key
        
        except Exception as e:
            print(f'Error in process_excel_sheet {e}')
    
    def get_processed_datasets(self):
        """Get datasets that are fully processed (not pending sheet selection)"""
        processed = {}
        try:
            for key, dataset in self.session_state.datasets.items():
                if ('df' in dataset and 
                    not dataset.get('pending_sheet_selection', False)):
                    processed[key] = dataset
            return processed
        except Exception as e:
            print(f'Error in get_processed_datasets {e}')
    
    def join_datasets(self, dataset_keys, join_name):
        """Join multiple datasets with matching columns"""
        
        try:
            joined_df, first_dataset, message = process_join(self.session_state.datasets, dataset_keys)
            
            if joined_df is None:
                return None, message
            
            # Create a new dataset entry for the joined data
            join_key = f"joined_{len([k for k in self.session_state.datasets.keys() if k.startswith('joined_')])}"
            
            # Reuse metadata from the first dataset
            self.session_state.datasets[join_key] = {
                'df': joined_df,
                'filename': join_name,
                'source_datasets': dataset_keys,
                'columns': first_dataset['columns'],
                'numeric_columns': first_dataset['numeric_columns'],
                'description': first_dataset.get('description', f"Joined dataset created from {len(dataset_keys)} source datasets.")
            }
            
            self.session_state.active_dataset = join_key
            return join_key, message
        except Exception as e:
            print(f'Error in join_datasets DataService {e}')