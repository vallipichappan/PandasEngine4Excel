import streamlit as st
from io import BytesIO

class DataProcessingUI:
    def __init__(self, data_service):
        self.data_service = data_service
    
    def show_upload_page(self):
        """Main upload page UI"""
        try:
            uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xls", "xlsx"])
            
            if uploaded_file:
                self.data_service.upload_file(uploaded_file)
            
            self._show_excel_sheet_processing()
            self._show_uploaded_datasets()
        except Exception as e:
            print(f"Error show_upload_page {str(e)}")
    
    def _show_excel_sheet_processing(self):
        """Handle Excel sheet selection UI"""
        try:
            excel_files = self.data_service.get_excel_files_needing_processing()
            
            if not excel_files:
                return
                
            st.subheader("Process Excel Sheets")
            
            # Select Excel file if multiple
            if len(excel_files) > 1:
                selected_excel = st.selectbox(
                    "Select Excel file:", 
                    excel_files,
                    format_func=lambda x: st.session_state.datasets[x]['filename']
                )
            else:
                selected_excel = excel_files[0]
            
            self._show_sheet_selector(selected_excel)
        except Exception as e:
            print(f"Error _show_excel_sheet_processing {str(e)}")
    
    def _show_sheet_selector(self, excel_key):
        """Show sheet selection for a specific Excel file"""
        try:
            dataset = st.session_state.datasets[excel_key]
            processed_sheets = dataset.get('processed_sheets', [])
            available_sheets = [s for s in dataset['sheet_names'] if s not in processed_sheets]
            
            if not available_sheets:
                st.info(f"All sheets from {dataset['filename']} have been processed.")
                return
            
            selected_sheet = st.selectbox(
                f"Choose a sheet from {dataset['filename']}:", 
                available_sheets,
                key=f"sheet_select_{excel_key}"
            )
            
            if st.button("Process Selected Sheet", key=f"process_sheet_{excel_key}"):
                with st.spinner(f"Processing sheet '{selected_sheet}'..."):
                    sheet_key = self.data_service.process_excel_sheet(excel_key, selected_sheet)
                    st.success(f"Sheet '{selected_sheet}' processed successfully!")
                    st.rerun()
        except Exception as e:
            print(f"Error _show_sheet_selector {str(e)}")
    
    def _show_uploaded_datasets(self):
        """Display list of processed datasets"""
        try:
            processed_datasets = self.data_service.get_processed_datasets()
            
            if not processed_datasets:
                return
                
            st.write("**Uploaded Datasets**")
            
            for key, dataset in processed_datasets.items():
                if 'df' not in dataset:
                    continue
                
                with st.container():
                    col1, col2 = st.columns([5, 1])
                    
                    with col1:
                        sheet_info = f" (Sheet: {dataset.get('sheet_name', '')})" if dataset.get('sheet_name') else ""
                        st.button(f"**{dataset['filename']}{sheet_info}**", key=f"name_{key}", use_container_width=True)
                    
                    with col2:
                        info_clicked = st.button("ℹ️", key=f"info_{key}")
                    
                    if info_clicked or st.session_state.get(f"show_desc_{key}", False):
                        st.session_state[f"show_desc_{key}"] = True
                        with st.expander("Dataset Description", expanded=True):
                            st.write(dataset.get('description', 'No description available'))
                    
                    with st.expander("Preview"):
                        st.dataframe(dataset['df'].head(10), use_container_width=True)
        except Exception as e:
            print(f"Error _show_uploaded_datasets {str(e)}")