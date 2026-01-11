import numpy as np
import pandas as pd
import streamlit as st
from typing import Dict, List
from services.query_service import QueryService

class QueryUI:
    def __init__(self, query_service: QueryService):
        self.query_service = query_service

    def show_query_page(self):
        """Main method to display the query page"""
        st.write("### Chat with Your Data")
        
        if not self.query_service.session_state.pivot_tables:
            st.info("No pivot tables available. Please create pivot tables first.")
            return
        
        self._initialise_messages()
        selected_pivot = self._render_pivot_selection()
        
        if selected_pivot:
            self._handle_pivot_change(selected_pivot)
            self._render_chat_messages(selected_pivot)
            self._handle_chat_input(selected_pivot)

            if hasattr(self.query_service.session_state, 'execute_retry') and self.query_service.session_state.execute_retry:
                self._handle_retry_query(selected_pivot)

    def _initialise_messages(self):
        """initialise message state if not present"""
        if 'messages' not in self.query_service.session_state:
            self.query_service.session_state.messages = []
        
        if 'explanations_in_progress' not in self.query_service.session_state:
            self.query_service.session_state.explanations_in_progress = {}

    def _render_pivot_selection(self):
        """Render pivot table selection dropdown"""
        pivot_options = {
            k: f"{v['name']} ({self.query_service.session_state.datasets[v['source_dataset']]['filename']})" 
            for k, v in self.query_service.session_state.pivot_tables.items()
        }
        
        return st.selectbox(
            "Select pivot table to query",
            options=list(pivot_options.keys()),
            format_func=lambda x: pivot_options[x]
        )

    def _handle_pivot_change(self, selected_pivot: str):
        """Handle pivot table change and initialise query engine"""
        pivot_changed = self.query_service.session_state.get('active_pivot') != selected_pivot
        
        if pivot_changed:
            self.query_service.session_state.messages = []
            pivot_data = self.query_service.session_state.pivot_tables[selected_pivot]
            dataset_name = self.query_service.session_state.datasets[pivot_data['source_dataset']]['filename']
            
            system_msg = f"I'm now analyzing the pivot table '{pivot_data['name']}' from dataset '{dataset_name}'. What would you like to know?"
            self.query_service.session_state.messages.append({"role": "assistant", "content": system_msg})
        
        self.query_service.session_state.active_pivot = selected_pivot
        pivot_data = self.query_service.session_state.pivot_tables[selected_pivot]
        
        # initialise query engine through the service
        pivot_data = self.query_service.initialise_query_engine(pivot_data)
        self.query_service.session_state.pivot_tables[selected_pivot] = pivot_data

    def _render_chat_messages(self, selected_pivot: str):
        """Render all chat messages with interactive elements"""
        pivot_data = self.query_service.session_state.pivot_tables[selected_pivot]
        
        # Find last assistant message with code
        last_assistant_idx = -1
        for i, msg in enumerate(self.query_service.session_state.messages):
            if msg["role"] == "assistant" and "pandas_code" in msg and msg["pandas_code"]:
                last_assistant_idx = i
        
        # Display all messages
        for i, message in enumerate(self.query_service.session_state.messages):
            with st.chat_message(message["role"]):
                if message["role"] == "assistant" and "content" in message:
                    self._display_query_response(message["content"])
                else:
                    st.write(message["content"])
                
                # Add additional UI elements for assistant messages with pandas code
                if message["role"] == "assistant" and "pandas_code" in message and message["pandas_code"]:
                    self._render_code_expander(message)
                    self._render_explanation_ui(i, message, last_assistant_idx, pivot_data)

    def _render_code_expander(self, message: Dict):
        """Render code expander for a message"""
        with st.expander("View Code"):
            st.code(message["pandas_code"], language="python")

    def _render_explanation_ui(self, message_idx: int, message: Dict, last_assistant_idx: int, pivot_data: Dict):
        """Render explanation and retry UI for a message"""
        if "explanation" in message and message["explanation"]:
            with st.expander("Explanation", expanded=True):
                st.write(message["explanation"])
        else:
            col1, col2 = st.columns(2)
            
            with col1:
                explain_key = f"explain_{message_idx}"
                if st.button("Explain", key=explain_key, use_container_width=True):
                    with st.spinner("Generating explanation..."):

                        class TempResponse:
                            def __init__(self, pandas_code):
                                self.metadata = {'pandas_instruction_str': pandas_code}

                        temp_response = TempResponse(message["pandas_code"])
                        explanation = self.query_service.explain_pandas_script(temp_response)
                        message["explanation"] = explanation
                        st.rerun()
            
            with col2:
                if message_idx == last_assistant_idx:
                    retry_key = f"retry_{message_idx}"
                    if st.button("Retry", key=retry_key, use_container_width=True):
                        self._prepare_retry_state(message_idx)
                        st.rerun()
    
    def _prepare_retry_state(self, message_idx: int):
        """Prepare the session state for retry operation"""
        # Get the user message that triggered this response
        if message_idx > 0 and self.query_service.session_state.messages[message_idx-1]["role"] == "user":
            query = self.query_service.session_state.messages[message_idx-1]["content"]
            # Remove this response
            self.query_service.session_state.messages.pop(message_idx)
            # Set to re-query
            self.query_service.session_state.retry_query = query
            self.query_service.session_state.execute_retry = True

    def _handle_retry_query(self, selected_pivot: str):
        """Handle retry query execution"""
        pivot_data = self.query_service.session_state.pivot_tables[selected_pivot]
        retry_query = self.query_service.session_state.retry_query
        
        # Reset flags
        del self.query_service.session_state.execute_retry
        del self.query_service.session_state.retry_query
        
        # Process retry
        with st.chat_message("assistant"):
            with st.spinner("Retrying analysis..."):
                # Build context from previous messages
                context = self.query_service.build_chat_context(
                    self.query_service.session_state.messages, 
                    pivot_data
                )
                
                # Execute query with context
                success, response, error = self.query_service.execute_query(
                    pivot_data['query_engine'], 
                    retry_query, 
                    context
                )
                
                if success:
                    # Extract pandas code from response metadata
                    pandas_code = response.metadata.get('pandas_instruction_str', '')
                    
                    # Display response
                    self._display_query_response(response.response)
                    
                    # Add to message history
                    message = {
                        "role": "assistant", 
                        "content": response.response,
                        "pandas_code": pandas_code,
                        "explanation": None  # Will be filled if user requests
                    }
                    self.query_service.session_state.messages.append(message)
                    st.rerun()
                else:
                    # Display error
                    error_msg = "I couldn't process that question. Please try rephrasing."
                    st.error(error_msg)
                    if error:
                        st.error(f"Error details: {error}")
                    
                    # Still add to message history, but mark as error
                    self.query_service.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg,
                        "error": True
                    })
                    st.rerun()

    def _handle_chat_input(self, selected_pivot: str):
        """Handle new chat input and process queries"""
        pivot_data = self.query_service.session_state.pivot_tables[selected_pivot]
        
        if prompt := st.chat_input("Ask a question about your data..."):
            self._process_user_query(prompt, pivot_data)

    def _process_user_query(self, prompt: str, pivot_data: Dict):
        """Process a user query and generate response"""
        self.query_service.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.write(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Analysing your data..."):
                self._execute_query(prompt, pivot_data)

    def _execute_query(self, query: str, pivot_data: Dict):
        """Execute query and handle response"""
        context = self.query_service.build_chat_context(
            self.query_service.session_state.messages,
            pivot_data
        )
        
        success, response, error = self.query_service.execute_overall_query(
            pivot_data['query_engine'], 
            query, 
            context
        )
        
        if success:
            self._handle_successful_response(response)
        else:
            self._handle_error_response(error)

    def _handle_successful_response(self, response):
        """Handle successful query response"""
        pandas_code = response.metadata.get('pandas_instruction_str', '')
        
        message = {
            "role": "assistant", 
            "content": response.response,
            "pandas_code": pandas_code,
            "explanation": None
        }
        self.query_service.session_state.messages.append(message)
        st.rerun()

    def _handle_error_response(self, error):
        """Handle query error response"""
        error_msg = "I couldn't process that question. Please try rephrasing."
        st.error(error_msg)
        if error:
            st.error(f"Error details: {error}")
        
        self.query_service.session_state.messages.append({
            "role": "assistant",
            "content": error_msg,
            "error": True
        })
        st.rerun()

    def _display_query_response(self, content):
        """Format and display query responses based on their type"""
    
        # For debugging
        print(f"Content: {content[:100]}...")  # Print first 100 chars
        print(f"Type: {type(content)}")
        
        # If the content is already a DataFrame
        if isinstance(content, pd.DataFrame):
            st.dataframe(content)
            return
        
        # If content is a numeric value
        if isinstance(content, (int, float, np.number)):
            result_df = pd.DataFrame({
                "Result": [content]
            })
            st.table(result_df)
            return
        
        # Check if content is a string
        if isinstance(content, str):
            # Handle "Pandas Output:" prefix if present
            if "Pandas Output:" in content:
                # Extract the actual content after the prefix
                output_text = content.split("Pandas Output:")[1].strip()
                
                # Try to interpret as a numeric value
                try:
                    if output_text.replace('.', '', 1).replace('e', '', 1).replace('-', '', 1).replace('+', '', 1).isdigit():
                        numeric_value = float(output_text)
                        result_df = pd.DataFrame({
                            "Result": [numeric_value]
                        })
                        st.table(result_df)
                        return
                except:
                    pass
                    
                # If not numeric, check if it's tabular
                if '\n' in output_text:
                    st.text("Data from pandas:")
                    st.code(output_text, language=None)
                    return
                    
                # If not tabular, just display the output
                st.write(output_text)
                return
                
            # If no prefix, try to handle a single numeric value
            if content.strip().replace('.', '', 1).replace('e', '', 1).replace('-', '', 1).replace('+', '', 1).isdigit():
                try:
                    numeric_value = float(content.strip())
                    result_df = pd.DataFrame({
                        "Result": [numeric_value]
                    })
                    st.table(result_df)
                    return
                except:
                    pass
            
            # Check if content looks like a table (has multiple lines with structured data)
            if '\n' in content and any(c in content for c in ['\t', '  ']):
                try:
                    # Check for common table indicators
                    table_indicators = ['Length:', 'dtype:', '<class', 'Platform Unit', 'Work Type']
                    if any(indicator in content for indicator in table_indicators):
                        st.text("Table data:")
                        st.code(content, language=None)
                        return
                except:
                    pass
        
        st.write(content)