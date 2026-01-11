import streamlit as st

def initialise_session_state():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = False
    if 'token' not in st.session_state:
        st.session_state.token = None
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'pivot_result' not in st.session_state:
        st.session_state.pivot_result = None
    if 'query_engine' not in st.session_state:
        st.session_state.query_engine = None
    if 'llm' not in st.session_state:
        st.session_state.llm = None

def initialise_extended_session_state():
    # Original initialization
    initialise_session_state()
    
    # New session state variables
    if 'current_tab' not in st.session_state:
        st.session_state.current_tab = "upload"
    if 'datasets' not in st.session_state:
        st.session_state.datasets = {}  # Multiple datasets
    if 'pivot_tables' not in st.session_state:
        st.session_state.pivot_tables = {}  # Multiple pivot tables
    if 'active_dataset' not in st.session_state:
        st.session_state.active_dataset = None
    if 'active_pivot' not in st.session_state:
        st.session_state.active_pivot = None
    if 'current_response' not in st.session_state:
        st.session_state.current_response = None
    if 'explained_response' not in st.session_state:
        st.session_state.explained_response = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = None
    if 'filter_value' not in st.session_state:
        st.session_state.filter_value = None


    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'chat_context' not in st.session_state:
        st.session_state.chat_context = {} 
    if 'reset_query' not in st.session_state:
        st.session_state.reset_query = False
