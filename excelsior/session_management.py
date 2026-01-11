import time
import uuid
from datetime import datetime
import logging
import sys
import pandas as pd
import streamlit as st

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialise_session():
    """initialise a new session if one doesn't exist"""
    if 'session_id' not in st.session_state:
        # Generate a unique session ID
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.session_created = time.time()
        st.session_state.last_activity = time.time()
        logger.info(f"New session initialised: {st.session_state.session_id}")

def update_session_activity():
    """Update the last activity timestamp"""
    if 'session_id' in st.session_state:
        st.session_state.last_activity = time.time()

def check_session_timeout(timeout_minutes=60):
    """Check if the session has timed out and should be cleaned up"""
    if 'last_activity' in st.session_state:
        time_since_activity = time.time() - st.session_state.last_activity
        timeout_seconds = timeout_minutes * 60
        
        if time_since_activity > timeout_seconds:
            logger.info(f"Session timeout reached for {st.session_state.session_id}")
            clean_up_session()
            return True
    return False

def clean_up_session():
    """Clean up the current session and reset state"""
    if 'session_id' in st.session_state:
        logger.info(f"Cleaning up session: {st.session_state.session_id}")
        
        # Clear all datasets and pivot tables
        st.session_state.datasets = {}
        st.session_state.pivot_tables = {}
        
        # Reset active selections
        st.session_state.active_dataset = None
        st.session_state.active_pivot = None
        
        # Clear messages
        if 'messages' in st.session_state:
            st.session_state.messages = []
        
        # Reset timestamps
        st.session_state.session_created = time.time()
        st.session_state.last_activity = time.time()

def session_info_widget():
    """Display session information in the sidebar"""
    if 'session_id' in st.session_state:
        with st.sidebar.expander("Session Info"):
            st.write(f"Session ID: {st.session_state.session_id[:8]}...")
            created_time = datetime.fromtimestamp(st.session_state.session_created)
            st.write(f"Created: {created_time.strftime('%Y-%m-%d %H:%M')}")
            
            # Calculate and show session duration
            duration = time.time() - st.session_state.session_created
            hours, remainder = divmod(duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            st.write(f"Duration: {int(hours)}h {int(minutes)}m")
            
            # Show memory usage
            dataset_memory = sum(sys.getsizeof(v.get('df', pd.DataFrame())) for v in st.session_state.datasets.values())
            pivot_memory = sum(sys.getsizeof(v.get('result', pd.DataFrame())) for v in st.session_state.pivot_tables.values())
            total_mb = (dataset_memory + pivot_memory) / (1024*1024)
            st.write(f"Memory usage: ~{total_mb:.1f} MB")
            
            # Add manual cleanup button
            if st.button("Clear Session Data"):
                clean_up_session()
                st.success("Session data cleared!")
                st.rerun()