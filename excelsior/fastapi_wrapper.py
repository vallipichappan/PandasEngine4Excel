# fastapi_wrapper.py
import signal
import sys
import subprocess
import logging
import time
from fastapi import FastAPI
from excelsior.session_management import clean_up_session
import streamlit as st
import uvicorn
import threading


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class AppState:
    """Class to hold application state instead of using globals"""
    def __init__(self):
        self.streamlit_process = None

app_state = AppState()

def cleanup_storage():
    """Clean up storage when the application shuts down"""
    logger.info("Application shutting down, cleaning up storage...")
    
    try:
        
        # Initialize minimal session state to ensure clean_up_session works
        if not hasattr(st, 'session_state'):
            class SessionState(dict):
                pass
            st.session_state = SessionState()
            
            # Initialize required session state keys based on your code
            st.session_state.session_id = "shutdown-cleanup"
            st.session_state.datasets = {}
            st.session_state.pivot_tables = {}
            st.session_state.active_dataset = None
            st.session_state.active_pivot = None
            if 'messages' not in st.session_state:
                st.session_state.messages = []
        
        # Call your existing cleanup method
        clean_up_session()
        logger.info("Storage cleanup completed successfully")
    except Exception as e:
        logger.error(f"Error during storage cleanup: {str(e)}")

def start_streamlit():
    """Start Streamlit as a subprocess and store it in app_state"""
    cmd = ["streamlit", "run", "excelsior/app.py"]
    app_state.streamlit_process = subprocess.Popen(cmd)
    logger.info(f"Started Streamlit process with PID {app_state.streamlit_process.pid}")
    
    # Give Streamlit a moment to start up
    time.sleep(2)

@app.on_event("startup")
async def startup_event():
    """Handle FastAPI startup event"""
    logger.info("FastAPI server starting up")
    # Start Streamlit in a separate thread
    threading.Thread(target=start_streamlit).start()

@app.on_event("shutdown")
async def shutdown_event():
    """Handle FastAPI shutdown event"""
    logger.info("FastAPI server shutting down")
    if app_state.streamlit_process:
        logger.info(f"Terminating Streamlit process {app_state.streamlit_process.pid}")
        app_state.streamlit_process.terminate()
        try:
            app_state.streamlit_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("Streamlit didn't terminate gracefully, forcing...")
            app_state.streamlit_process.kill()
    
    cleanup_storage()

@app.get("/")
async def root():
    return {"message": "Welcome to the Excelsior FastAPI app"}

@app.get("/healthcheck")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}

# Add a manual cleanup endpoint (optional, for admin/debug use)
@app.get("/admin/cleanup", include_in_schema=False)
async def manual_cleanup():
    """Endpoint to trigger manual cleanup"""
    cleanup_storage()
    return {"status": "cleanup completed"}

if __name__ == "__main__":
    # Handle signals
    def signal_handler(signum, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        sys.exit(0)
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_handler)
    
    # Start the FastAPI server
    uvicorn.run("fastapi_wrapper:app", host="0.0.0.0", port=8000)