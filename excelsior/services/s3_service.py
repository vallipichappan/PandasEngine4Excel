import boto3
import os
from io import BytesIO
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import time 

import threading 

from io import BytesIO
import logging
from telemetry.setup_telemetry import setupLogging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

setupLogging()

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# Load the .env file
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
aws_bucket_key=os.getenv('AWS_BUCKET_KEY')
aws_bucket_name=os.getenv('AWS_BUCKET_NAME')
aws_endpoint_url=os.getenv('AWS_ENDPOINT_URL')


def get_s3_client():
    try:
        return boto3.client(
            's3',
            endpoint_url=aws_endpoint_url,
            verify=False,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    except Exception as e:
            print(f"Error get_s3_client {str(e)}")

def upload_to_s3(uploaded_file):
    s3_client = get_s3_client()  
    username = st.session_state.username  
    file_name = uploaded_file.name
    s3_key = aws_bucket_key % (username, file_name)
    
    # Create progress bar and status text
    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text("Preparing to upload file...")
    
    # Flag to track upload completion
    upload_complete = threading.Event()
    upload_success = [False]  # Using list to make it mutable in threads
    
    def perform_upload():
        try:
            file_buffer = BytesIO(uploaded_file.getvalue())
            s3_client.upload_fileobj(
                file_buffer, 
                aws_bucket_name, 
                s3_key
            )
            file_buffer.close()
            upload_success[0] = True
            logger.info("File uploaded to S3 successfully.")
        except Exception as e:
            print(f"Error uploading to S3: {str(e)}")
        finally:
            file_buffer.close()
            upload_complete.set()  # Signal that upload is done
    
    # Start upload in a separate thread
    upload_thread = threading.Thread(target=perform_upload)
    upload_thread.start()
    
    # Simulate progress while waiting for upload to complete
    progress = 0
    while not upload_complete.is_set():
        # Increment progress, but keep it below 100% until we know it's done
        if progress < 95:
            progress += 1
        status_text.text(f"Uploading: {progress}% complete")
        progress_bar.progress(progress)
        time.sleep(0.1)  # Adjust speed of progress bar
    
    # Upload is complete, set to 100%
    if upload_success[0]:
        progress_bar.progress(100)
        status_text.text("Upload complete!")
        time.sleep(1)  # Show completion for a moment
    else:
        status_text.text("Upload failed. Please try again.")
    
    # Optional: clear the progress indicators after a delay
    time.sleep(1)
    progress_bar.empty()
    status_text.empty()
    
    return upload_success[0]


def get_dataset_from_s3(file_key):
    """Load dataset from S3 only when needed"""
    if 'datasets' not in st.session_state or file_key not in st.session_state.datasets:
        return None
        
    # Create file buffer from S3
    file_name = st.session_state.datasets[file_key]['filename']
    username = st.session_state.username
    s3_key = aws_bucket_key % (username, file_name)
    
    # Create progress indicator
    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text(f"Loading {file_name} from storage...")
    
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=aws_bucket_name, Key=s3_key)
        file_buffer = BytesIO(response['Body'].read())
        
        progress_bar.progress(50)
        status_text.text(f"Processing {file_name}...")
        
        # Process file based on extension
        file_ext = os.path.splitext(file_name)[1].lower()
        if file_ext == ".csv":
            df = pd.read_csv(file_buffer, thousands=",", low_memory=False)
        elif file_ext in [".xls", ".xlsx"]:
            sheet_name = st.session_state.datasets[file_key].get('selected_sheet')
            df = pd.read_excel(file_buffer, sheet_name=sheet_name)
        else:
            raise ValueError("Unsupported file format")
        
        progress_bar.progress(100)
        status_text.text(f"Data loaded successfully!")
        time.sleep(0.5)  # Brief pause to show completion
        
        # Clean up UI elements
        progress_bar.empty()
        status_text.empty()
            
        return df
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"Error loading data: {str(e)}")
        return None