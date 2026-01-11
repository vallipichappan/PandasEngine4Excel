import json
import os
import requests
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

def authenticate_user(user_id, password):
    try:

        url = ''
        payload = {

        }
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        
        if response.status_code == 200:
            json_dict = json.loads(response.text)
            return json_dict.get('id_token')
        else:
            return None
        
    except Exception as e:
        print(f'Error in authenticate_user {e}')