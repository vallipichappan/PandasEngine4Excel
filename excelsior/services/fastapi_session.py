# services/session_service.py
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd

class SessionService:
    def __init__(self, app_data: Dict[str, Any]):
        self.app_data = app_data
        
    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.app_data["sessions"][session_id] = {
            "created_at": datetime.now(),
            "last_activity": datetime.now(),
            "datasets": {},
            "pivot_tables": {},
            "active_dataset": None,
            "active_pivot": None,
            "messages": []
        }
        return session_id
        
    def get_session(self, session_id: str) -> Dict[str, Any]:
        return self.app_data["sessions"].get(session_id)
        
    def cleanup_expired_sessions(self, timeout_minutes: int = 60):
        now = datetime.now()
        expired = []
        
        for session_id, session_data in self.app_data["sessions"].items():
            if now - session_data["last_activity"] > timedelta(minutes=timeout_minutes):
                expired.append(session_id)
                
        for session_id in expired:
            self._cleanup_session(session_id)
            
    def _cleanup_session(self, session_id: str):
        if session_id in self.app_data["sessions"]:
            # Explicitly clear pandas DataFrames to free memory
            session = self.app_data["sessions"][session_id]
            for dataset in session["datasets"].values():
                if "df" in dataset:
                    dataset["df"] = pd.DataFrame()  # Clear DataFrame
            for pivot in session["pivot_tables"].values():
                if "result" in pivot:
                    pivot["result"] = pd.DataFrame()
                    
            del self.app_data["sessions"][session_id]