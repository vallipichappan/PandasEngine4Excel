from llama_index.core import Settings

class LLMService:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise ValueError("LLM not initialized")
        return cls._instance
    
    @classmethod
    def initialize(cls, llm):
        cls._instance = llm
        Settings.llm = llm