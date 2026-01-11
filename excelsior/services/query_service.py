
from typing import Dict, List, Optional, Tuple
from llama_index.core.base.response.schema import Response
from services.llm_service import LLMService
from resources.query_engine import create_query_engine, CustomPandasQueryEngine

class QueryService:
    def __init__(self, session_state):
        self.session_state = session_state
        self.llm = LLMService.get_instance()  # Assuming LLM is in session state

    def initialise_query_engine(self, pivot_data: Dict) -> Dict:
        """initialise query engine for pivot table"""
        try:
            if 'query_engine' not in pivot_data:
                pivot_data['query_engine'] = create_query_engine(pivot_data['result'])
            return pivot_data
        except Exception as e:
            print(f'Error in initialise_query_engine {e}')


    def execute_query(self, query_engine: CustomPandasQueryEngine, query: str, context: Optional[str] = None) -> Tuple[bool, Optional[Response], Optional[str]]:
        """Execute a query with retry mechanism"""
        try:
            retry_count = 0
            max_retries = 3
            final_query = query
            if context:
                final_query = f"{context}\n\nCurrent question: {query}\n\nProvide a clear, concise answer."
            
            while retry_count < max_retries:
                try:
                    response = query_engine.query(final_query)
                    return True, response, None
                except Exception as e:
                    retry_count += 1
                    if retry_count == 2 and context:
                        final_query = f"Analyzing pivot table. Question: {query}"
                    if retry_count >= max_retries:
                        return False, None, str(e)
            
            return False, None, "Maximum retries exceeded"
        except Exception as e:
            print(f'Error in execute_query {e}')
    

    def execute_overall_query(self, query_engine: CustomPandasQueryEngine, query: str, context: Optional[str] = None) -> Tuple[bool, Optional[Response], Optional[str]]:
        """Execute a query with integrated analytical capabilities"""
        try:
            analytical_keywords = ["analyse", "why", "explain", "interpret", "insight", 
                                "trend", "pattern", "meaning", "implication", "compare",
                                "reason", "understand", "contribute", "who"]
            is_analytical = any(keyword in query.lower() for keyword in analytical_keywords)
            
            if is_analytical:
                return self.provide_analysis_on_previous_results(query, context)
            else:
                return self.execute_query(query_engine, query, context)
        except Exception as e:
            print(f'Error in execute_overall_query {e}')

    def provide_analysis_on_previous_results(self, query: str, context: Optional[str] = None) -> Tuple[bool, Optional[Response], Optional[str]]:
        """Provide analysis based on the previous query results without executing new queries"""
        previous_pandas_code = None
        previous_result = None

        for message in reversed(self.session_state.messages):
            if message["role"] == "assistant" and "content" in message:
                previous_result = message["content"]
                previous_pandas_code = message.get("pandas_code", "# No code available")
                break
        
        if not previous_result:
            return False, None, "No previous results to analyze"
        
        analytical_prompt = f"""
        You are a financial analyst reviewing data that has already been retrieved.
        
        {context if context else ""}
        
        Previous data result: 
        {previous_result}
        
        Current analytical question: {query}
        
        Please provide a thoughtful analysis of these results that:
        1. Interprets the numbers in a business context
        2. Identifies any patterns, trends, or anomalies
        3. Explains possible reasons for the observed data
        4. Provides strategic implications or recommendations if appropriate
        
        Focus ONLY on analyzing and interpreting the data that has already been retrieved.
        """
        
        try:
            analysis = self.llm(analytical_prompt)
            
            class AnalyticalResponse:
                def __init__(self, response, pandas_code):
                    self.response = response
                    self.metadata = {'pandas_instruction_str': pandas_code}
            
            response_obj = AnalyticalResponse(
                f"**Analysis of Previous Results:**\n{analysis}",
                previous_pandas_code
            )
            
            return True, response_obj, None
            
        except Exception as e:
            return False, None, f"Error in analysis: {str(e)}"

    def build_chat_context(self, messages: List[Dict], pivot_data: Dict) -> str:
        """Build context information from previous chat messages"""
        dataset = self.session_state.datasets[pivot_data['source_dataset']]
        relevant_messages = messages[-10:] if len(messages) > 10 else messages
        
        context = f"""
        You are analyzing a pivot table named '{pivot_data['name']}' created from the dataset '{dataset['filename']}'.
        
        Previous conversation:
        """
        try:
            for msg in relevant_messages:
                role = "User" if msg["role"] == "user" else "Assistant"
                context += f"\n{role}: {msg['content']}\n"
                
                if msg["role"] == "assistant" and "pandas_code" in msg and msg["pandas_code"]:
                    context += f"\nCode used: {msg['pandas_code']}\n"
            
            if 'config' in pivot_data:
                config = pivot_data['config']
                context += f"\nPivot table configuration:\n"
                context += f"- Rows: {', '.join(config['rows']) if config['rows'] else 'None'}\n"
                context += f"- Values: {', '.join(config['values']) if config['values'] else 'None'}\n"
                context += f"- Aggregation: {config['aggfunc']}\n"
            
            return context
        except Exception as e:
            print(f'Error in build_chat_context {e}')

    def explain_pandas_script(self, pandas_result: Dict) -> str:
        """Explain a pandas operation in plain financial terms"""
        prompt = f"""
        Explain to a financial analyst who is not familiar with python programming
        Basically what you need to do is translate python to english using financial lingo
        what the following pandas script is doing in terms of excel terms
        list all the columns it picked to aggregate, all the filtering and aggregations it's doing and values used
        pandas script: {pandas_result.metadata['pandas_instruction_str']}
        """
        try:
            return self.llm(prompt)
        except Exception as e:
            print(f'Error in explain_pandas_script {e}')