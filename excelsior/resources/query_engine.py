import json
import re
import pandas as pd
from llama_index.experimental.query_engine import PandasQueryEngine
from llama_index.core import PromptTemplate
from dotenv import load_dotenv
import streamlit as st

from services.llm_service import LLMService

load_dotenv()
class CustomPandasQueryEngine(PandasQueryEngine):
    def __init__(self, df, **kwargs):
        super().__init__(df, **kwargs)
        self.df = df
        self.schema = self._get_schema(df)
        self.pandas_prompt = kwargs.get('pandas_prompt', None)
        

    def query(self, query_str: str):
        schema_analysis = self._analyse_query(self.schema, query_str)
        self._pandas_prompt = PromptTemplate(self.pandas_prompt.template).partial_format(
            query_str=query_str,
            query_analysis=schema_analysis  
        )

        return super().query(query_str)

    def _process_pandas_instructions(self, pandas_instructions: str) -> str:
        try:
            local_vars = {'df': self.df}
            result = eval(pandas_instructions.strip(), {}, local_vars)
             # Format numeric results
            if hasattr(result, 'round'):
                result = result.round(2)  
            
            if isinstance(result, pd.Series):
                result = result.apply(lambda x: '{:,.2f}'.format(x))
            
            return str(result)
        except Exception as e:
            return f"Error evaluating def _process_pandas_instructionn {str(e)}"
        
    
    def _get_schema(self, df):
        """Return schema information for the dataframe columns"""
        schema = {}
        try:
            for column in df.columns:
                if pd.api.types.is_numeric_dtype(df[column]):
                    schema[column] = {
                        'type': 'numeric'
                    }
                else:
                    unique_values = df[column].unique().tolist()
                    schema[column] = {
                        'type': 'categorical',
                        'unique_values': unique_values[:10],  # Limit to first 10 values
                        'total_unique': len(unique_values)
                    }
        except Exception as e:
            print(f"_get_schema {e}")
        return schema


    def _analyse_query(self, schema, query: str):
        schema_prompt = f"""
        Analyze this query and map it to the available columns in our financial dataset.
        Remember:
        1. Data will contain information on investment/costs, profit and loss/revenue. It might not be the name of rows or columns but might be a group within a column. Identify this.
        2. It will have information on platform units and countries. User might say things like retrieve by team/group and you should understand which row./column/group they want.
        3. It will have data in terms of date/month. Identify this.
        4. All of these information will either be in the form of columns or rows or within a column. Use the schema wisely to decide the right filters.

        Query: {query}
        
        Available columns and their properties:
        {schema}
        
        Please identify:
        1. Metric columns (numeric values we need to calculate)
        2. Filter columns (what we're filtering by)
        3. Group by columns (what we're grouping by)
        4. Time periods (if any)
        5. Aggregation function needed (sum, average, etc.)
        
        Return ONLY a raw JSON object without any formatting, quotes, or backticks:
        {{
            "metric_columns": [],
            "filter_columns": {{"column_name": "filter_value"}},
            "group_by_columns": [],
            "time_period": "",
            "aggregation": ""
        }}
        """
        try:
            llm = LLMService.get_instance()
            response = llm(schema_prompt)
            print('schema response')
            print(response)
            cleaned_response = re.sub(r'```[\w]*\n?|\n```|`', '', response).strip()
            return json.loads(cleaned_response)
        
        except Exception as e:
            print("Error parsing response from _analyse_query:", e)
            return None

def create_query_engine(pivot_result):
    instruction_str = (
        "1. Convert the query to executable Python code using Pandas.\n"
        "2. For simple queries, return just the numerical calculation.\n"
        "3. The final line should return a dictionary with:\n"
        "   - 'comparison': the numerical comparison\n"
        "   - 'analysis': contextual information\n"
        "4. The code should represent a solution to the query.\n"
        "5. PRINT ONLY THE EXPRESSION.\n"
        "6. Do not quote the expression.\n"
        "7. Use 'df' as the DataFrame name in your expression.\n"
    )

    pandas_prompt_str = (
        "You are working with a pandas dataframe containing financial data..\n"
        "The name of the dataframe is `df`.\n"
        "Follow these instructions:\n"
        "Remember! the column names may not be straightforward\n"
        "These are the columns, choose the right column name from here: {df_columns}\n"
        "Here's an analysis of the query that breaks it into logical parts: {query_analysis}\n"
        "You may need things like group by and multiple filters"
        "Date time columns are stored as string values. Sometimes it is a complete date or it's just month and year. So when it's being queried, you may have to do the right kind of query/match."
        "You may have to do a .contains() or if appropriate .str.startswith() or .str.endswith() or pd.datetime() then dt.strftime(%)=="
        "{instruction_str}\n"
        "Query: {query_str}\n\n"
        "Expression:"
    )

    response_synthesis_prompt_str = (
        "Given an input question, provide the answer and if required a detailed analysis on this financial data\n"
        "Query: {query_str}\n\n"
        "Data Output: {pandas_output}\n\n"
        "1. Put the numerical answer or table of numbers in a proper table format like a financial analyst would like to see\n"
        "2. For comparative queries:\n"
        "   - Identify the highest/lowest values\n"
        "   - Identify statistically what unit/month/country/value could be required to answer the query\n"
        "Response: "
    )

    pandas_prompt = PromptTemplate(pandas_prompt_str).partial_format(
        instruction_str=instruction_str,
        df_str=pivot_result.head().to_string(),
        df_columns=str(list(pivot_result.columns))
    )
    response_synthesis_prompt = PromptTemplate(response_synthesis_prompt_str)
    try:
        result = CustomPandasQueryEngine(
        df=pivot_result,
        verbose=True,
        pandas_prompt=pandas_prompt,
        response_synthesis_prompt=response_synthesis_prompt
    )
        print('query engine')
        print(result)
        return result
    
    except Exception as e:
        print("Error parsing response from create_query_engine:", e)
        return None