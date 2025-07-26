import psycopg2
from psycopg2 import extras
import logging
from langchain_google_vertexai import ChatVertexAI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database_connection')

# Database configuration
DB_CONFIG = {
    'user': 'yugabyte',
    'password': 'Password123%23',
    'host': '10.9.109.47',
    'port': 5433,
    'dbname': 'yugabyte'
}

# Initialize Gemini LLM
gemini_llm = ChatVertexAI(model="gemini-2.5-pro", temperature=0)

def create_dsn():
    """Create the DSN string from configuration."""
    # Use key-value format which is more reliable for special characters
    return (
        f"user={DB_CONFIG['user']} "
        f"password={DB_CONFIG['password']} "
        f"host={DB_CONFIG['host']} "
        f"port={DB_CONFIG['port']} "
        f"dbname={DB_CONFIG['dbname']}"
    )

def connect():
    """Connect to the PostgreSQL database."""
    connection = None
    cursor = None
    try:
        logger.info(f"Attempting to connect to: {DB_CONFIG['host']}:{DB_CONFIG['port']} as {DB_CONFIG['user']}")
        # Use keyword arguments instead of DSN for better handling of special characters
        connection = psycopg2.connect(
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'].replace('%23', '#'),  # Convert URL-encoded # back to actual #
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['dbname'],
            connect_timeout=10
        )
        cursor = connection.cursor(cursor_factory=extras.DictCursor)
        logger.info("Successfully connected to the database")
        return connection, cursor
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        raise

def disconnect(connection, cursor):
    """Close the database connection."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing connection: {str(e)}")

def execute_query(connection, cursor, query, params=None):
    """Execute a SQL query with optional parameters."""
    try:
        cursor.execute(query, params)
        connection.commit()
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        connection.rollback()
        raise

def get_explain_plan(connection, cursor, query):
    """Get the explain plan for a query after replacing bind variables."""
    try:
        # Ask the LLM to replace bind variables
        prompt = f"""
        Replace any bind variables ($1, $2, etc.) in the following SQL query with actual values.
        If the query doesn't contain bind variables, return it unchanged.
        
        SQL Query: {query}
        
        Return ONLY the SQL query with bind variables replaced. Do not include any markdown formatting.
        """
        
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        response = gemini_llm.invoke(messages)
        
        # Clean up the response
        query_with_values = response.content
        
        # Remove any markdown formatting
        query_with_values = query_with_values.replace('```sql', '').replace('```', '').strip()
        
        # Log the query being used for explain plan
        logger.info(f"Query used for explain plan: \n\n {query_with_values}")
        
        # Get the explain plan
        explain_query = f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {query_with_values}"
        result = execute_query(connection, cursor, explain_query)
        return result[0][0]  # Return the first plan in JSON format
    except Exception as e:
        logger.error(f"Error getting explain plan: {str(e)}")
        return None

def get_table_indexes(connection, cursor, table_name):
    """Get all indexes for a given table."""
    try:
        query = """
        SELECT 
            indexdef
        FROM pg_indexes
        WHERE tablename = %s;
        """
        results = execute_query(connection, cursor, query, (table_name,))
        return [row['indexdef'] for row in results]
    except Exception as e:
        logger.error(f"Error getting indexes for table {table_name}: {str(e)}")
        return []

def analyze_explain_plan(query, explain_plan):
    """Analyze the explain plan for sequential scans on range predicates."""
    prompt = f"""
    Analyze the following explain plan and determine if there are any sequential scans (Seq Scan) 
    on fields used in range predicates. A range predicate is a WHERE clause that uses operators 
    like BETWEEN, >, <, >=, <=, or IN with a range of values.
    
    SQL Query: {query}
    
    Explain Plan: {explain_plan}
    
    Context about YugabyteDB Partitioning:
    - By default, indexes are HASH which produces even distribution of rows across tablets.
    - RANGE partitioning (PRIMARY KEY(field ASC)) keeps rows ordered but may create write hotspot in a tablet where all activity such as inserts and selects are only on the most recent values..
    - Common query problem: Queries using range predicates or ORDER BY without a supporting range index often trigger sequential scans, even when only a small key range is needed.
    
    Analysis Tasks:
    1. Identify any range predicates in the query
    2. Check if there are sequential scans on fields used in these range predicates
    3. If sequential scans are found:
       a. Check if the field is the primary key
       b. If it is the primary key, suggest:
          - Recreating the table with RANGE partitioning on this field
          - Or adding a secondary range index (CREATE INDEX table_field_idx ON table_name (field ASC))
       c. If it is not the primary key:
          - Suggest creating a secondary range index (CREATE INDEX table_field_idx ON table_name (field ASC))
    4. Return a detailed analysis of your findings, including:
       - Whether sequential scans are found
       - Which fields are affected
       - Recommended index creation strategy
       - Explanation of why the current index (if it exists) is not sufficient
    
    Do not include pleasantries or introductory phrases. Be concise and factual.
    """
    
    messages = [
        {"role": "user", "content": prompt}
    ]
    
    try:
        response = gemini_llm.invoke(messages)
        return response.content
    except Exception as e:
        logger.error(f"Error analyzing explain plan: {str(e)}")
        return "Failed to analyze explain plan"

def analyze_query(query, connection, cursor):
    """Analyze a SQL query using Gemini LLM."""
    prompt = f"""
    Analyze the following SQL query and determine if it contains any range predicates.
    A range predicate is a WHERE clause that uses operators like BETWEEN, >, <, >=, <=, or IN with a range of values.
    
    SQL Query: {query}
    
    Does this query contain any range predicates? If yes, specify what they are.
    """
    
    # Create a message in the correct format for ChatVertexAI
    messages = [
        {"role": "user", "content": prompt}
    ]
    
    # Use the invoke method instead of __call__ as per deprecation warning
    response = gemini_llm.invoke(messages)
    
    # Check if the query contains range predicates
    contains_range = "yes" in response.content.lower()
    
    # If it contains range predicates, get the explain plan and analyze it
    explain_plan = None
    explain_analysis = None
    if contains_range:
        explain_plan = get_explain_plan(connection, cursor, query)
        if explain_plan:
            explain_analysis = analyze_explain_plan(query, explain_plan)
    
    # Return both analysis and explain plan
    return {
        "analysis": response.content,
        "explain_plan": explain_plan,
        "explain_analysis": explain_analysis,
        "contains_range": contains_range
    }

def analyze_slow_queries():
    """Analyze the top 5 slowest queries for range predicates."""
    try:
        # Get database connection
        connection, cursor = connect()
        
        # Get top 5 slowest queries
        query = """
        SELECT query , total_exec_time , calls, total_exec_time / calls as avg_exec_time
        FROM pg_stat_statements 
        ORDER BY total_exec_time DESC 
        LIMIT 2
        """
        
        results = execute_query(connection, cursor, query)
        
        # Print each row
        print("\nRows from pg_stat_statements:")
        print("-" * 100)
        for i, row in enumerate(results, 1):
            print(f"\nRow {i}:")
            print(f"  Calls: {row['calls']}")
            print(f"  Total Exec Time (ms): {row['total_exec_time']}")
            print(f"  Avg Exec Time (ms): {row['avg_exec_time']}")
            print("\n  Query:")
            print(f"{row['query']}")
            print("-" * 100)
            
        # Analyze each query
        print("\nQuery Analysis:")
        print("-" * 100)
        for i, row in enumerate(results, 1):
            # Analyze the query
            sql_query = row['query']
            logger.info(f"Analyzing query {i}: \n\n {sql_query}\n\n")
            analysis = analyze_query(sql_query, connection, cursor)
            
            # Extract table name from query (simple extraction, may need improvement)
            table_name = None
            if 'FROM' in sql_query:
                from_part = sql_query.split('FROM')[1]
                table_name = from_part.split()[0].strip('(),')
            
            # Get indexes if table name was found
            indexes = []
            if table_name:
                indexes = get_table_indexes(connection, cursor, table_name)
            
            # Print analysis results
            print(f"\nAnalysis for Query {i}:")
            print("-" * 50)
            print("\nQuery Analysis:")
            print(analysis['analysis'])
            
            if analysis['contains_range']:
                print("\nExplain Plan Analysis:")
                if analysis['explain_analysis']:
                    print(analysis['explain_analysis'])
                else:
                    print("No explain plan analysis available")
                
                if indexes:
                    print("\nExisting Indexes:")
                    for idx in indexes:
                        print(f"  {idx}")
                else:
                    print("No indexes found for this table")
            print("-" * 50)
            
    except Exception as e:
        logger.error(f"Error analyzing queries: {str(e)}")
        raise
    finally:
        disconnect(connection, cursor)

if __name__ == "__main__":
    # Example usage
    try:
        # Example query
        connection, cursor = connect()
        result = execute_query(connection, cursor, "SELECT version();")
        print(f"Database version: {result[0]['version']}")
        
        # Analyze slow queries
        print("\nAnalyzing slow queries...")
        analyze_slow_queries()
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise
    finally:
        try:
            disconnect(connection, cursor)
        except NameError:
            # If connection failed, these variables won't exist
            pass
