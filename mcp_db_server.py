# MCP Database Query Server
# Secure read-only SQL database access for LLMs via Model Context Protocol

"""
MCP Database Query Server

This server exposes three MCP tools to Claude Desktop for secure, read-only
database querying:
- get_schema_info: List all tables and columns
- describe_table: Get detailed table information with samples
- execute_read_only_query: Execute validated SELECT queries

Architecture:
- Single-file Python application
- SQLAlchemy for database abstraction
- Pandas for result formatting
- fastmcp for MCP protocol handling
- Defense-in-depth security validation
"""

# ============================================================================
# IMPORTS & DEPENDENCIES
# ============================================================================

# Core Python imports
import os
import re
import sys
import time
import logging
import atexit
import functools
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict

# Third-party imports
import pandas as pd
from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
from sqlalchemy.pool import QueuePool
from mcp.server import Server
import mcp.types as types

# Optional: Environment variable loading from .env file
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Connection configuration
DEFAULT_POOL_SIZE = 5
DEFAULT_POOL_RECYCLE = 3600  # 1 hour
MAX_RESULT_ROWS = 500
QUERY_TIMEOUT_SECONDS = 30
SCHEMA_CACHE_TTL = 300  # 5 minutes
MAX_QUERY_LENGTH = 10000

# Rate limiting configuration
DEFAULT_RATE_LIMIT_ENABLED = True
DEFAULT_RATE_LIMIT_REQUESTS = 100  # requests per window
DEFAULT_RATE_LIMIT_WINDOW = 60  # seconds
DEFAULT_RATE_LIMIT_BURST = 10  # burst allowance

# Security constants
ALLOWED_QUERY_PREFIXES = ['SELECT', 'WITH']
FORBIDDEN_KEYWORDS = [
    'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE',
    'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'MERGE', 'EXECUTE', 'EXEC',
    'DECLARE', 'CALL', 'LOCK', 'UNLOCK', 'BACKUP', 'RESTORE', 'SHUTDOWN',
    'ATTACH', 'DETACH', 'VACUUM', 'ANALYZE', 'RENAME'
]

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Global logger
logger = None

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

def load_configuration() -> Dict[str, Any]:
    """Load and validate configuration from environment variables."""
    
    # Try to load .env file if dotenv is available
    if DOTENV_AVAILABLE:
        try:
            # Load from current directory
            from dotenv import load_dotenv
            load_dotenv()
            # Also try to load from parent directory (common in project structures)
            load_dotenv('.env')
        except Exception as e:
            print(f"Warning: Could not load .env file: {e}", file=sys.stderr)
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        # Provide more helpful error message
        current_dir = os.getcwd()
        env_files = [f for f in os.listdir(current_dir) if f.endswith('.env')]
        
        error_msg = [
            "DATABASE_URL environment variable is required.",
            "Example: postgresql://user:pass@localhost/dbname",
            "For SQL Server: mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server",
            "",
            f"Current directory: {current_dir}",
            f"Found .env files: {', '.join(env_files) if env_files else 'None'}",
            "",
            "Solutions:",
            "1. Set DATABASE_URL environment variable",
            "2. Create a .env file with DATABASE_URL=...",
            "3. Pass DATABASE_URL in Claude Desktop configuration"
        ]
        raise ValueError("\n".join(error_msg))
    
    return {
        'database_url': database_url,
        'pool_size': int(os.getenv('DB_POOL_SIZE', DEFAULT_POOL_SIZE)),
        'pool_recycle': int(os.getenv('DB_POOL_RECYCLE', DEFAULT_POOL_RECYCLE)),
        'echo_sql': os.getenv('DB_ECHO_SQL', 'false').lower() == 'true',
        'log_level': os.getenv('MCP_LOG_LEVEL', 'INFO').upper(),
        'query_timeout': int(os.getenv('QUERY_TIMEOUT', QUERY_TIMEOUT_SECONDS)),
        'max_result_rows': int(os.getenv('MAX_RESULT_ROWS', MAX_RESULT_ROWS)),
        'rate_limit_enabled': os.getenv('RATE_LIMIT_ENABLED', str(DEFAULT_RATE_LIMIT_ENABLED)).lower() == 'true',
        'rate_limit_requests': int(os.getenv('RATE_LIMIT_REQUESTS', DEFAULT_RATE_LIMIT_REQUESTS)),
        'rate_limit_window': int(os.getenv('RATE_LIMIT_WINDOW', DEFAULT_RATE_LIMIT_WINDOW)),
        'rate_limit_burst': int(os.getenv('RATE_LIMIT_BURST', DEFAULT_RATE_LIMIT_BURST)),
    }


def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """Configure application logging."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stderr)]
    )
    return logging.getLogger(__name__)


# ============================================================================
# DATABASE MANAGER COMPONENT
# ============================================================================

class DatabaseManager:
    """Manages database connections and engine lifecycle."""
    
    _engine: Optional[Engine] = None
    _config: Optional[Dict[str, Any]] = None
    
    @classmethod
    def initialize(cls, config: Dict[str, Any]):
        """Initialize database engine with configuration."""
        cls._config = config
        
        # Create engine with connection pooling
        cls._engine = create_engine(
            config['database_url'],
            poolclass=QueuePool,
            pool_size=config['pool_size'],
            pool_recycle=config['pool_recycle'],
            echo=config['echo_sql'],
            pool_pre_ping=True  # Validate connections before use
        )
        
        # Register cleanup handler
        atexit.register(cls.cleanup)
        
        # Test connection
        cls.test_connection()
    
    @classmethod
    def get_engine(cls) -> Engine:
        """Get database engine singleton."""
        if cls._engine is None:
            raise RuntimeError("DatabaseManager not initialized. Call initialize() first.")
        return cls._engine
    
    @classmethod
    def get_dialect_name(cls) -> str:
        """Get the database dialect name."""
        if cls._engine is None:
            return "unknown"
        return cls._engine.dialect.name
    
    @classmethod
    def test_connection(cls) -> bool:
        """Test database connectivity."""
        try:
            with cls._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    @classmethod
    def cleanup(cls):
        """Clean up database connections."""
        if cls._engine:
            cls._engine.dispose()
            logger.info("Database connections cleaned up")
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager for database connections."""
        connection = cls._engine.connect()
        try:
            yield connection
        finally:
            connection.close()


# ============================================================================
# SECURITY VALIDATOR COMPONENT
# ============================================================================

class QueryValidator:
    """Validates SQL queries for safety and compliance."""
    
    # Compiled regex patterns for performance
    _comment_pattern = re.compile(r'--.*$|/\*.*?\*/', re.MULTILINE | re.DOTALL)
    _query_prefix_pattern = re.compile(r'^\s*(SELECT|WITH)', re.IGNORECASE)
    _limit_pattern = re.compile(r'\bLIMIT\s+\d+\s*$', re.IGNORECASE)
    _order_by_pattern = re.compile(
        r'\bORDER\s+BY\b.*?(?=\b(?:LIMIT|OFFSET|$))', 
        re.IGNORECASE | re.DOTALL
    )
    
    @classmethod
    def normalize_query(cls, sql: str) -> str:
        """Normalize SQL query for validation."""
        # Remove comments
        sql = cls._comment_pattern.sub('', sql)
        # Trim whitespace
        sql = sql.strip()
        # Collapse multiple spaces ONLY outside of string literals
        # This regex preserves content within single or double quotes
        # Pattern explanation:
        # - ('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*") captures quoted strings
        # - (\s+) captures whitespace sequences
        # We replace whitespace with single space but preserve quoted strings as-is
        def collapse_spaces(match):
            if match.group(1):  # It's a quoted string
                return match.group(1)
            else:  # It's whitespace
                return ' '
        
        sql = re.sub(r"('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")|(\s+)", collapse_spaces, sql)
        return sql
    
    @classmethod
    def is_safe_query(cls, sql: str, dialect: str = 'postgresql') -> Tuple[bool, str, List[str]]:
        """
        Validate if SQL query is safe for execution.
        
        Returns:
            Tuple of (is_safe, modified_query, warnings)
        """
        warnings = []
        
        # Check query length
        if len(sql) > MAX_QUERY_LENGTH:
            return False, sql, ["Query exceeds maximum length"]
        
        # Normalize query
        normalized = cls.normalize_query(sql)
        
        if not normalized:
            return False, sql, ["Query is empty after normalization"]
        
        # Check query prefix
        if not cls._query_prefix_pattern.match(normalized):
            return False, sql, ["Query must start with SELECT or WITH"]
        
        # Check for forbidden keywords
        upper_sql = normalized.upper()
        for keyword in FORBIDDEN_KEYWORDS:
            # Use word boundaries to avoid false positives
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, upper_sql):
                return False, sql, [f"Query contains forbidden keyword: {keyword}"]
        
        # Add LIMIT/TOP if missing
        modified = cls.add_limit_if_missing(normalized, dialect)
        if modified != normalized:
            warnings.append(f"Query automatically limited to {MAX_RESULT_ROWS} rows")
        
        return True, modified, warnings
    
    @classmethod
    def add_limit_if_missing(cls, sql: str, dialect: str = 'postgresql') -> str:
        """Add LIMIT or TOP clause to query if not present."""
        
        if dialect == 'mssql':
            # Check if TOP already exists
            if re.search(r'^\s*SELECT\s+(?:DISTINCT\s+)?TOP\s+\d+', sql, re.IGNORECASE):
                return sql
            
            # Check for OFFSET/FETCH (SQL Server 2012+)
            if 'OFFSET' in sql.upper() and 'FETCH' in sql.upper():
                return sql
            
            # Inject TOP
            # Handle SELECT DISTINCT
            if re.match(r'^\s*SELECT\s+DISTINCT', sql, re.IGNORECASE):
                return re.sub(r'^\s*SELECT\s+DISTINCT', f'SELECT DISTINCT TOP {MAX_RESULT_ROWS}', sql, count=1, flags=re.IGNORECASE)
            else:
                return re.sub(r'^\s*SELECT', f'SELECT TOP {MAX_RESULT_ROWS}', sql, count=1, flags=re.IGNORECASE)
        
        else:
            # Check if LIMIT already exists
            if cls._limit_pattern.search(sql):
                return sql
            
            # Check if ORDER BY exists
            order_by_match = cls._order_by_pattern.search(sql)
            if order_by_match:
                # Insert LIMIT after ORDER BY
                order_by_end = order_by_match.end()
                return sql[:order_by_end] + f' LIMIT {MAX_RESULT_ROWS}' + sql[order_by_end:]
            else:
                # Add LIMIT at end of query
                return sql + f' LIMIT {MAX_RESULT_ROWS}'


# ============================================================================
# CACHING DECORATOR
# ============================================================================

def cached(ttl: int = SCHEMA_CACHE_TTL):
    """Decorator for caching function results with TTL."""
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from arguments
            key = (func.__name__, args, tuple(sorted(kwargs.items())))
            
            # Check cache
            if key in cache:
                timestamp, value = cache[key]
                if time.time() - timestamp < ttl:
                    return value
            
            # Call function and cache result
            value = func(*args, **kwargs)
            cache[key] = (time.time(), value)
            return value
        
        return wrapper
    return decorator


# ============================================================================
# SCHEMA INSPECTOR COMPONENT
# ============================================================================

class SchemaInspector:
    """Inspects database schema and provides metadata."""
    
    @staticmethod
    @cached(ttl=SCHEMA_CACHE_TTL)
    def get_all_tables() -> List[Dict[str, Any]]:
        """Get list of all tables in database."""
        inspector = inspect(DatabaseManager.get_engine())
        tables = []
        
        for table_name in inspector.get_table_names():
            try:
                columns = inspector.get_columns(table_name)
                primary_keys = inspector.get_pk_constraint(table_name)
                pk_cols = primary_keys.get('constrained_columns', []) if primary_keys else []
                
                tables.append({
                    'name': table_name,
                    'columns': [
                        {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col.get('nullable', True),
                            'primary_key': col['name'] in pk_cols
                        }
                        for col in columns
                    ],
                    'row_count': SchemaInspector._estimate_row_count(table_name)
                })
            except Exception as e:
                logger.warning(f"Failed to inspect table {table_name}: {e}")
                continue
        
        return tables
    
    @staticmethod
    def _estimate_row_count(table_name: str) -> Optional[int]:
        """Estimate row count for a table."""
        try:
            with DatabaseManager.get_connection() as conn:
                dialect = DatabaseManager.get_dialect_name()
                
                if dialect == 'mssql':
                    # SQL Server optimization using sys.partitions
                    # Note: We use OBJECT_ID to avoid SQL injection, though table_name comes from inspector
                    query = text("SELECT SUM(rows) FROM sys.partitions WHERE object_id = OBJECT_ID(:table_name) AND index_id IN (0,1)")
                    result = conn.execute(query, {'table_name': table_name}).scalar()
                else:
                    # Use generic count query (may be slow for large tables)
                    # Database-specific optimizations could be added here
                    query = text(f"SELECT COUNT(*) FROM {table_name}")
                    result = conn.execute(query).scalar()
                    
                return int(result) if result is not None else None
        except Exception:
            return None
    
    @staticmethod
    def describe_table(table_name: str) -> Dict[str, Any]:
        """Get detailed description of a specific table."""
        inspector = inspect(DatabaseManager.get_engine())
        
        # Validate table exists
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Get column details
        columns = inspector.get_columns(table_name)
        column_details = []
        
        for col in columns:
            column_details.append({
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col.get('nullable', True),
                'default': str(col.get('default', '')) if col.get('default') else '',
                'comment': col.get('comment', '')
            })
        
        # Get sample rows
        sample_rows = SchemaInspector._get_sample_rows(table_name)
        
        # Get indexes
        indexes = inspector.get_indexes(table_name)
        
        # Get foreign keys
        foreign_keys = inspector.get_foreign_keys(table_name)
        
        return {
            'table_name': table_name,
            'columns': column_details,
            'sample_rows': sample_rows,
            'indexes': indexes,
            'foreign_keys': foreign_keys,
            'row_count': SchemaInspector._estimate_row_count(table_name)
        }
    
    @staticmethod
    def _get_sample_rows(table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get sample rows from a table."""
        try:
            with DatabaseManager.get_connection() as conn:
                dialect = DatabaseManager.get_dialect_name()
                
                if dialect == 'mssql':
                    query = text(f"SELECT TOP :limit * FROM {table_name}")
                else:
                    query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                
                result = conn.execute(query, {'limit': limit})
                
                rows = []
                for row in result:
                    # Convert SQLAlchemy Row to dict
                    rows.append(dict(row._mapping))
                return rows
        except Exception as e:
            logger.warning(f"Failed to get sample rows from {table_name}: {e}")
            return []


# ============================================================================
# QUERY EXECUTOR COMPONENT
# ============================================================================

class QueryExecutor:
    """Executes validated SQL queries and formats results."""
    
    @staticmethod
    def execute_query(sql: str) -> Dict[str, Any]:
        """Execute a validated SQL query and return results."""
        start_time = time.time()
        
        try:
            # Get dialect
            dialect = DatabaseManager.get_dialect_name()
            
            # Validate query
            is_safe, modified_sql, warnings = QueryValidator.is_safe_query(sql, dialect)
            if not is_safe:
                raise ValueError(f"Unsafe query: {', '.join(warnings)}")
            
            # Execute query
            with DatabaseManager.get_connection() as conn:
                # Apply timeout if supported by database
                if dialect == 'postgresql':
                    timeout_ms = QUERY_TIMEOUT_SECONDS * 1000
                    conn.execute(text(f"SET statement_timeout = {timeout_ms}"))
                elif dialect == 'mssql':
                    # Set lock timeout (milliseconds)
                    timeout_ms = QUERY_TIMEOUT_SECONDS * 1000
                    conn.execute(text(f"SET LOCK_TIMEOUT {timeout_ms}"))
                
                result = conn.execute(text(modified_sql))
                
                # Convert to pandas DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                # Add performance warnings
                execution_time = (time.time() - start_time) * 1000
                if execution_time > 1000:  # > 1 second
                    warnings.append(f"Query took {execution_time:.0f}ms")
                
                if len(df) == MAX_RESULT_ROWS:
                    warnings.append(
                        f"Result limited to {MAX_RESULT_ROWS} rows. "
                        "Add specific WHERE clause for more precise results."
                    )
                
                # Format results
                markdown_output = QueryExecutor._format_as_markdown(df, modified_sql, warnings)
                
                return {
                    'success': True,
                    'query': modified_sql,
                    'columns': list(df.columns),
                    'row_count': len(df),
                    'limited': ('LIMIT' in modified_sql.upper() and 'LIMIT' not in sql.upper()) or 
                              ('TOP' in modified_sql.upper() and 'TOP' not in sql.upper()),
                    'execution_time_ms': execution_time,
                    'warnings': warnings,
                    'markdown_output': markdown_output
                }
                
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Query execution failed: {e}")
            
            return {
                'success': False,
                'error': str(e),
                'query': sql,
                'execution_time_ms': execution_time,
                'suggestion': QueryExecutor._get_error_suggestion(e)
            }
    
    @staticmethod
    def _format_as_markdown(df: pd.DataFrame, query: str, warnings: List[str]) -> str:
        """Format DataFrame as markdown with appropriate truncation."""
        # Truncate wide tables for readability
        max_cols = 10
        max_rows = 50
        
        if len(df.columns) > max_cols:
            df_display = df.iloc[:, :max_cols].copy()
            warnings.append(f"Table truncated to first {max_cols} of {len(df.columns)} columns")
        else:
            df_display = df.copy()
        
        if len(df) > max_rows:
            df_display = df_display.head(max_rows)
            warnings.append(f"Showing first {max_rows} of {len(df)} rows")
        
        # Convert to markdown
        markdown = "# Query Results\n\n"
        markdown += f"**Query**: `{query}`\n\n"
        markdown += f"**Returned**: {len(df)} row{'s' if len(df) != 1 else ''}\n\n"
        
        if warnings:
            markdown += "**Notes**:\n"
            for warning in warnings:
                markdown += f"- {warning}\n"
            markdown += "\n"
        
        if len(df) > 0:
            markdown += df_display.to_markdown(index=False)
        else:
            markdown += "*No results returned*"
        
        return markdown
    
    @staticmethod
    def _get_error_suggestion(error: Exception) -> str:
        """Get user-friendly suggestion based on error type."""
        error_str = str(error).lower()
        
        if 'table' in error_str and 'not exist' in error_str:
            return "Check the table name using get_schema_info()"
        elif 'column' in error_str and 'not exist' in error_str:
            return "Check column names using describe_table(table_name)"
        elif 'syntax' in error_str:
            return "Verify SQL syntax. Consider simplifying the query."
        elif 'timeout' in error_str:
            return "Query took too long. Add more specific filters or limit results."
        else:
            return "Check the query syntax and try again."


# ============================================================================
# MCP TOOL WRAPPERS
# ============================================================================

# Initialize MCP server
mcp_server = Server("mcp-database-query-server")


@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    """List available MCP tools."""
    return [
        types.Tool(
            name="get_schema_info",
            description="Get information about all tables and columns in the database. "
                       "Returns a list of tables with their columns, data types, and constraints.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        types.Tool(
            name="describe_table",
            description="Get detailed information about a specific table including column types, "
                       "sample data (first 3 rows), and table statistics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe"
                    }
                },
                "required": ["table_name"]
            }
        ),
        types.Tool(
            name="execute_read_only_query",
            description="Execute a read-only SQL query (SELECT or WITH statements only). "
                       "Query is automatically validated for safety and limited to 500 rows if no LIMIT is specified.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "SQL query to execute (must be SELECT or WITH statement)"
                    }
                },
                "required": ["sql_query"]
            }
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Handle tool calls."""
    try:
        if name == "get_schema_info":
            result = tool_get_schema_info()
        elif name == "describe_table":
            table_name = arguments.get("table_name")
            if not table_name:
                raise ValueError("table_name parameter is required")
            result = tool_describe_table(table_name)
        elif name == "execute_read_only_query":
            sql_query = arguments.get("sql_query")
            if not sql_query:
                raise ValueError("sql_query parameter is required")
            result = tool_execute_read_only_query(sql_query)
        else:
            raise ValueError(f"Unknown tool: {name}")
        
        return [types.TextContent(type="text", text=result)]
        
    except Exception as e:
        logger.error(f"Tool execution failed: {e}")
        error_msg = f"# Error\n\n**Tool**: {name}\n\n**Error**: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]


def tool_get_schema_info() -> str:
    """
    Get information about all tables and columns in the database.
    
    Returns:
        Markdown-formatted schema information
    """
    try:
        tables = SchemaInspector.get_all_tables()
        
        if not tables:
            return "No tables found in the database."
        
        # Format as markdown
        output = "# Database Schema\n\n"
        output += f"## Tables ({len(tables)} total)\n\n"
        
        for table in tables:
            output += f"### {table['name']}\n"
            for col in table['columns']:
                constraints = []
                if col['primary_key']:
                    constraints.append("Primary Key")
                if not col['nullable']:
                    constraints.append("Not Null")
                
                constraint_str = f" ({', '.join(constraints)})" if constraints else ""
                output += f"- **{col['name']}**: {col['type']}{constraint_str}\n"
            
            if table.get('row_count') is not None:
                output += f"\n*~{table['row_count']:,} rows*\n"
            
            output += "\n"
        
        return output
        
    except Exception as e:
        logger.error(f"Failed to get schema info: {e}")
        return f"Error retrieving schema information: {str(e)}"


def tool_describe_table(table_name: str) -> str:
    """
    Get detailed information about a specific table.
    
    Args:
        table_name: Name of the table to describe
        
    Returns:
        Markdown-formatted table description with sample rows
    """
    try:
        description = SchemaInspector.describe_table(table_name)
        
        # Format as markdown
        output = f"# Table: {description['table_name']}\n\n"
        
        # Columns section
        output += "## Columns\n"
        output += "| Name | Type | Nullable | Default | Comment |\n"
        output += "|------|------|----------|---------|---------|\ n"
        
        for col in description['columns']:
            nullable = "YES" if col['nullable'] else "NO"
            default = col.get('default', '')
            comment = col.get('comment', '')
            output += f"| {col['name']} | {col['type']} | {nullable} | {default} | {comment} |\n"
        
        output += "\n"
        
        # Sample rows section
        if description['sample_rows']:
            output += f"## Sample Data ({len(description['sample_rows'])} rows)\n"
            
            # Convert sample rows to markdown table
            sample_df = pd.DataFrame(description['sample_rows'])
            output += sample_df.to_markdown(index=False)
            output += "\n\n"
        
        # Statistics section
        output += "## Statistics\n"
        if description.get('row_count') is not None:
            output += f"- **Total rows**: ~{description['row_count']:,}\n"
        if description.get('indexes'):
            output += f"- **Indexes**: {len(description['indexes'])} indexes\n"
        if description.get('foreign_keys'):
            output += f"- **Foreign keys**: {len(description['foreign_keys'])} references\n"
        
        return output
        
    except ValueError as e:
        return f"Error: {str(e)}"
    except Exception as e:
        logger.error(f"Failed to describe table {table_name}: {e}")
        return f"Error describing table: {str(e)}"


def tool_execute_read_only_query(sql_query: str) -> str:
    """
    Execute a read-only SQL query and return results.
    
    Args:
        sql_query: SQL query to execute (must be SELECT or WITH statement)
        
    Returns:
        Markdown-formatted query results
    """
    try:
        result = QueryExecutor.execute_query(sql_query)
        
        if result['success']:
            return result['markdown_output']
        else:
            error_msg = "# Query Error\n\n"
            error_msg += f"**Error**: {result['error']}\n\n"
            if result.get('suggestion'):
                error_msg += f"**Suggestion**: {result['suggestion']}\n\n"
            error_msg += f"**Query**: `{result['query']}`\n"
            return error_msg
            
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return f"Error executing query: {str(e)}"


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point for the MCP server."""
    global logger
    
    try:
        # Load configuration
        config = load_configuration()
        
        # Setup logging
        logger = setup_logging(config['log_level'])
        logger.info("Starting MCP Database Query Server")
        
        # Initialize database
        DatabaseManager.initialize(config)
        logger.info("Database connection established")
        
        # Import and run MCP server
        from mcp.server.stdio import stdio_server
        
        async with stdio_server() as (read_stream, write_stream):
            logger.info("MCP server ready")
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options()
            )
        
    except Exception as e:
        if logger:
            logger.error(f"Server failed to start: {e}")
        else:
            print(f"Server failed to start: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
