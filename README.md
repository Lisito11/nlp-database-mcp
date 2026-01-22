# 🗄️ NLP Database MCP Server

Connect your LLMs to SQL databases safely and intuitively using the Model Context Protocol (MCP). **NLP Database** acts as a secure, read-only bridge that allows AI agents to explore schemas and query data using natural language.

---

## 🚀 Key Features

* **Read-Only Security**: Strict regex validation ensures only `SELECT` and `WITH` statements are executed.
* **Smart Guardrails**: Automatic `LIMIT 500` on all queries to prevent system bloat.
* **Universal Compatibility**: Native support for PostgreSQL, MySQL, SQL Server, and SQLite.
* **Agent-Optimized**: Designed to provide descriptive errors that help LLMs self-correct.
* **Performance**: 5-minute schema caching to reduce database overhead.

---

## Usage Example

Once the server is connected to your LLM (Claude, Gemini, etc.), the agent gains access to two main tools: `get_schema` and `execute_query`.

### Typical Workflow

1. **Exploration**: The user asks a question like: *"How many users signed up last month?"*
2. **Schema Inspection**: The LLM automatically calls `get_schema` to understand your table names and columns.
3. **Query Execution**: The LLM generates a SQL query and calls `execute_query`.
4. **Natural Response**: The LLM receives the data and translates it back to you in plain English or Spanish.

### Example Interaction

**User:**

> "List the top 3 products by total sales revenue."

**LLM (Internal Thought Process):**

1. Call `get_schema` to find relevant tables (finds `products` and `orders`).
2. Generate SQL: `SELECT p.name, SUM(o.amount) FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.name ORDER BY 2 DESC LIMIT 3`.
3. Call `execute_query` with the generated SQL.

**LLM Response:**

> "The top 3 products by revenue are:
> 1. **Enterprise Subscription** ($50,200)
> 2. **Professional License** ($32,150)
> 3. **Basic Plan** ($12,400)"
> 
> 

---

### Available Tools

| Tool | Parameters | Description |
| --- | --- | --- |
| `get_schema` | *(none)* | Returns a list of all tables, their columns, and data types. |
| `execute_query` | `sql_query` | Executes a safe `SELECT` statement and returns the results as JSON. |

---

## 🛠️ 1. Installation & Drivers

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-repo/nlp-database.git
cd nlp-database

```

### Step 2: Install Dependencies

You can install dependencies directly or use a virtual environment (recommended for isolation).

**Option A: Using a Virtual Environment (Recommended)**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

```

**Option B: Direct Installation**

```bash
pip install -r requirements.txt

```

### Step 3: Install Database Drivers

Install only the driver required for your specific database:

* **PostgreSQL**: `pip install psycopg2-binary`
* **MySQL**: `pip install pymysql`
* **SQL Server**: `pip install pyodbc`
* **SQLite**: Already included in Python standard library.

---

## 🔗 2. Connection Strings (`DATABASE_URL`)

| Database | Connection String Format |
| --- | --- |
| **PostgreSQL** | `postgresql://user:pass@localhost:5432/dbname` |
| **MySQL** | `mysql+pymysql://user:pass@localhost:3306/dbname` |
| **SQL Server** | `mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server` |
| **SQLite** | `sqlite:///C:/absolute/path/to/database.db` |

---

## ⚙️ 3. Client Configuration

### A. Claude Code (CLI)

```bash
claude mcp add nlp-database -- python C:/path/to/nlp_database.py --env DATABASE_URL="your_connection_string"

```

### B. Gemini CLI

Add this to your `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "nlp-database": {
      "command": "python",
      "args": ["C:/path/to/nlp_database.py"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost/db"
      }
    }
  }
}

```

### C. Google Antigravity

Locate your `mcp_config.json` (usually in `~/.gemini/antigravity/`):

```json
{
  "mcpServers": {
    "nlp-database": {
      "command": "python",
      "args": ["C:/path/to/nlp_database.py"],
      "env": {
        "DATABASE_URL": "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
      }
    }
  }
}

```

### D. OpenCode

Edit `%USERPROFILE%\.opencode\opencode.jsonc`:

```jsonc
{
  "mcp": {
    "nlp-database": {
      "type": "local",
      "command": "python",
      "args": ["C:/path/to/nlp_database.py"],
      "enabled": true,
      "environment": {
        "DATABASE_URL": "mysql+pymysql://user:pass@localhost/db"
      }
    }
  }
}

```

### E. Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "nlp-database": {
      "command": "python",
      "args": ["C:/path/to/nlp_database.py"],
      "env": {
        "DATABASE_URL": "sqlite:///C:/data/prod.db"
      }
    }
  }
}

```

---

## 🔒 Security: Dedicated Read-Only User

Always use a restricted database user. Here is how to create one:

**PostgreSQL Example:**

```sql
CREATE USER nlp_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE my_db TO nlp_readonly;
GRANT USAGE ON SCHEMA public TO nlp_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO nlp_readonly;

```

---

## 📝 Configuration Options

| Environment Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | *Required* | SQLAlchemy connection string. |
| `MAX_RESULT_ROWS` | `500` | Max rows returned to the LLM. |
| `QUERY_TIMEOUT` | `30` | Max execution time in seconds. |
| `DB_ECHO_SQL` | `false` | Enable to log raw SQL queries to console. |

---
