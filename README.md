# Doris MCP Server

Doris MCP (Model Control Panel) Server is a backend service built with Python and FastAPI. It implements the MCP (Model Control Panel) protocol, allowing clients to interact with it through defined "Tools". It's primarily designed to connect to Apache Doris databases, potentially leveraging Large Language Models (LLMs) for tasks like converting natural language queries to SQL (NL2SQL), executing queries, and performing metadata management and analysis.

## Core Features

*   **MCP Protocol Implementation**: Provides standard MCP interfaces, supporting tool calls, resource management, and prompt interactions.
*   **Multiple Communication Modes**:
    *   **SSE (Server-Sent Events)**: Served via `/sse` (initialization) and `/mcp/messages` (communication) endpoints (`src/sse_server.py`).
    *   **Streamable HTTP**: Served via the unified `/mcp` endpoint, supporting request/response and streaming (`src/streamable_server.py`).
    *   **(Optional) Stdio**: Interaction possible via standard input/output (`src/stdio_server.py`), requires specific startup configuration.
*   **Tool-Based Interface**: Core functionalities are encapsulated as MCP tools that clients can call as needed. Currently available key tools focus on direct database interaction:
    *   SQL Execution (`mcp_doris_exec_query`)
    *   Database and Table Listing (`mcp_doris_get_db_list`, `mcp_doris_get_db_table_list`)
    *   Metadata Retrieval (`mcp_doris_get_table_schema`, `mcp_doris_get_table_comment`, `mcp_doris_get_table_column_comments`, `mcp_doris_get_table_indexes`)
    *   Audit Log Retrieval (`mcp_doris_get_recent_audit_logs`)
    *Note: Current tools primarily focus on direct DB operations.*
*   **Database Interaction**: Provides functionality to connect to Apache Doris (or other compatible databases) and execute queries (`src/utils/db.py`).
*   **Flexible Configuration**: Configured via a `.env` file, supporting settings for database connections, LLM providers/models, API keys, logging levels, etc.
*   **Metadata Extraction**: Capable of extracting database metadata information (`src/utils/schema_extractor.py`).

## System Requirements

*   Python 3.12+
*   Database connection details (e.g., Doris Host, Port, User, Password, Database)

## Quick Start

### 1. Clone the Repository

```bash
# Replace with the actual repository URL if different
git clone https://github.com/apache/doris-mcp-server.git
cd doris-mcp-server
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the `.env.example` file to `.env` and modify the settings according to your environment:

```bash
cp env.example .env
```

**Key Environment Variables:**

*   **Database Connection**:
    *   `DB_HOST`: Database hostname
    *   `DB_PORT`: Database port (default 9030)
    *   `DB_USER`: Database username
    *   `DB_PASSWORD`: Database password
    *   `DB_DATABASE`: Default database name
*   **Server Configuration**:
    *   `SERVER_HOST`: Host address the server listens on (default `0.0.0.0`)
    *   `SERVER_PORT`: Port the server listens on (default `3000`)
    *   `ALLOWED_ORIGINS`: CORS allowed origins (comma-separated, `*` allows all)
    *   `MCP_ALLOW_CREDENTIALS`: Whether to allow CORS credentials (default `false`)
*   **Logging Configuration**:
    *   `LOG_DIR`: Directory for log files (default `./logs`)
    *   `LOG_LEVEL`: Log level (e.g., `INFO`, `DEBUG`, `WARNING`, `ERROR`, default `INFO`)
    *   `CONSOLE_LOGGING`: Whether to output logs to the console (default `false`)

### Available MCP Tools

The following table lists the main tools currently available for invocation via an MCP client:

| Tool Name                         | Description                                                 | Parameters                                                                                                 | Status   |
| :-------------------------------- | :---------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------- | :------- |
| `mcp_doris_get_db_list`           | Get a list of all database names on the server.             | `random_string` (string, Required)                                                                         | ✅ Active |
| `mcp_doris_get_db_table_list`     | Get a list of all table names in the specified database.    | `random_string` (string, Required), `db_name` (string, Optional, defaults to current db)                   | ✅ Active |
| `mcp_doris_get_table_schema`      | Get detailed structure of the specified table.              | `random_string` (string, Required), `table_name` (string, Required), `db_name` (string, Optional)           | ✅ Active |
| `mcp_doris_get_table_comment`     | Get the comment for the specified table.                    | `random_string` (string, Required), `table_name` (string, Required), `db_name` (string, Optional)           | ✅ Active |
| `mcp_doris_get_table_column_comments` | Get comments for all columns in the specified table.      | `random_string` (string, Required), `table_name` (string, Required), `db_name` (string, Optional)           | ✅ Active |
| `mcp_doris_get_table_indexes`     | Get index information for the specified table.              | `random_string` (string, Required), `table_name` (string, Required), `db_name` (string, Optional)           | ✅ Active |
| `mcp_doris_exec_query`            | Execute SQL query and return result command.                | `random_string` (string, Required), `sql` (string, Required), `db_name` (string, Optional), `max_rows` (integer, Optional, default 100), `timeout` (integer, Optional, default 30) | ✅ Active |
| `mcp_doris_get_recent_audit_logs` | Get audit log records for a recent period.                  | `random_string` (string, Required), `days` (integer, Optional, default 7), `limit` (integer, Optional, default 100) | ✅ Active |

**Note:** All tools require a `random_string` parameter as a call identifier, typically handled automatically by the MCP client. "Optional" and "Required" refer to the tool's internal logic; the client might need to provide values for all parameters depending on its implementation. The tool names listed here are the base names; clients might see them prefixed (e.g., `mcp_doris_stdio3_get_db_list`) depending on the connection mode.

### 4. Run the Service

If you use SSE mode, execute the following command:

```bash
./start_server.sh
```

This command starts the FastAPI application, providing both SSE and Streamable HTTP MCP services by default.

**Service Endpoints:**

*   **SSE Initialization**: `http://<host>:<port>/sse`
*   **SSE Communication**: `http://<host>:<port>/mcp/messages` (POST)
*   **Streamable HTTP**: `http://<host>:<port>/mcp` (Supports GET, POST, DELETE, OPTIONS)
*   **Health Check**: `http://<host>:<port>/health`
*   **(Potential) Status Check**: `http://<host>:<port>/status` (Confirm if implemented in `main.py`)

## Usage

Interaction with the Doris MCP Server requires an **MCP Client**. The client connects to the server's SSE or Streamable HTTP endpoints and sends requests (like `tool_call`) according to the MCP specification to invoke the server's tools.

**Main Interaction Flow:**

1.  **Client Initialization**: Connect to `/sse` (SSE) or send an `initialize` method call to `/mcp` (Streamable).
2.  **(Optional) Discover Tools**: The client can call `mcp/listTools` or `mcp/listOfferings` to get the list of supported tools, their descriptions, and parameter schemas.
3.  **Call Tool**: The client sends a `tool_call` message/request, specifying the `tool_name` and `arguments`.
    *   **Example: Get Table Schema**
        *   `tool_name`: `mcp_doris_get_table_schema` (or the mode-specific name)
        *   `arguments`: Include `random_string`, `table_name`, `db_name`.
4.  **Handle Response**:
    *   **Non-streaming**: The client receives a response containing `result` or `error`.
    *   **Streaming**: The client receives a series of `tools/progress` notifications, followed by a final response containing the `result` or `error`.

Specific tool names and parameters should be referenced from the `src/tools/` code or obtained via MCP discovery mechanisms.

## Connecting with Cursor

You can connect Cursor to this MCP server using either Stdio or SSE mode.

### Stdio Mode

Stdio mode allows Cursor to manage the server process directly. Configuration is done within Cursor's MCP Server settings file (typically `~/.cursor/mcp.json` or similar).

If you use stdio mode, please execute the following command to download and build the environment dependency package, **but please note that you need to change the project path to the correct path address**:

```bash
uv --project /your/path/doris-mcp-server run doris-mcp
```

1.  **Configure Cursor:** Add an entry like the following to your Cursor MCP configuration:

    ```json
    {
      "mcpServers": {
        "doris-stdio": {
          "command": "uv",
          "args": ["--project", "/path/to/your/doris-mcp-server", "run", "doris-mcp"],
          "env": {
            "DB_HOST": "127.0.0.1",
            "DB_PORT": "9030",
            "DB_USER": "root",
            "DB_PASSWORD": "your_db_password",
            "DB_DATABASE": "your_default_db" 
          }
        },
        // ... other server configurations ...
      }
    }
    ```

2.  **Key Points:**
    *   Replace `/path/to/your/doris-mcp` with the actual absolute path to the project's root directory on your system. The `--project` argument is crucial for `uv` to find the `pyproject.toml` and run the correct command.
    *   The `command` is set to `uv` (assuming you use `uv` for package management as indicated by `uv.lock`). The `args` include `--project`, the path, `run`, and `mcp-doris` (which should correspond to a script defined in your `pyproject.toml`).
    *   Database connection details (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_DATABASE`) are set directly in the `env` block within the configuration file. Cursor will pass these to the server process. No `.env` file is needed for this mode when configured via Cursor.

### SSE Mode

SSE mode requires you to run the MCP server independently first, and then tell Cursor how to connect to it.

1.  **Configure `.env`:** Ensure your database credentials and any other necessary settings (like `SERVER_PORT` if not using the default 3000) are correctly configured in the `.env` file within the project directory.
2.  **Start the Server:** Run the server from your terminal in the project's root directory:
    ```bash
    ./start_server.sh
    ```
    This script typically reads the `.env` file and starts the FastAPI server in SSE mode (check the script and `sse_server.py` / `main.py` for specifics). Note the host and port the server is listening on (default is `0.0.0.0:3000`).
3.  **Configure Cursor:** Add an entry like the following to your Cursor MCP configuration, pointing to the running server's SSE endpoint:

    ```json
    {
      "mcpServers": {
        "doris-sse": {
           "url": "http://127.0.0.1:3000/sse" // Adjust host/port if your server runs elsewhere
        },
        // ... other server configurations ...
      }
    }
    ```
    *Note: The example uses the default port `3000`. If your server is configured to run on a different port (like `3010` in the user example), adjust the URL accordingly.*

After configuring either mode in Cursor, you should be able to select the server (e.g., `doris-stdio` or `doris-sse`) and use its tools.

## Directory Structure

```
doris-mcp-server/
├── doris_mcp_server/    # Source code for the MCP server
│   ├── main.py          # Main entry point, FastAPI app definition
│   ├── mcp_core.py      # Core MCP tool registration and Stdio handling
│   ├── sse_server.py    # SSE server implementation
│   ├── streamable_server.py # Streamable HTTP server implementation
│   ├── config.py        # Configuration loading
│   ├── tools/           # MCP tool definitions
│   │   ├── mcp_doris_tools.py # Main Doris-related MCP tools
│   │   ├── tool_initializer.py # Tool registration helper (used by mcp_core.py)
│   │   └── __init__.py
│   ├── utils/           # Utility classes and helper functions
│   │   ├── db.py              # Database connection and operations
│   │   ├── logger.py          # Logging configuration
│   │   ├── schema_extractor.py # Doris metadata/schema extraction logic
│   │   ├── sql_executor_tools.py # SQL execution helper (might be legacy)
│   │   └── __init__.py
│   └── __init__.py
├── logs/                # Log file directory (if file logging enabled)
├── README.md            # This file
├── .env.example         # Example environment variable file
├── requirements.txt     # Python dependencies for pip
├── pyproject.toml       # Project metadata and build system configuration (PEP 518)
├── uv.lock              # Lock file for 'uv' package manager (alternative to pip)
├── start_server.sh      # Script to start the server
└── restart_server.sh    # Script to restart the server
```

## Developing New Tools

This section outlines the process for adding new MCP tools to the Doris MCP Server, considering the current project structure.

### 1. Leverage Utility Modules

Before writing new database interaction logic from scratch, check the existing utility modules:

*   **`doris_mcp_server/utils/db.py`**: Provides basic functions for getting database connections (`get_db_connection`) and executing raw queries (`execute_query`, `execute_query_df`).
*   **`doris_mcp_server/utils/schema_extractor.py` (`MetadataExtractor` class)**: Offers high-level methods to retrieve database metadata, such as listing databases/tables (`get_all_databases`, `get_database_tables`), getting table schemas/comments/indexes (`get_table_schema`, `get_table_comment`, `get_column_comments`, `get_table_indexes`), and accessing audit logs (`get_recent_audit_logs`). It includes caching mechanisms.
*   **`doris_mcp_server/utils/sql_executor_tools.py` (`execute_sql_query` function)**: Provides a wrapper around `db.execute_query` that includes security checks (optional, controlled by `ENABLE_SQL_SECURITY_CHECK` env var), adds automatic `LIMIT` to SELECT queries, handles result serialization (dates, decimals), and formats the output into the standard MCP success/error structure. **It's recommended to use this for executing user-provided or generated SQL.**

You can import and combine functionalities from these modules to build your new tool.

### 2. Implement Tool Logic

Implement the core logic for your new tool as an `async` function within `doris_mcp_server/tools/mcp_doris_tools.py`. This keeps the primary tool implementations centralized. Ensure your function returns data in a format that can be easily wrapped into the standard MCP response structure (see `_format_response` in the same file for reference).

**Example:** Let's create a simple tool `get_server_time`.

```python
# In doris_mcp_server/tools/mcp_doris_tools.py
import datetime
# ... other imports ...
from doris_mcp_server.tools.mcp_doris_tools import _format_response # Reuse formatter

# ... existing tools ...

async def mcp_doris_get_server_time() -> Dict[str, Any]:
    """Gets the current server time."""
    logger.info(f"MCP Tool Call: mcp_doris_get_server_time")
    try:
        current_time = datetime.datetime.now().isoformat()
        # Use the existing formatter for consistency
        return _format_response(success=True, result={"server_time": current_time})
    except Exception as e:
        logger.error(f"MCP tool execution failed mcp_doris_get_server_time: {str(e)}", exc_info=True)
        return _format_response(success=False, error=str(e), message="Error getting server time")

```

### 3. Register the Tool (Dual Registration)

Due to the separate handling of SSE/Streamable and Stdio modes, you need to register the tool in two places:

**A. SSE/Streamable Registration (`tool_initializer.py`)**

*   Import your new tool function from `mcp_doris_tools.py`.
*   Inside the `register_mcp_tools` function, add a new wrapper function decorated with `@mcp.tool()`.
*   The wrapper function should call your core tool function.
*   Define the tool name and provide a detailed description (including parameters if any) in the decorator. Remember to include the mandatory `random_string` parameter description for client compatibility, even if your wrapper doesn't explicitly use it.

**Example (`tool_initializer.py`):**

```python
# In doris_mcp_server/tools/tool_initializer.py
# ... other imports ...
from doris_mcp_server.tools.mcp_doris_tools import (
    # ... existing tool imports ...
    mcp_doris_get_server_time # <-- Import the new tool
)

async def register_mcp_tools(mcp):
    # ... existing tool registrations ...

    # Register Tool: Get Server Time
    @mcp.tool("get_server_time", description="""[Function Description]: Get the current time of the MCP server.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n""")
    async def get_server_time_tool() -> Dict[str, Any]:
        """Wrapper: Get server time"""
        # Note: No parameters needed for the core function call here
        return await mcp_doris_get_server_time()

    # ... logging registration count ...
```

**B. Stdio Registration (`mcp_core.py`)**

*   Similar to SSE, add a new wrapper function decorated with `@stdio_mcp.tool()`.
*   **Important:** Import your core tool function (`mcp_doris_get_server_time`) *inside* the wrapper function (delayed import pattern used in this file).
*   The wrapper calls the core tool function. The wrapper itself *might* need to be `async def` depending on how `FastMCP` handles tools in Stdio mode, even if the underlying function is simple (as seen in the current file structure). Ensure the call matches (e.g., use `await` if calling an async function).

**Example (`mcp_core.py`):**

```python
# In doris_mcp_server/mcp_core.py
# ... other imports and setup ...

# ... existing Stdio tool registrations ...

# Register Tool: Get Server Time (for Stdio)
@stdio_mcp.tool("get_server_time", description="""[Function Description]: Get the current time of the MCP server.\n
[Parameter Content]:\n
- random_string (string) [Required] - Unique identifier for the tool call\n""")
async def get_server_time_tool_stdio() -> Dict[str, Any]: # Using a slightly different wrapper name for clarity if needed
    """Wrapper: Get server time (Stdio)"""
    from doris_mcp_server.tools.mcp_doris_tools import mcp_doris_get_server_time # <-- Delayed import
    # Assuming the Stdio runner handles async wrappers correctly
    return await mcp_doris_get_server_time()

# --- Register Tools --- (Or wherever the registrations are finalized)
```

### 4. Restart and Test

After implementing and registering the tool in both files, restart the MCP server (both SSE mode via `./start_server.sh` and ensure the Stdio command used by Cursor is updated if necessary) and test the new tool using your MCP client (like Cursor) in both connection modes.

## Contributing

Contributions are welcome via Issues or Pull Requests.

## License

This project is licensed under the Apache 2.0 License. See the LICENSE file (if it exists) for details. 
