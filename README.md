# Databricks MCP Server

A comprehensive [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Databricks, built on the official [Databricks Python SDK](https://github.com/databricks/databricks-sdk-py).

Provides **157 tools** across 17 service domains, giving AI assistants full access to the Databricks platform.

## Features

- **SDK-first**: Uses `databricks-sdk` for type safety and automatic API freshness
- **Comprehensive**: Covers Unity Catalog, SQL, Compute, Jobs, Pipelines, Serving, Vector Search, Apps, Lakebase, Dashboards, Genie, Secrets, IAM, Connections, Experiments, and Delta Sharing
- **Zero custom auth**: Delegates authentication entirely to the SDK (PAT, OAuth, Azure AD, service principal -- all automatic)
- **Selective loading**: Include/exclude tool modules via environment variables
- **MCP Resources**: Read-only workspace context (URL, current user, auth type)

## Quick Start

### Installation

```bash
pip install databricks-mcp-server
```

Or install from source:

```bash
git clone https://github.com/your-org/databricks-mcp-server.git
cd databricks-mcp-server
pip install -e ".[dev]"
```

### Authentication

Authentication is handled by the Databricks SDK. Set one of:

**Personal Access Token (simplest):**

```bash
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_TOKEN=dapi...
```

**OAuth (M2M):**

```bash
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_CLIENT_ID=...
export DATABRICKS_CLIENT_SECRET=...
```

**Other methods**: Azure AD, Databricks CLI profile, Azure Managed Identity -- all auto-detected by the SDK.

### Running

```bash
databricks-mcp
```

This starts the MCP server using stdio transport.

### Claude Code Integration

Add to your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.databricks.com",
        "DATABRICKS_TOKEN": "dapi..."
      }
    }
  }
}
```

## Tool Modules

| Module | Tools | Description |
|--------|-------|-------------|
| `unity_catalog` | 19 | Catalogs, schemas, tables, volumes, functions, models |
| `sql` | 14 | Warehouses, SQL execution, queries, alerts, history |
| `workspace` | 10 | Notebooks, files, repos |
| `compute` | 10 | Clusters, instance pools, policies |
| `jobs` | 10 | Jobs, runs, tasks |
| `pipelines` | 8 | DLT / Lakeflow pipelines |
| `serving` | 9 | Serving endpoints, model versions |
| `vector_search` | 8 | Vector search endpoints and indexes |
| `apps` | 10 | Databricks Apps lifecycle |
| `database` | 10 | Lakebase PostgreSQL instances |
| `dashboards` | 8 | Lakeview AI/BI dashboards |
| `genie` | 5 | Genie AI/BI conversations |
| `secrets` | 8 | Secret scopes and secrets |
| `iam` | 10 | Users, groups, service principals, permissions |
| `connections` | 5 | External connections |
| `experiments` | 8 | MLflow experiments and runs |
| `sharing` | 5 | Delta Sharing providers and recipients |

## Selective Tool Loading

Control which tool modules are loaded via environment variables:

```bash
# Only include specific modules
export DATABRICKS_MCP_TOOLS_INCLUDE=unity_catalog,sql,serving

# Exclude specific modules (cannot combine with INCLUDE)
export DATABRICKS_MCP_TOOLS_EXCLUDE=iam,sharing,experiments
```

If `INCLUDE` is set, only those modules load. If `EXCLUDE` is set, everything except those modules loads. `INCLUDE` takes precedence if both are set.

## MCP Resources

| URI | Description |
|-----|-------------|
| `databricks://workspace/info` | Workspace URL, current user, auth type |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Lint
ruff check databricks_mcp/

# Test
pytest tests/ -v
```

## Author

**Pramod Bhat**

## License

Apache 2.0 -- see [LICENSE](LICENSE).
