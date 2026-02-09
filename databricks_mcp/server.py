"""Databricks MCP Server — comprehensive tool access to all Databricks services."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import is_module_enabled

mcp = FastMCP(
    "databricks",
    description="Comprehensive MCP server for Databricks. Provides tools for Unity Catalog, SQL, "
    "compute, jobs, pipelines, serving endpoints, vector search, apps, Lakebase, dashboards, "
    "Genie, secrets, IAM, connections, experiments, and Delta Sharing.",
)

# Register tool modules conditionally based on DATABRICKS_MCP_TOOLS_INCLUDE/EXCLUDE
_TOOL_MODULES = [
    ("unity_catalog", "databricks_mcp.tools.unity_catalog"),
    ("sql", "databricks_mcp.tools.sql"),
    ("workspace", "databricks_mcp.tools.workspace"),
    ("compute", "databricks_mcp.tools.compute"),
    ("jobs", "databricks_mcp.tools.jobs"),
    ("pipelines", "databricks_mcp.tools.pipelines"),
    ("serving", "databricks_mcp.tools.serving"),
    ("vector_search", "databricks_mcp.tools.vector_search"),
    ("apps", "databricks_mcp.tools.apps"),
    ("database", "databricks_mcp.tools.database"),
    ("dashboards", "databricks_mcp.tools.dashboards"),
    ("genie", "databricks_mcp.tools.genie"),
    ("secrets", "databricks_mcp.tools.secrets"),
    ("iam", "databricks_mcp.tools.iam"),
    ("connections", "databricks_mcp.tools.connections"),
    ("experiments", "databricks_mcp.tools.experiments"),
    ("sharing", "databricks_mcp.tools.sharing"),
]


def _register_tools() -> None:
    """Import and register all enabled tool modules."""
    import importlib

    for module_name, module_path in _TOOL_MODULES:
        if is_module_enabled(module_name):
            try:
                mod = importlib.import_module(module_path)
                # Each module has a register_tools(mcp) function
                if hasattr(mod, "register_tools"):
                    mod.register_tools(mcp)
            except ImportError as e:
                import sys

                print(f"Warning: Failed to import {module_path}: {e}", file=sys.stderr)

    # Always register resources
    from databricks_mcp.resources.workspace_info import register_resources

    register_resources(mcp)


def main() -> None:
    """Entry point for the databricks-mcp command."""
    _register_tools()
    mcp.run()


if __name__ == "__main__":
    main()
