"""MCP resources providing read-only Databricks workspace context."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import get_workspace_client
from databricks_mcp.utils import format_error, to_json


def register_resources(mcp: FastMCP) -> None:
    """Register workspace info resources."""

    @mcp.resource("databricks://workspace/info")
    def workspace_info() -> str:
        """Get current Databricks workspace information: URL, cloud provider, and current user."""
        try:
            w = get_workspace_client()
            me = w.current_user.me()
            config = w.config

            info = {
                "host": config.host,
                "user": {
                    "user_name": me.user_name,
                    "display_name": me.display_name,
                    "id": me.id,
                },
                "auth_type": config.auth_type,
            }
            return to_json(info)
        except Exception as e:
            return f"Error getting workspace info: {format_error(e)}"
