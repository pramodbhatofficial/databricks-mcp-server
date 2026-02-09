"""Delta Sharing management tools for Databricks MCP.

Provides tools for managing shares and recipients in Delta Sharing.
Shares define collections of tables and schemas that can be shared with
external organizations, while recipients represent the external parties
who consume the shared data.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import get_workspace_client
from databricks_mcp.utils import format_error, paginate, to_json


def register_tools(mcp: FastMCP) -> None:
    """Register all Delta Sharing tools with the MCP server."""

    # -- Shares ----------------------------------------------------------------

    @mcp.tool()
    def databricks_list_shares() -> str:
        """List all Delta Sharing shares in the metastore.

        Shares are named objects that contain a collection of tables and
        schemas from one or more catalogs. They define what data is available
        for sharing with external recipients.

        Returns:
            JSON array of share objects, each containing name, owner,
            comment, and created/updated timestamps. Results are capped
            at 100 items.
        """
        try:
            w = get_workspace_client()
            results = paginate(w.shares.list())
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_share(name: str) -> str:
        """Get detailed information about a specific share.

        Returns the share definition including all tables and schemas that
        are part of the share.

        Args:
            name: The name of the share to retrieve.

        Returns:
            JSON object with full share details including name, owner,
            comment, objects (shared tables and schemas), created_at,
            and updated_at.
        """
        try:
            w = get_workspace_client()
            result = w.shares.get(name)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_share(name: str, comment: str = "") -> str:
        """Create a new Delta Sharing share.

        Creates an empty share that can later have tables and schemas added
        to it. The caller must have the CREATE_SHARE privilege on the
        metastore.

        Args:
            name: Unique name for the new share within the metastore.
            comment: Optional human-readable description of the share and
                     the data it will contain.

        Returns:
            JSON object with the created share's details.
        """
        try:
            w = get_workspace_client()
            result = w.shares.create(name=name, comment=comment)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    # -- Recipients ------------------------------------------------------------

    @mcp.tool()
    def databricks_list_recipients() -> str:
        """List all Delta Sharing recipients in the metastore.

        Recipients represent external organizations or users who are authorized
        to consume shared data. Each recipient has an authentication type and
        associated credentials or tokens.

        Returns:
            JSON array of recipient objects, each containing name,
            authentication_type, comment, sharing_code (if TOKEN auth),
            and activation status. Results are capped at 100 items.
        """
        try:
            w = get_workspace_client()
            results = paginate(w.recipients.list())
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_recipient(
        name: str,
        authentication_type: str = "TOKEN",
        comment: str = "",
    ) -> str:
        """Create a new Delta Sharing recipient.

        Creates a recipient that can be granted access to shares. The caller
        must have the CREATE_RECIPIENT privilege on the metastore.

        Args:
            name: Unique name for the recipient within the metastore.
            authentication_type: Authentication method for the recipient.
                                 Valid values:
                                 - "TOKEN" (default): Bearer token authentication.
                                   A sharing token is generated automatically and
                                   must be sent to the recipient out of band.
                                 - "DATABRICKS": Databricks-to-Databricks sharing.
                                   The recipient authenticates using their own
                                   Databricks identity.
            comment: Optional human-readable description of the recipient and
                     the organization they represent.

        Returns:
            JSON object with the created recipient's details, including the
            activation URL and sharing token (for TOKEN authentication).
        """
        try:
            from databricks.sdk.service.sharing import AuthenticationType

            w = get_workspace_client()
            result = w.recipients.create(
                name=name,
                authentication_type=AuthenticationType[authentication_type],
                comment=comment,
            )
            return to_json(result)
        except Exception as e:
            return format_error(e)
