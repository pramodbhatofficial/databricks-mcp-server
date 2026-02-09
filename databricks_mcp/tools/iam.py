"""Identity and access management tools for Databricks MCP.

Provides tools for managing users, groups, service principals, and
permissions within the Databricks workspace. Uses the SCIM 2.0 API
for identity management and the Permissions API for object-level
access control.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import get_workspace_client
from databricks_mcp.utils import format_error, paginate, to_json


def register_tools(mcp: FastMCP) -> None:
    """Register all IAM tools with the MCP server."""

    # -- Users -----------------------------------------------------------------

    @mcp.tool()
    def databricks_list_users(filter_str: str = "", count: int = 100) -> str:
        """List workspace users with optional SCIM filtering.

        Uses the SCIM 2.0 API to list users. Supports SCIM filter expressions
        for server-side filtering (e.g. 'displayName co "john"').

        Args:
            filter_str: Optional SCIM filter expression to narrow results.
                        Examples:
                        - 'userName eq "user@example.com"' (exact match)
                        - 'displayName co "john"' (contains)
                        - 'active eq true' (only active users)
                        Leave empty to list all users.
            count: Maximum number of users to return (default 100).

        Returns:
            JSON array of user objects, each containing id, userName,
            displayName, active status, and group memberships.
        """
        try:
            w = get_workspace_client()
            if filter_str:
                results = paginate(w.users.list(filter=filter_str, count=count), max_items=count)
            else:
                results = paginate(w.users.list(count=count), max_items=count)
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_user(user_id: str) -> str:
        """Get detailed information about a specific user.

        Args:
            user_id: The numeric ID of the user to retrieve (as returned by
                     list_users or create_user).

        Returns:
            JSON object with full user details including id, userName,
            displayName, active status, emails, groups, and roles.
        """
        try:
            w = get_workspace_client()
            result = w.users.get(user_id)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_user(user_name: str, display_name: str = "") -> str:
        """Create a new user in the workspace.

        The caller must be a workspace admin. The user_name is typically the
        user's email address and must be unique within the workspace.

        Args:
            user_name: The user's email address, used as their login identity.
                       Must be unique within the workspace.
            display_name: Optional human-readable display name. Defaults to the
                          user_name if not provided.

        Returns:
            JSON object with the created user's details including their
            assigned numeric id.
        """
        try:
            w = get_workspace_client()
            result = w.users.create(
                user_name=user_name,
                display_name=display_name or user_name,
            )
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_delete_user(user_id: str) -> str:
        """Delete a user from the workspace.

        The caller must be a workspace admin. This removes the user's access
        but does not delete resources they own.

        Args:
            user_id: The numeric ID of the user to delete.

        Returns:
            Confirmation message on success.
        """
        try:
            w = get_workspace_client()
            w.users.delete(user_id)
            return f"User '{user_id}' deleted successfully."
        except Exception as e:
            return format_error(e)

    # -- Groups ----------------------------------------------------------------

    @mcp.tool()
    def databricks_list_groups(filter_str: str = "", count: int = 100) -> str:
        """List workspace groups with optional SCIM filtering.

        Groups are used to manage permissions for collections of users and
        service principals.

        Args:
            filter_str: Optional SCIM filter expression to narrow results.
                        Examples:
                        - 'displayName eq "admins"' (exact match)
                        - 'displayName co "eng"' (contains)
                        Leave empty to list all groups.
            count: Maximum number of groups to return (default 100).

        Returns:
            JSON array of group objects, each containing id, displayName,
            members, and roles.
        """
        try:
            w = get_workspace_client()
            if filter_str:
                results = paginate(w.groups.list(filter=filter_str, count=count), max_items=count)
            else:
                results = paginate(w.groups.list(count=count), max_items=count)
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_group(display_name: str) -> str:
        """Create a new group in the workspace.

        The caller must be a workspace admin. Groups are used to organize
        users and service principals for permission management.

        Args:
            display_name: The display name for the new group. Must be unique
                          within the workspace.

        Returns:
            JSON object with the created group's details including its
            assigned numeric id.
        """
        try:
            w = get_workspace_client()
            result = w.groups.create(display_name=display_name)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_delete_group(group_id: str) -> str:
        """Delete a group from the workspace.

        The caller must be a workspace admin. This removes the group but does
        not delete its member users or service principals.

        Args:
            group_id: The numeric ID of the group to delete.

        Returns:
            Confirmation message on success.
        """
        try:
            w = get_workspace_client()
            w.groups.delete(group_id)
            return f"Group '{group_id}' deleted successfully."
        except Exception as e:
            return format_error(e)

    # -- Service Principals ----------------------------------------------------

    @mcp.tool()
    def databricks_list_service_principals(filter_str: str = "", count: int = 100) -> str:
        """List service principals in the workspace with optional SCIM filtering.

        Service principals are machine identities used for automated processes
        and CI/CD pipelines.

        Args:
            filter_str: Optional SCIM filter expression to narrow results.
                        Examples:
                        - 'displayName eq "my-pipeline"'
                        - 'applicationId eq "00000000-0000-0000-0000-000000000000"'
                        Leave empty to list all service principals.
            count: Maximum number of service principals to return (default 100).

        Returns:
            JSON array of service principal objects, each containing id,
            displayName, applicationId, and active status.
        """
        try:
            w = get_workspace_client()
            if filter_str:
                results = paginate(
                    w.service_principals.list(filter=filter_str, count=count),
                    max_items=count,
                )
            else:
                results = paginate(w.service_principals.list(count=count), max_items=count)
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_service_principal(display_name: str, application_id: str = "") -> str:
        """Create a new service principal in the workspace.

        Service principals are non-human identities used for automation,
        CI/CD, and application integrations. The caller must be a workspace
        admin.

        Args:
            display_name: A human-readable name for the service principal.
            application_id: Optional UUID for the Azure AD application associated
                            with this service principal. If not provided, one
                            will be auto-generated by the platform.

        Returns:
            JSON object with the created service principal's details including
            its assigned numeric id and applicationId.
        """
        try:
            w = get_workspace_client()
            kwargs: dict = {"display_name": display_name}
            if application_id:
                kwargs["application_id"] = application_id
            result = w.service_principals.create(**kwargs)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    # -- Permissions -----------------------------------------------------------

    @mcp.tool()
    def databricks_get_permission_levels(object_type: str, object_id: str) -> str:
        """Get the permission levels available for a workspace object.

        Returns all possible permission levels that can be granted on the
        specified object type. Use this to discover valid permission levels
        before setting permissions.

        Args:
            object_type: The type of object to query permission levels for.
                         Common values: "clusters", "cluster-policies",
                         "directories", "experiments", "jobs",
                         "notebooks", "pipelines", "registered-models",
                         "repos", "serving-endpoints", "sql/warehouses",
                         "tokens".
            object_id: The ID of the specific object to query permission
                       levels for. Format varies by object type.

        Returns:
            JSON object containing an array of available permission levels,
            each with a permission_level name and description.
        """
        try:
            w = get_workspace_client()
            result = w.permissions.get_permission_levels(
                request_object_type=object_type,
                request_object_id=object_id,
            )
            return to_json(result)
        except Exception as e:
            return format_error(e)
