"""Compute cluster and instance pool tools for Databricks MCP.

Provides tools for managing all-purpose and job compute clusters,
instance pools, and cluster policies.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import get_workspace_client
from databricks_mcp.utils import format_error, paginate, to_json


def register_tools(mcp: FastMCP) -> None:
    """Register all compute tools with the MCP server."""

    # ── Clusters ────────────────────────────────────────────────────────

    @mcp.tool()
    def databricks_list_clusters() -> str:
        """List all compute clusters in the workspace.

        Returns both running and terminated clusters, including all-purpose
        and job clusters. Each entry includes the cluster's current state.

        Returns:
            JSON array of cluster objects with cluster_id, cluster_name,
            state, spark_version, node_type_id, and autoscale config.
            Results are capped at 100 items.
        """
        try:
            w = get_workspace_client()
            results = paginate(w.clusters.list())
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_cluster(cluster_id: str) -> str:
        """Get detailed information about a specific compute cluster.

        Args:
            cluster_id: The unique identifier of the cluster.

        Returns:
            JSON object with full cluster details including cluster_name,
            state, state_message, spark_version, node_type_id, driver_node_type_id,
            num_workers, autoscale, spark_conf, and runtime information.
        """
        try:
            w = get_workspace_client()
            result = w.clusters.get(cluster_id)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_cluster(
        cluster_name: str,
        spark_version: str,
        node_type_id: str,
        num_workers: int = 0,
        autoscale_min: int = 0,
        autoscale_max: int = 0,
    ) -> str:
        """Create a new compute cluster.

        Creates an all-purpose cluster with either a fixed number of workers
        or autoscaling. If autoscale_min and autoscale_max are both greater
        than 0, autoscaling is enabled and num_workers is ignored. Otherwise,
        a fixed-size cluster is created with num_workers.

        Args:
            cluster_name: Display name for the cluster.
            spark_version: Spark runtime version string
                           (e.g. "14.3.x-scala2.12", "15.0.x-scala2.12").
                           Use the Spark version key from your workspace.
            node_type_id: Instance type for worker nodes
                          (e.g. "i3.xlarge", "Standard_DS3_v2").
            num_workers: Number of worker nodes for a fixed-size cluster.
                         Ignored if autoscaling is configured. Use 0 for
                         single-node clusters.
            autoscale_min: Minimum number of workers when autoscaling is enabled.
                           Set both autoscale_min and autoscale_max > 0 to enable.
            autoscale_max: Maximum number of workers when autoscaling is enabled.
                           Set both autoscale_min and autoscale_max > 0 to enable.

        Returns:
            JSON object with the created cluster details including cluster_id.
        """
        try:
            from databricks.sdk.service.compute import AutoScale

            w = get_workspace_client()
            kwargs = {
                "cluster_name": cluster_name,
                "spark_version": spark_version,
                "node_type_id": node_type_id,
            }

            # Use autoscaling if both min and max are specified and positive
            if autoscale_min > 0 and autoscale_max > 0:
                kwargs["autoscale"] = AutoScale(
                    min_workers=autoscale_min,
                    max_workers=autoscale_max,
                )
            else:
                kwargs["num_workers"] = num_workers

            result = w.clusters.create(**kwargs)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_start_cluster(cluster_id: str) -> str:
        """Start a terminated compute cluster.

        Initiates the start process for a previously terminated cluster.
        The cluster may take several minutes to become fully running.
        This call returns immediately without blocking.

        Args:
            cluster_id: The unique identifier of the cluster to start.

        Returns:
            Confirmation that the start was initiated.
        """
        try:
            w = get_workspace_client()
            w.clusters.start(cluster_id=cluster_id)
            return (
                f"Cluster '{cluster_id}' start initiated. "
                "It may take several minutes to become running."
            )
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_terminate_cluster(cluster_id: str) -> str:
        """Terminate a running compute cluster.

        Stops all Spark workloads and releases the cloud resources. The
        cluster configuration is preserved and it can be restarted later.
        This call returns immediately without blocking.

        Args:
            cluster_id: The unique identifier of the cluster to terminate.

        Returns:
            Confirmation that termination was initiated.
        """
        try:
            w = get_workspace_client()
            w.clusters.delete(cluster_id=cluster_id)
            return f"Cluster '{cluster_id}' termination initiated."
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_restart_cluster(cluster_id: str) -> str:
        """Restart a running compute cluster.

        Restarts the Spark driver and all workers. Use this to clear
        cached state or apply configuration changes. This call returns
        immediately without blocking.

        Args:
            cluster_id: The unique identifier of the cluster to restart.

        Returns:
            Confirmation that the restart was initiated.
        """
        try:
            w = get_workspace_client()
            w.clusters.restart(cluster_id=cluster_id)
            return (
                f"Cluster '{cluster_id}' restart initiated. "
                "It may take a few minutes to become running again."
            )
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_resize_cluster(cluster_id: str, num_workers: int) -> str:
        """Resize a running cluster to a different number of workers.

        Changes the number of worker nodes in a running cluster. The
        resize happens gracefully — existing tasks are not interrupted.

        Args:
            cluster_id: The unique identifier of the cluster to resize.
            num_workers: The new number of worker nodes. Use 0 for a
                         single-node cluster (driver only).

        Returns:
            Confirmation that the resize was initiated.
        """
        try:
            w = get_workspace_client()
            w.clusters.resize(cluster_id=cluster_id, num_workers=num_workers)
            return f"Cluster '{cluster_id}' resize to {num_workers} workers initiated."
        except Exception as e:
            return format_error(e)

    # ── Instance Pools ──────────────────────────────────────────────────

    @mcp.tool()
    def databricks_list_instance_pools() -> str:
        """List all instance pools in the workspace.

        Instance pools reduce cluster start and auto-scaling times by
        maintaining a set of idle, ready-to-use cloud instances.

        Returns:
            JSON array of instance pool objects with instance_pool_id,
            instance_pool_name, node_type_id, min_idle_instances, and
            state. Results are capped at 100 items.
        """
        try:
            w = get_workspace_client()
            results = paginate(w.instance_pools.list())
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_instance_pool(instance_pool_id: str) -> str:
        """Get detailed information about a specific instance pool.

        Args:
            instance_pool_id: The unique identifier of the instance pool.

        Returns:
            JSON object with pool details including instance_pool_name,
            node_type_id, min_idle_instances, max_capacity, idle_instance_autotermination_minutes,
            and stats about pending/used/idle instances.
        """
        try:
            w = get_workspace_client()
            result = w.instance_pools.get(instance_pool_id)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    # ── Cluster Policies ────────────────────────────────────────────────

    @mcp.tool()
    def databricks_list_cluster_policies() -> str:
        """List all cluster policies in the workspace.

        Cluster policies constrain the attributes available during cluster
        creation, enforcing organizational standards and cost controls.

        Returns:
            JSON array of policy objects with policy_id, name, description,
            definition, and creator information. Results are capped at 100 items.
        """
        try:
            w = get_workspace_client()
            results = paginate(w.cluster_policies.list())
            return to_json(results)
        except Exception as e:
            return format_error(e)
