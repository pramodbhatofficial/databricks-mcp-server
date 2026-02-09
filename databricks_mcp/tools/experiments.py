"""MLflow experiment and run management tools for Databricks MCP.

Provides tools for managing MLflow experiments and runs tracked within the
Databricks workspace. Experiments group related ML training runs, and each
run tracks parameters, metrics, artifacts, and model outputs.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import get_workspace_client
from databricks_mcp.utils import format_error, paginate, to_json


def register_tools(mcp: FastMCP) -> None:
    """Register all MLflow experiment tools with the MCP server."""

    # -- Experiments -----------------------------------------------------------

    @mcp.tool()
    def databricks_list_experiments(max_results: int = 25) -> str:
        """List MLflow experiments in the workspace.

        Returns experiments ordered by creation time (newest first). Each
        experiment contains one or more runs that track ML training iterations.

        Args:
            max_results: Maximum number of experiments to return (default 25).

        Returns:
            JSON array of experiment objects, each containing experiment_id,
            name, artifact_location, lifecycle_stage, last_update_time,
            and creation_time.
        """
        try:
            w = get_workspace_client()
            results = paginate(
                w.experiments.list_experiments(max_results=max_results),
                max_items=max_results,
            )
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_experiment(experiment_id: str) -> str:
        """Get detailed information about a specific MLflow experiment.

        Args:
            experiment_id: The numeric ID of the experiment to retrieve
                           (e.g. "123456789").

        Returns:
            JSON object with full experiment details including experiment_id,
            name, artifact_location, lifecycle_stage, tags, and timestamps.
        """
        try:
            w = get_workspace_client()
            result = w.experiments.get_experiment(experiment_id=experiment_id)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_create_experiment(name: str, artifact_location: str = "") -> str:
        """Create a new MLflow experiment.

        Creates an experiment as a container for ML training runs. The name
        must be unique and typically follows a path-like convention
        (e.g. "/Users/user@example.com/my-experiment").

        Args:
            name: Unique name for the experiment. Workspace experiments
                  typically use paths like "/Users/email/experiment-name"
                  or "/Shared/team/experiment-name".
            artifact_location: Optional root artifact URI for the experiment.
                               If not provided, the default workspace artifact
                               location is used. Example:
                               "dbfs:/databricks/mlflow-tracking/12345".

        Returns:
            JSON object with the created experiment's experiment_id.
        """
        try:
            w = get_workspace_client()
            kwargs: dict = {"name": name}
            if artifact_location:
                kwargs["artifact_location"] = artifact_location
            result = w.experiments.create_experiment(**kwargs)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_delete_experiment(experiment_id: str) -> str:
        """Mark an MLflow experiment as deleted (soft delete).

        The experiment is moved to a trash state and can be restored within
        the retention period. All runs within the experiment are also marked
        as deleted. The caller must be the experiment owner or a workspace
        admin.

        Args:
            experiment_id: The numeric ID of the experiment to delete.

        Returns:
            Confirmation message on success.
        """
        try:
            w = get_workspace_client()
            w.experiments.delete_experiment(experiment_id=experiment_id)
            return f"Experiment '{experiment_id}' deleted successfully."
        except Exception as e:
            return format_error(e)

    # -- Runs ------------------------------------------------------------------

    @mcp.tool()
    def databricks_search_runs(
        experiment_ids: str,
        filter_string: str = "",
        max_results: int = 25,
        order_by: str = "",
    ) -> str:
        """Search for MLflow runs across one or more experiments.

        Supports filtering by metrics, parameters, tags, and run attributes
        using the MLflow search syntax.

        Args:
            experiment_ids: Comma-separated list of experiment IDs to search
                            within (e.g. "123,456,789").
            filter_string: Optional MLflow filter expression. Examples:
                           - 'metrics.rmse < 0.5'
                           - 'params.learning_rate = "0.01"'
                           - 'tags.mlflow.source.type = "NOTEBOOK"'
                           - 'attributes.status = "FINISHED"'
                           Leave empty to return all runs.
            max_results: Maximum number of runs to return (default 25).
            order_by: Optional comma-separated list of columns to order by.
                      Prefix with "+" for ascending or "-" for descending.
                      Examples: "metrics.rmse ASC", "start_time DESC".

        Returns:
            JSON array of run objects, each containing run_id, experiment_id,
            status, start_time, end_time, metrics, params, and tags.
        """
        try:
            w = get_workspace_client()
            ids = [eid.strip() for eid in experiment_ids.split(",") if eid.strip()]
            kwargs: dict = {
                "experiment_ids": ids,
                "max_results": max_results,
            }
            if filter_string:
                kwargs["filter_string"] = filter_string
            if order_by:
                kwargs["order_by"] = [col.strip() for col in order_by.split(",") if col.strip()]
            results = paginate(w.experiments.search_runs(**kwargs), max_items=max_results)
            return to_json(results)
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_get_run(run_id: str) -> str:
        """Get detailed information about a specific MLflow run.

        Args:
            run_id: The UUID of the run to retrieve (e.g.
                    "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").

        Returns:
            JSON object with full run details including run_id, experiment_id,
            status, start_time, end_time, metrics, params, tags, and
            artifact_uri.
        """
        try:
            w = get_workspace_client()
            result = w.experiments.get_run(run_id=run_id)
            return to_json(result)
        except Exception as e:
            return format_error(e)

    # -- Run Logging -----------------------------------------------------------

    @mcp.tool()
    def databricks_log_metric(run_id: str, key: str, value: float, step: int = 0) -> str:
        """Log a metric value for an MLflow run.

        Metrics are numeric measurements that track model performance over time.
        Multiple values can be logged for the same key at different steps to
        record training curves and convergence behavior.

        Args:
            run_id: The UUID of the run to log the metric to.
            key: Name of the metric (e.g. "rmse", "accuracy", "loss").
            value: Numeric value of the metric at this step.
            step: Optional integer step number for tracking metrics over time.
                  Defaults to 0. Use sequential step values to record training
                  progress (e.g. epoch number or batch count).

        Returns:
            Confirmation message on success.
        """
        try:
            w = get_workspace_client()
            w.experiments.log_metric(run_id=run_id, key=key, value=value, step=step)
            return f"Metric '{key}={value}' logged at step {step} for run '{run_id}'."
        except Exception as e:
            return format_error(e)

    @mcp.tool()
    def databricks_log_param(run_id: str, key: str, value: str) -> str:
        """Log a parameter for an MLflow run.

        Parameters are key-value pairs that capture the configuration used
        for a training run (e.g. hyperparameters, data paths, model type).
        Each key can only be logged once per run.

        Args:
            run_id: The UUID of the run to log the parameter to.
            key: Name of the parameter (e.g. "learning_rate", "batch_size",
                 "model_type").
            value: String value of the parameter. Numeric values should be
                   passed as strings (e.g. "0.001", "32").

        Returns:
            Confirmation message on success.
        """
        try:
            w = get_workspace_client()
            w.experiments.log_param(run_id=run_id, key=key, value=value)
            return f"Parameter '{key}={value}' logged for run '{run_id}'."
        except Exception as e:
            return format_error(e)
