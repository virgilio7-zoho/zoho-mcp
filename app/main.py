"""
Main entry point for the Zoho Analytics MCP server.

This module defines a FastAPI application exposing a small set of HTTP
endpoints that wrap the Zoho Analytics REST API v2. It serves as the
server component for the Model Context Protocol (MCP) integration with
Zoho Analytics. The endpoints provide health checks and a handful of
operations such as listing workspaces, searching for views, retrieving
view metadata, exporting view data, and executing SQL queries.

The underlying API logic is delegated to functions in ``zoho_client.py``.
Those functions handle OAuth token refresh, HTTP requests to the
Analytics API, and some simple parameter validation. See the docstrings
in ``zoho_client.py`` for more details.

Note
----
You **must** configure the required environment variables before
starting this server; otherwise the client will raise runtime errors.
See ``config.py`` for the list of variables and their descriptions.
"""

from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Import the client helpers from the sibling module. Relative import avoids
# requiring ``app`` to be installed as a top-level package.
from .zoho_client import (
    health_info,
    get_workspaces_list,
    search_views,
    get_view_details,
    export_view,
    query_data,
)


app = FastAPI(title="Zoho Analytics MCP (v2) — Tools oficiales")

# Allow CORS from all origins. In production you may wish to restrict this.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Health ----------
@app.get("/health")
def health() -> dict:
    """Simple health endpoint returning runtime metadata.

    Returns a small JSON object containing the server status,
    organisation ID, configured server URL, data directory and
    whether an access token is currently cached.
    """
    return health_info()


# ---------- get_workspaces_list ----------
@app.get("/workspaces_v2")
def workspaces_v2() -> dict:
    """List all workspaces available to the authenticated user.

    Returns
    -------
    dict
        A JSON object representing the list of workspaces.
    """
    return get_workspaces_list()


# ---------- search_views ----------
@app.get("/views_v2")
def views_v2(
    workspace_id: str = Query(..., description="Workspace ID"),
    q: str | None = Query(None, description="Texto a buscar"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search or list views within a workspace.

    Parameters
    ----------
    workspace_id: str
        Identifier of the workspace whose views should be retrieved.
    q: str | None
        Optional search keyword. If provided, the API will attempt to match
        views by this keyword. If omitted, all views are returned (up to
        the specified limit).
    limit: int
        Maximum number of results to return (bounded by server-side
        restrictions). Defaults to 200.
    offset: int
        Index of the first result to return (for pagination). Defaults to 0.

    Returns
    -------
    dict
        A JSON object with the matching views.
    """
    return search_views(workspace_id, q, limit, offset)


# ---------- get_view_details ----------
@app.get("/view_details_v2")
def view_details_v2(
    workspace_id: str = Query(
        ...,
        description=(
            "Workspace ID (retained for compatibility; ignored by the underlying API)."
        ),
    ),
    view_id: str = Query(..., description="View ID o nombre exacto"),
) -> dict:
    """Retrieve details for a specific view.

    While the Zoho Analytics API endpoint used to fetch view details
    (``/restapi/v2/views/<view-id>``) does not need the workspace ID, this
    parameter is still accepted by this endpoint for backward compatibility
    with older clients. The workspace ID is ignored in the underlying API
    call but may be validated for emptiness. This design mirrors the
    behaviour described in the official documentation【357598884937503†L126-L134】.

    Parameters
    ----------
    workspace_id: str
        Identifier of the workspace (not used by this API call).
    view_id: str
        Identifier or exact name of the view.

    Returns
    -------
    dict
        JSON response containing metadata of the specified view.
    """
    return get_view_details(workspace_id, view_id)


# ---------- export_view ----------
class ExportViewBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    view: str = Field(..., description="ID o nombre de la vista/tabla")
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)


@app.post("/export_view_v2")
def export_view_v2(payload: ExportViewBody = Body(...)) -> dict:
    """Export data from a specific view.

    This endpoint accepts a workspace ID, a view identifier and pagination
    parameters (``limit`` and ``offset``). It delegates to the
    ``export_view`` helper in ``zoho_client.py``, which uses the Zoho
    Analytics Bulk API to asynchronously export the view's data in JSON
    format【215211381353514†L1082-L1101】. The helper transparently falls back
    to the synchronous export API when the bulk API is unavailable and
    performs client‑side slicing according to the requested limit and
    offset. See the helper's docstring for full details.
    """
    return export_view(payload.workspace_id, payload.view, payload.limit, payload.offset)


# ---------- query_data ----------
class QueryBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    sql: str = Field(..., description="Consulta SQL")


@app.post("/query_v2")
def query_v2(payload: QueryBody = Body(...)) -> dict:
    """Execute a SQL query against a workspace.

    For complex analytical queries the Zoho Analytics API provides a SQL
    endpoint which accepts arbitrary SQL queries (subject to security
    restrictions). This endpoint simply forwards the provided SQL to the
    underlying API and returns the resulting data set.
    """
    return query_data(payload.workspace_id, payload.sql)
