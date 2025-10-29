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
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .zoho_client import (
    health_info,
    get_workspaces_list,
    search_views,
    get_view_details,
    export_view,
    query_data,
)

# ---------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------

app = FastAPI(title="Zoho Analytics MCP (v2) — Tools oficiales")

# Allow CORS from all origins (useful for local and MCP clients)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Optional alias for MCP clients that expect `/sse`
# ---------------------------------------------------------------------

@app.get("/sse")
def sse_redirect():
    """Redirects `/sse` → `/openapi.json` for MCP clients."""
    return RedirectResponse(url="/openapi.json")


# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    """Simple health endpoint returning runtime metadata."""
    return health_info()


# ---------------------------------------------------------------------
# get_workspaces_list
# ---------------------------------------------------------------------

@app.get("/workspaces_v2")
def workspaces_v2() -> dict:
    """List all workspaces available to the authenticated user."""
    return get_workspaces_list()


# ---------------------------------------------------------------------
# search_views
# ---------------------------------------------------------------------

@app.get("/views_v2")
def views_v2(
    workspace_id: str = Query(..., description="Workspace ID"),
    q: str | None = Query(None, description="Texto a buscar"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict:
    """Search or list views within a workspace."""
    return search_views(workspace_id, q, limit, offset)


# ---------------------------------------------------------------------
# get_view_details
# ---------------------------------------------------------------------

@app.get("/view_details_v2")
def view_details_v2(
    workspace_id: str = Query(
        ..., description="Workspace ID (kept for compatibility; ignored)."
    ),
    view_id: str = Query(..., description="View ID o nombre exacto"),
) -> dict:
    """Retrieve metadata for a specific view."""
    return get_view_details(workspace_id, view_id)


# ---------------------------------------------------------------------
# export_view
# ---------------------------------------------------------------------

class ExportViewBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    view: str = Field(..., description="ID o nombre de la vista/tabla")
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)


@app.post("/export_view_v2")
def export_view_v2(payload: ExportViewBody = Body(...)) -> dict:
    """Export data from a specific view."""
    return export_view(payload.workspace_id, payload.view, payload.limit, payload.offset)


# ---------------------------------------------------------------------
# query_data
# ---------------------------------------------------------------------

class QueryBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    sql: str = Field(..., description="Consulta SQL")


@app.post("/query_v2")
def query_v2(payload: QueryBody = Body(...)) -> dict:
    """Execute a SQL query against a workspace."""
    return query_data(payload.workspace_id, payload.sql)
