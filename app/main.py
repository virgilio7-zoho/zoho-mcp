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

from fastapi import FastAPI, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

import json
import asyncio

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


# ============================================================
# ===============  MCP MINIMAL IMPLEMENTATION  ===============
# ============================================================

# A list of tool definitions that will be sent via SSE to the MCP client.
# Each tool entry declares the action name, a short description and a JSON
# schema describing its accepted input. The MCP client uses this schema to
# validate and construct requests.
ACTIONS: list[dict] = [
    {
        "name": "workspaces_v2",
        "description": "List the workspaces available to the authenticated user.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "views_v2",
        "description": "Search or list views within a workspace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_id": {"type": "string"},
                "q": {"type": ["string", "null"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
                "offset": {"type": "integer", "minimum": 0},
            },
            "required": ["workspace_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "view_details_v2",
        "description": "Retrieve metadata for a specific view.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_id": {"type": "string"},
                "view_id": {"type": "string"},
            },
            "required": ["workspace_id", "view_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "export_view_v2",
        "description": "Export data from a specific view.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_id": {"type": "string"},
                "view": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 10000},
                "offset": {"type": "integer", "minimum": 0},
            },
            "required": ["workspace_id", "view"],
            "additionalProperties": False,
        },
    },
    {
        "name": "query_v2",
        "description": "Execute a SQL query against a workspace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_id": {"type": "string"},
                "sql": {"type": "string"},
            },
            "required": ["workspace_id", "sql"],
            "additionalProperties": False,
        },
    },
]


def _sse_frame(event: str, data_obj: dict) -> bytes:
    """Encode an SSE frame with the given event name and JSON-serialisable data.

    Parameters
    ----------
    event: str
        The event type (e.g. ``actions``).
    data_obj: dict
        The data payload to send. It will be JSON-encoded.

    Returns
    -------
    bytes
        A bytes object representing the SSE frame.
    """
    payload = json.dumps(data_obj, ensure_ascii=False)
    return (f"event: {event}\n" + f"data: {payload}\n\n").encode("utf-8")


@app.get("/sse")
async def sse_actions(request: Request) -> StreamingResponse:
    """Serve the MCP actions via Server‑Sent Events.

    When a MCP client connects to this endpoint with ``Accept: text/event-stream``,
    it will receive a single ``actions`` event containing all tool definitions.
    Afterwards, periodic ``keep-alive`` comments are sent to keep the HTTP
    connection open. The connection terminates when the client disconnects or
    after a short idle period.

    Parameters
    ----------
    request: Request
        The incoming HTTP request, used to detect client disconnects.

    Returns
    -------
    StreamingResponse
        A streaming response that yields SSE frames.
    """

    async def event_generator():
        # Send the list of actions once.
        yield _sse_frame("actions", {"actions": ACTIONS})
        # Keep the connection alive with periodic comments.
        # Some MCP clients require the stream to stay open for further events.
        for _ in range(5):
            if await request.is_disconnected():
                break
            await asyncio.sleep(1)
            # ``:`` denotes a comment line in SSE; this acts as a ping.
            yield b": keep-alive\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


class McpInvoke(BaseModel):
    """Model for an MCP invocation request.

    The MCP client sends a JSON object containing the name of the action to
    execute and an ``input`` object with the parameters for that action. This
    model validates the structure before the request is routed to the
    appropriate helper function.
    """

    action: str = Field(..., description="Name of the MCP action to invoke")
    input: dict = Field(default_factory=dict, description="Parameters for the action")


@app.post("/mcp")
def mcp_invoke(payload: McpInvoke):
    """Invoke one of the defined MCP actions.

    This endpoint receives a JSON payload with an ``action`` field indicating
    which tool should run and an ``input`` field containing the arguments.
    Supported actions are: ``workspaces_v2``, ``views_v2``, ``view_details_v2``,
    ``export_view_v2`` and ``query_v2``. The helper functions defined at the
    top of this module are used to perform the actual API calls.

    Returns
    -------
    dict
        A JSON object containing either the result of the action or an error
        description if the action name is not recognised.
    """
    name = payload.action
    params = payload.input or {}

    try:
        if name == "workspaces_v2":
            result = get_workspaces_list()
        elif name == "views_v2":
            workspace_id = params.get("workspace_id")
            if not workspace_id:
                raise ValueError("'workspace_id' es requerido para views_v2")
            result = search_views(
                workspace_id,
                params.get("q"),
                int(params.get("limit", 200)),
                int(params.get("offset", 0)),
            )
        elif name == "view_details_v2":
            workspace_id = params.get("workspace_id")
            view_id = params.get("view_id")
            if not (workspace_id and view_id):
                raise ValueError("'workspace_id' y 'view_id' son requeridos para view_details_v2")
            result = get_view_details(workspace_id, view_id)
        elif name == "export_view_v2":
            workspace_id = params.get("workspace_id")
            view = params.get("view")
            if not (workspace_id and view):
                raise ValueError("'workspace_id' y 'view' son requeridos para export_view_v2")
            limit = int(params.get("limit", 100))
            offset = int(params.get("offset", 0))
            result = export_view(workspace_id, view, limit, offset)
        elif name == "query_v2":
            workspace_id = params.get("workspace_id")
            sql = params.get("sql")
            if not (workspace_id and sql):
                raise ValueError("'workspace_id' y 'sql' son requeridos para query_v2")
            result = query_data(workspace_id, sql)
        else:
            return JSONResponse(status_code=404, content={"ok": False, "error": f"Acción desconocida: {name}"})

        return {"ok": True, "action": name, "result": result}
    except Exception as e:
        # For debugging purposes, return the error message. In production, you
        # may wish to log the exception and return a generic error message.
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})
