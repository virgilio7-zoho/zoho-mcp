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
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

import json
import asyncio
from datetime import datetime

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

# ---------------------------------------------------------------------
# Well-known endpoints for OpenID configuration and OAuth protected
# resource discovery. ChatGPT's MCP client attempts to probe these
# endpoints even when no authentication is configured. To prevent 404
# errors in logs and potential failure, we provide minimal JSON
# responses. In a production system these would expose actual
# configuration details or return 404 as appropriate. Here we return
# empty objects.

@app.get("/.well-known/openid-configuration", include_in_schema=False)
def well_known_openid_config() -> dict:
    """Minimal OpenID configuration endpoint.

    MCP clients may request this endpoint when negotiating OAuth
    details. We return an empty configuration to signal that no
    OpenID configuration is provided.
    """
    return {}


@app.get("/.well-known/openid-configuration/{subpath:path}", include_in_schema=False)
def well_known_openid_config_sub(subpath: str) -> dict:
    """Catch-all for OpenID configuration subpaths."""
    return {}


@app.get("/.well-known/oauth-authorization-server", include_in_schema=False)
def well_known_oauth_authorization_server() -> dict:
    """Minimal OAuth authorization server discovery endpoint."""
    return {}


@app.get("/.well-known/oauth-authorization-server/{subpath:path}", include_in_schema=False)
def well_known_oauth_authorization_server_sub(subpath: str) -> dict:
    """Catch-all for OAuth authorization server subpaths."""
    return {}


@app.get("/.well-known/oauth-protected-resource", include_in_schema=False)
def well_known_oauth_protected_resource() -> dict:
    """Minimal OAuth protected resource discovery endpoint."""
    return {}


@app.get("/.well-known/oauth-protected-resource/{subpath:path}", include_in_schema=False)
def well_known_oauth_protected_resource_sub(subpath: str) -> dict:
    """Catch-all for OAuth protected resource subpaths."""
    return {}


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
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "views_v2",
        "description": "Search or list views within a workspace.",
        "parameters": {
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
        "parameters": {
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
        "parameters": {
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
        "parameters": {
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


#
# JSON-RPC structures for MCP discovery and invocation
#

# Define the tool metadata for JSON-RPC tools/list responses. Each tool object
# includes a name, human‑readable title, description and JSON Schema for the
# input parameters. This mirrors the structure outlined in the MCP
# documentation for tool discovery【136852395087279†L414-L431】.
TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "workspaces_v2",
        "title": "List Workspaces",
        "description": "List all workspaces available to the authenticated user.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "views_v2",
        "title": "Search Views",
        "description": "Search or list views within a workspace.",
        "inputSchema": {
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
        "title": "View Details",
        "description": "Retrieve metadata for a specific view.",
        "inputSchema": {
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
        "title": "Export View",
        "description": "Export data from a specific view.",
        "inputSchema": {
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
        "title": "Execute SQL",
        "description": "Execute a SQL query against a workspace.",
        "inputSchema": {
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


# Handle both the simple MCP invocation format (``{action, input}``) and
# JSON‑RPC requests as used by the MCP specification. Clients like ChatGPT
# send JSON‑RPC requests to discover and invoke tools. This handler
# inspects the incoming payload to determine which protocol is being used
# and returns the appropriate response format.
@app.post("/mcp")
async def mcp_invoke(
    payload: Optional[dict] = Body(
        default=None,
        description=(
            "JSON‑RPC request payload. For MCP clients this should contain a "
            "`jsonrpc` version, a `method` such as `initialize`, `tools/list` "
            "or `tools/call`, an `id`, and optionally `params`."
        ),
    ),
    request: Request = None,
):
    """
    Invoke MCP methods or execute simple actions.

    This endpoint accepts both JSON‑RPC 2.0 messages (used by ChatGPT and other
    MCP clients) and a simplified `{action, input}` format for backward
    compatibility. When invoked via Swagger/OpenAPI, provide a JSON‑RPC object
    in the request body to test tool discovery and execution. For example:

    ```
    {
      "jsonrpc": "2.0",
      "id": 1,
      "method": "initialize",
      "params": {}
    }
    ```
    """
    # Determine the incoming data. If `payload` is provided by FastAPI's body
    # parser, use it; otherwise fall back to reading the raw request body.
    if payload is not None:
        data = payload
    else:
        try:
            body_bytes = await request.body()
            if not body_bytes:
                data = {}
            else:
                data = json.loads(body_bytes.decode())
        except Exception:
            return JSONResponse(status_code=400, content={"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}})

    # --- JSON‑RPC 2.0 handling ---
    if isinstance(data, dict) and data.get("jsonrpc") == "2.0":
        jsonrpc_id = data.get("id")
        method = data.get("method")
        params = data.get("params", {}) or {}

        # Tool discovery
        if method == "tools/list":
            result = {"tools": TOOL_DEFINITIONS}
            return {"jsonrpc": "2.0", "id": jsonrpc_id, "result": result}

        # Initialization handshake
        # Clients send an ``initialize`` request with a desired protocolVersion
        # and (optionally) client capabilities. According to the MCP spec,
        # servers should return the negotiated protocolVersion, declare their
        # supported capabilities, and may include serverInfo. We reflect
        # back the requested protocolVersion if provided, defaulting to
        # today's date if absent.
        if method == "initialize":
            requested_proto = None
            # ``params`` may contain a ``protocolVersion`` requested by the client
            # and a ``capabilities`` object. We ignore client capabilities for
            # now and support only tools.
            try:
                requested_proto = params.get("protocolVersion")
            except Exception:
                requested_proto = None
            # Use requested protocol version or current date as fallback
            if requested_proto:
                protocol_version = requested_proto
            else:
                protocol_version = datetime.utcnow().strftime("%Y-%m-%d")
            capabilities = {
                "tools": {"listChanged": False},
            }
            server_info = {
                "name": "Zoho Analytics MCP",
                "version": "0.1.0",
            }
            return {
                "jsonrpc": "2.0",
                "id": jsonrpc_id,
                "result": {
                    "protocolVersion": protocol_version,
                    "capabilities": capabilities,
                    "serverInfo": server_info,
                },
            }
        # Tool execution (within JSON‑RPC handler)
        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments", {}) or {}
            try:
                # Dispatch to the corresponding helper based on the tool name
                if name == "workspaces_v2":
                    result_data = get_workspaces_list()
                elif name == "views_v2":
                    workspace_id = arguments.get("workspace_id")
                    if not workspace_id:
                        raise ValueError("Missing required parameter: workspace_id")
                    result_data = search_views(
                        workspace_id,
                        arguments.get("q"),
                        int(arguments.get("limit", 200)),
                        int(arguments.get("offset", 0)),
                    )
                elif name == "view_details_v2":
                    workspace_id = arguments.get("workspace_id")
                    view_id = arguments.get("view_id")
                    if not (workspace_id and view_id):
                        raise ValueError("Missing required parameters: workspace_id and view_id")
                    result_data = get_view_details(workspace_id, view_id)
                elif name == "export_view_v2":
                    workspace_id = arguments.get("workspace_id")
                    view = arguments.get("view")
                    if not (workspace_id and view):
                        raise ValueError("Missing required parameters: workspace_id and view")
                    limit = int(arguments.get("limit", 100))
                    offset = int(arguments.get("offset", 0))
                    result_data = export_view(workspace_id, view, limit, offset)
                elif name == "query_v2":
                    workspace_id = arguments.get("workspace_id")
                    sql = arguments.get("sql")
                    if not (workspace_id and sql):
                        raise ValueError("Missing required parameters: workspace_id and sql")
                    result_data = query_data(workspace_id, sql)
                else:
                    return JSONResponse(status_code=404, content={"jsonrpc": "2.0", "id": jsonrpc_id, "error": {"code": -32601, "message": f"Unknown tool: {name}"}})

                # Return the result as content array per MCP spec
                return {
                    "jsonrpc": "2.0",
                    "id": jsonrpc_id,
                    "result": {
                        "content": [
                            {
                                "type": "json",
                                "value": result_data,
                            }
                        ]
                    },
                }
            except Exception as exc:
                return JSONResponse(status_code=400, content={
                    "jsonrpc": "2.0",
                    "id": jsonrpc_id,
                    "error": {"code": -32000, "message": str(exc)}
                })

        # Unknown method for JSON‑RPC 2.0
        return JSONResponse(status_code=404, content={
            "jsonrpc": "2.0",
            "id": jsonrpc_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        })

    # --- Simple (legacy) invocation format ---
    # Accept the previous `{action: name, input: {…}}` schema for backward compatibility.
    if isinstance(data, dict) and "action" in data:
        name = data.get("action")
        arguments = data.get("input", {}) or {}
        try:
            if name == "workspaces_v2":
                result_data = get_workspaces_list()
            elif name == "views_v2":
                workspace_id = arguments.get("workspace_id")
                if not workspace_id:
                    raise ValueError("Missing required parameter: workspace_id")
                result_data = search_views(
                    workspace_id,
                    arguments.get("q"),
                    int(arguments.get("limit", 200)),
                    int(arguments.get("offset", 0)),
                )
            elif name == "view_details_v2":
                workspace_id = arguments.get("workspace_id")
                view_id = arguments.get("view_id")
                if not (workspace_id and view_id):
                    raise ValueError("Missing required parameters: workspace_id and view_id")
                result_data = get_view_details(workspace_id, view_id)
            elif name == "export_view_v2":
                workspace_id = arguments.get("workspace_id")
                view = arguments.get("view")
                if not (workspace_id and view):
                    raise ValueError("Missing required parameters: workspace_id and view")
                limit = int(arguments.get("limit", 100))
                offset = int(arguments.get("offset", 0))
                result_data = export_view(workspace_id, view, limit, offset)
            elif name == "query_v2":
                workspace_id = arguments.get("workspace_id")
                sql = arguments.get("sql")
                if not (workspace_id and sql):
                    raise ValueError("Missing required parameters: workspace_id and sql")
                result_data = query_data(workspace_id, sql)
            else:
                return JSONResponse(status_code=404, content={"ok": False, "error": f"Unknown action: {name}"})

            return {"ok": True, "action": name, "result": result_data}
        except Exception as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})

    # If the payload does not match any known format, return an error.
    return JSONResponse(status_code=400, content={"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}})

# Support trailing slash in the MCP endpoint. Some MCP clients may
# invoke ``/mcp/`` instead of ``/mcp``. FastAPI treats these as
# distinct paths for POST requests, so we register an alias that
# delegates to the main ``mcp_invoke`` handler.
@app.post("/mcp/")
async def mcp_invoke_alias(
    payload: Optional[dict] = Body(
        default=None,
        description=(
            "JSON‑RPC request payload. See ``/mcp`` for details."
        ),
    ),
    request: Request = None,
):
    # Delegate to the main mcp_invoke handler. We explicitly pass the
    # payload and request so the logic remains the same.
    return await mcp_invoke(payload=payload, request=request)
