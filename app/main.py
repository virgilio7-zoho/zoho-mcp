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

import os, secrets, time, json, asyncio
from fastapi import FastAPI, Query, Body, Request, Header, Depends, HTTPException, Form
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
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
# ========= AUTH LIGERA: API KEY o BEARER emitido por este servidor =========
API_KEY = os.getenv("API_KEY", "")
# Almacén de tokens emitidos por nuestro "AS" mínimo (en memoria)
# En producción podrías cambiarlos por Redis/DB. Aquí basta en memoria.
_OAUTH_TOKENS: dict[str, float] = {}   # access_token -> exp_ts
# authorization_code -> (exp_ts, client_id, redirect_uri)
_OAUTH_CODES: dict[str, tuple[float, str, str]] = {}
# refresh_token -> exp_ts
_OAUTH_REFRESH: dict[str, float] = {}
# === TTL configurables (por env) ===
ACCESS_TTL_SECONDS = int(os.getenv("ACCESS_TTL_SECONDS", "3600"))   # access token: 1h
REFRESH_TTL_DAYS   = int(os.getenv("REFRESH_TTL_DAYS",   "3650"))   # refresh: ~10 años

# (Opcional, para sobrevivir reinicios) Seed de refresh tokens conocidos
PRESEEDED_REFRESH_TOKENS = [
    t.strip() for t in (os.getenv("PRESEEDED_REFRESH_TOKENS", "")).split(",") if t.strip()
]
for _rt in PRESEEDED_REFRESH_TOKENS:
    _OAUTH_REFRESH[_rt] = time.time() + (REFRESH_TTL_DAYS * 24 * 3600)

def _bearer_valid(auth_header: str | None) -> bool:
    if not auth_header or not auth_header.lower().startswith("bearer "):
        return False
    token = auth_header.split(" ", 1)[1].strip()
    exp = _OAUTH_TOKENS.get(token)
    return bool(exp and exp > time.time())

def require_key_or_bearer(
    x_api_key: str | None = Header(None),
    authorization: str | None = Header(None),
):
    """
    Permite EITHER:
      - X-API-Key (nuestra clave fija); o
      - Authorization: Bearer <access_token> emitido por este mismo servidor.
    """
    if API_KEY and x_api_key == API_KEY:
        return
    if _bearer_valid(authorization):
        return
    raise HTTPException(status_code=401, detail="Auth required: X-API-Key or Bearer token")

app = FastAPI(title="Zoho Analytics MCP (v2) — Tools oficiales")

# Allow CORS from all origins. In production you may wish to restrict this.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================= OAUTH MÍNIMO (para el conector) =======================
def _issuer(req: Request) -> str:
    """
    Devuelve el issuer respetando cabeceras de proxy.
    IMPORTANTE: Siempre retorna HTTPS porque Render usa HTTPS en la URL pública.
    """
    # Priorizar headers de proxy (Render/Cloudflare)
    proto = (req.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    host  = (req.headers.get("x-forwarded-host")  or "").split(",")[0].strip()
    
    # Fallback a los valores directos de la request
    if not host:
        host = req.url.netloc
    if not proto or proto not in ("http", "https"):
        proto = "https"  # Default a HTTPS para Render
    
    return f"{proto}://{host}"

@app.get("/.well-known/oauth-protected-resource", include_in_schema=False)
def oauth_protected_resource(req: Request):
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_servers": [base],
    }

@app.get("/.well-known/oauth-authorization-server", include_in_schema=False)
def oauth_authorization_server(req: Request):
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/authorize",
        "token_endpoint": f"{base}/token",
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "scopes_supported": ["default", "offline_access"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "token_endpoint_auth_methods_supported": ["none"],   # <- clave para este flujo
        "authorization_response_iss_parameter_supported": True,
    }

@app.get("/.well-known/openid-configuration", include_in_schema=False)
def openid_configuration(req: Request):
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/authorize",
        "token_endpoint": f"{base}/token",
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "scopes_supported": ["default", "offline_access"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "token_endpoint_auth_methods_supported": ["none"],
        "authorization_response_iss_parameter_supported": True,
    }

# ========= CORRECCIÓN: Endpoint /authorize =========
@app.get("/authorize", include_in_schema=False)
def oauth_authorize(
    request: Request,
    response_type: str = Query(...),
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    scope: str | None = Query(None),
    state: str | None = Query(None),
    code_challenge: str | None = Query(None),
    code_challenge_method: str | None = Query(None),
):
    """Consent inmediato y redirección con `code`."""
    if response_type != "code":
        raise HTTPException(status_code=400, detail="unsupported_response_type")

    # Código temporal (10 min)
    code = secrets.token_urlsafe(24)
    _OAUTH_CODES[code] = (time.time() + 600, client_id, redirect_uri)

    # ✅ USAR _issuer() para consistencia
    base = _issuer(request)

    # Construir redirect con iss
    sep = "&" if "?" in redirect_uri else "?"
    final_url = f"{redirect_uri}{sep}code={code}"
    if state:
        final_url += f"&state={state}"
    final_url += f"&iss={base}"

    print(f"[OAUTH] Redirecting to: {final_url}")  # Debug
    return RedirectResponse(url=final_url, status_code=302)


def _body_value(body: dict, name: str, default=None):
    """Lee un valor del body admitiendo JSON o form y listas."""
    if name in body:
        v = body[name]
        if isinstance(v, list) and v:
            return v[0]
        return v
    return default

# ========= CORRECCIÓN: Endpoint /token - Manejo robusto =========
@app.post("/token", include_in_schema=False)
async def oauth_token(request: Request):
    """
    Endpoint de intercambio de tokens OAuth 2.0.
    Soporta grant_type: authorization_code y refresh_token.
    """
    ctype = (request.headers.get("content-type") or "").lower()
    data: dict = {}

    # Intentar parsear form-urlencoded o multipart
    if "application/x-www-form-urlencoded" in ctype or "multipart/form-data" in ctype:
        try:
            form = await request.form()
            data = dict(form)
        except Exception as e:
            print(f"[TOKEN] Error parsing form: {e}")

    # Fallback a JSON
    if not data:
        try:
            data = await request.json()
        except Exception as e:
            print(f"[TOKEN] Error parsing JSON: {e}")

    # También revisar query params (algunos clientes los usan)
    qp = dict(request.query_params)

    def pick(name: str, default=None):
        val = data.get(name) or qp.get(name)
        # Si es lista (de form), tomar primer elemento
        if isinstance(val, list) and val:
            return val[0]
        return val or default

    grant_type = pick("grant_type")
    code = pick("code")
    redirect_uri = pick("redirect_uri", "")
    client_id = pick("client_id", "")
    refresh_tok = pick("refresh_token")
    code_verifier = pick("code_verifier")

    # Log para debugging
    print({
        "event": "token_request",
        "content_type": ctype,
        "grant_type": grant_type,
        "has_code": bool(code),
        "has_refresh": bool(refresh_tok),
        "client_id": client_id
    })

    # Validar grant_type
    if grant_type not in ("authorization_code", "refresh_token"):
        return JSONResponse(
            status_code=400,
            content={"error": "unsupported_grant_type", "error_description": f"Grant type '{grant_type}' not supported"}
        )

    # ========= AUTHORIZATION_CODE FLOW =========
    if grant_type == "authorization_code":
        if not code:
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_request", "error_description": "Missing 'code' parameter"}
            )

        # Verificar el código
        code_data = _OAUTH_CODES.pop(code, None)
        if not code_data:
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_grant", "error_description": "Invalid or expired authorization code"}
            )

        exp_ts, expected_client, expected_redirect = code_data

        # Validar expiración
        if exp_ts < time.time():
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_grant", "error_description": "Authorization code expired"}
            )

        # Generar tokens
        access_token = secrets.token_urlsafe(32)
        refresh_token = secrets.token_urlsafe(32)

        _OAUTH_TOKENS[access_token] = time.time() + ACCESS_TTL_SECONDS
        _OAUTH_REFRESH[refresh_token] = time.time() + (REFRESH_TTL_DAYS * 24 * 3600)

        print(f"[TOKEN] Issued access_token (expires in {ACCESS_TTL_SECONDS}s)")

        return {
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": ACCESS_TTL_SECONDS,
            "refresh_token": refresh_token,
            "scope": "default offline_access",
        }

    # ========= REFRESH_TOKEN FLOW =========
    elif grant_type == "refresh_token":
        if not refresh_tok:
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_request", "error_description": "Missing 'refresh_token' parameter"}
            )

        exp = _OAUTH_REFRESH.get(refresh_tok)
        if not exp or exp < time.time():
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_grant", "error_description": "Invalid or expired refresh token"}
            )

        # Generar nuevo access token
        access_token = secrets.token_urlsafe(32)
        _OAUTH_TOKENS[access_token] = time.time() + ACCESS_TTL_SECONDS

        print(f"[TOKEN] Refreshed access_token")

        return {
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": ACCESS_TTL_SECONDS,
            "refresh_token": refresh_tok,  # Mantener el mismo refresh token
            "scope": "default offline_access",
        }

# ========= ADICIONAL: Endpoint de debug =========
@app.get("/debug/oauth-state", include_in_schema=False)
def debug_oauth_state():
    """Endpoint para debugging (REMOVER EN PRODUCCIÓN)"""
    return {
        "active_codes": len(_OAUTH_CODES),
        "active_tokens": len(_OAUTH_TOKENS),
        "active_refresh": len(_OAUTH_REFRESH),
        "issuer_example": "Use _issuer() with a Request object"
    }
# ========= CORRECCIÓN: Agregar POST / como endpoint MCP =========

# REEMPLAZAR el endpoint GET / existente con este:

@app.get("/", include_in_schema=False)
def root():
    """
    Raíz GET para verificación de disponibilidad.
    """
    return {
        "status": "ok",
        "message": "Zoho Analytics MCP server online",
        "endpoints": ["/health", "/authorize", "/token", "/mcp"],
        "mcp_endpoint": "/mcp",
        "protocol": "MCP",
    }


# AGREGAR ESTE NUEVO ENDPOINT POST /:
@app.post("/", include_in_schema=False)
async def root_mcp(
    payload: Optional[dict] = Body(default=None),
    request: Request = None,
):
    """
    POST / - Alias del endpoint /mcp para compatibilidad con clientes MCP.
    Algunos clientes MCP (como ChatGPT) envían las solicitudes JSON-RPC a la raíz.
    """
    # Delegar directamente a mcp_invoke
    return await mcp_invoke(payload=payload, request=request)



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
@app.get("/workspaces_v2", dependencies=[Depends(require_key_or_bearer)])
def workspaces_v2() -> dict:
    """List all workspaces available to the authenticated user.

    Returns
    -------
    dict
        A JSON object representing the list of workspaces.
    """
    return get_workspaces_list()


# ---------- search_views ----------
@app.get("/views_v2", dependencies=[Depends(require_key_or_bearer)])
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
@app.get("/view_details_v2", dependencies=[Depends(require_key_or_bearer)])
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


@app.post("/export_view_v2", dependencies=[Depends(require_key_or_bearer)])
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


@app.post("/query_v2", dependencies=[Depends(require_key_or_bearer)])
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
# El resto del código permanece igual, pero asegúrate de que
# el endpoint /mcp también tenga el decorator correcto:

@app.post("/mcp")
async def mcp_invoke(
    payload: Optional[dict] = Body(default=None),
    request: Request = None,
):
    """
    Invoke MCP methods or execute simple actions.
    
    Acepta JSON-RPC 2.0 para tools/list, initialize, tools/call, etc.
    """
    # Determinar los datos entrantes
    if payload is not None:
        data = payload
    else:
        try:
            body_bytes = await request.body()
            if not body_bytes:
                data = {}
            else:
                data = json.loads(body_bytes.decode())
        except Exception as e:
            print(f"[MCP] Parse error: {e}")
            return JSONResponse(
                status_code=400,
                content={
                    "jsonrpc": "2.0",
                    "error": {"code": -32700, "message": "Parse error"}
                }
            )

    # Log de debugging
    method = data.get("method", "unknown")
    print(f"[MCP] Request: method={method}, has_params={bool(data.get('params'))}")

    # --- JSON-RPC 2.0 handling ---
    if isinstance(data, dict) and data.get("jsonrpc") == "2.0":
        jsonrpc_id = data.get("id")
        method = data.get("method")
        params = data.get("params", {}) or {}

        # === INITIALIZE ===
        if method == "initialize":
            requested_proto = None
            try:
                requested_proto = params.get("protocolVersion")
            except Exception:
                pass
            
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
            
            result = {
                "protocolVersion": protocol_version,
                "capabilities": capabilities,
                "serverInfo": server_info,
            }
            
            print(f"[MCP] Initialize response: {result}")
            
            return {
                "jsonrpc": "2.0",
                "id": jsonrpc_id,
                "result": result,
            }

        # === TOOLS/LIST ===
        if method == "tools/list":
            result = {"tools": TOOL_DEFINITIONS}
            print(f"[MCP] Returning {len(TOOL_DEFINITIONS)} tools")
            return {
                "jsonrpc": "2.0",
                "id": jsonrpc_id,
                "result": result
            }

        # === TOOLS/CALL ===
        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments", {}) or {}
            
            print(f"[MCP] Tool call: {name} with args: {list(arguments.keys())}")
            
            try:
                # Dispatch según el tool
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
                    print(f"[MCP] Unknown tool: {name}")
                    return JSONResponse(
                        status_code=404,
                        content={
                            "jsonrpc": "2.0",
                            "id": jsonrpc_id,
                            "error": {
                                "code": -32601,
                                "message": f"Unknown tool: {name}"
                            }
                        }
                    )

                # Retornar según spec MCP
                print(f"[MCP] Tool {name} executed successfully")
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
                print(f"[MCP] Tool execution error: {exc}")
                return JSONResponse(
                    status_code=400,
                    content={
                        "jsonrpc": "2.0",
                        "id": jsonrpc_id,
                        "error": {
                            "code": -32000,
                            "message": str(exc)
                        }
                    }
                )

        # Método desconocido
        print(f"[MCP] Unknown method: {method}")
        return JSONResponse(
            status_code=404,
            content={
                "jsonrpc": "2.0",
                "id": jsonrpc_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
        )

    # --- Legacy format {action, input} ---
    if isinstance(data, dict) and "action" in data:
        name = data.get("action")
        arguments = data.get("input", {}) or {}
        
        print(f"[MCP] Legacy action call: {name}")
        
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
                    raise ValueError("Missing required parameters")
                result_data = get_view_details(workspace_id, view_id)
            elif name == "export_view_v2":
                workspace_id = arguments.get("workspace_id")
                view = arguments.get("view")
                if not (workspace_id and view):
                    raise ValueError("Missing required parameters")
                limit = int(arguments.get("limit", 100))
                offset = int(arguments.get("offset", 0))
                result_data = export_view(workspace_id, view, limit, offset)
            elif name == "query_v2":
                workspace_id = arguments.get("workspace_id")
                sql = arguments.get("sql")
                if not (workspace_id and sql):
                    raise ValueError("Missing required parameters")
                result_data = query_data(workspace_id, sql)
            else:
                return JSONResponse(
                    status_code=404,
                    content={"ok": False, "error": f"Unknown action: {name}"}
                )

            return {"ok": True, "action": name, "result": result_data}
            
        except Exception as exc:
            print(f"[MCP] Legacy action error: {exc}")
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": str(exc)}
            )

    # Payload no reconocido
    print(f"[MCP] Invalid request format")
    return JSONResponse(
        status_code=400,
        content={
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": "Invalid request"
            }
        }
    )

# El endpoint /mcp/ (con trailing slash) puede permanecer como alias
@app.post("/mcp/")
async def mcp_invoke_alias(
    payload: Optional[dict] = Body(default=None),
    request: Request = None,
):
    """Alias con trailing slash."""
    return await mcp_invoke(payload=payload, request=request)
