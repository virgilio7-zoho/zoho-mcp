"""
Main entry point for the Zoho Analytics MCP server (API Key + Minimal OAuth).

- Auth ligera: X-API-Key o Bearer (emitido por este servidor)
- OAuth mínimo: /.well-known/* , /authorize, /token
- Endpoints de datos: workspaces_v2, views_v2, view_details_v2, export_view_v2, query_v2
"""

import os, secrets, time, json, asyncio
from datetime import datetime
from typing import Optional

from fastapi import (
    FastAPI, Query, Body, Request, Header, Depends, HTTPException, Form
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, RedirectResponse

from pydantic import BaseModel, Field

# ---- Cliente Zoho (tu módulo) ----
from .zoho_client import (
    health_info,
    get_workspaces_list,
    search_views,
    get_view_details,
    export_view,
    query_data,
)

# ============================================================================
# ============================  AUTH LIGERA  =================================
# ============================================================================

API_KEY = os.getenv("API_KEY", "").strip()

# Tokens emitidos por este servidor (en memoria)
# access_token -> exp_ts
_OAUTH_TOKENS: dict[str, float] = {}
# authorization_code -> (exp_ts, client_id, redirect_uri)
_OAUTH_CODES: dict[str, tuple[float, str, str]] = {}
# refresh_token -> exp_ts
_OAUTH_REFRESH: dict[str, float] = {}

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

# ============================================================================
# ===============================  APP  ======================================
# ============================================================================

app = FastAPI(title="Zoho Analytics MCP (v2) — Tools oficiales")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ----------------------------------------------------------------------------
# =======================  OAUTH MÍNIMO P/ CONECTOR  =========================
# ----------------------------------------------------------------------------

def _issuer(req: Request) -> str:
    # construye issuer con host/puerto correctos
    return f"{req.url.scheme}://{req.url.netloc}"

@app.get("/.well-known/oauth-protected-resource", include_in_schema=False)
def oauth_protected_resource(req: Request):
    """
    Metadata del recurso protegido: indica qué Authorization Server usar.
    """
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_servers": [base],
    }

@app.get("/.well-known/oauth-authorization-server", include_in_schema=False)
def oauth_authorization_server(req: Request):
    """
    Metadata del Authorization Server (AS).
    """
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/authorize",
        "token_endpoint": f"{base}/token",
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "scopes_supported": ["default"],
    }

@app.get("/.well-known/openid-configuration", include_in_schema=False)
def openid_configuration(req: Request):
    """
    Algunos clientes consultan también esta ruta. Devolvemos lo mismo.
    """
    base = _issuer(req)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/authorize",
        "token_endpoint": f"{base}/token",
        "scopes_supported": ["default"],
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256", "plain"],
    }

@app.get("/authorize", include_in_schema=False)
def oauth_authorize(
    req: Request,
    response_type: str = Query(...),          # "code"
    client_id: str = Query(...),              # el conector suele mandar algo (p.ej. "chatgpt")
    redirect_uri: str = Query(...),
    scope: str = Query("default"),
    state: str = Query(""),
    code_challenge: str | None = Query(None),         # toleramos ambos
    code_challenge_method: str | None = Query(None),  # "S256" | "plain"
):
    """
    Consent inmediato y redirección con `code`.
    """
    if response_type != "code":
        raise HTTPException(status_code=400, detail="unsupported_response_type")

    code = secrets.token_urlsafe(24)
    _OAUTH_CODES[code] = (time.time() + 120, client_id, redirect_uri)  # 2 min

    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(url=f"{redirect_uri}{sep}code={code}&state={state}")

# ---- helpers de /token para soportar JSON y x-www-form-urlencoded ----
def _body_value(body: dict, name: str, default=None):
    # admite claves tanto embed=True como planas
    if name in body:
        return body[name]
    # algunas libs mandan param como {"grant_type":"..."} o {"grant_type":["..."]}
    v = body.get(name, default)
    if isinstance(v, list) and v:
        return v[0]
    return v if v is not None else default

@app.post("/token", include_in_schema=False)
async def oauth_token(
    request: Request,
    # JSON body (embed=True) — opcional: si viene en JSON, FastAPI lo captura
    grant_type_json: Optional[str] = Body(None, embed=True),
    code_json: Optional[str] = Body(None, embed=True),
    redirect_uri_json: Optional[str] = Body(None, embed=True),
    client_id_json: Optional[str] = Body(None, embed=True),
    refresh_token_json: Optional[str] = Body(None, embed=True),
    code_verifier_json: Optional[str] = Body(None, embed=True),
    # Form (x-www-form-urlencoded) — opcional: si viene en form, FastAPI lo captura
    grant_type_form: Optional[str] = Form(None),
    code_form: Optional[str] = Form(None),
    redirect_uri_form: Optional[str] = Form(None),
    client_id_form: Optional[str] = Form(None),
    refresh_token_form: Optional[str] = Form(None),
    code_verifier_form: Optional[str] = Form(None),
):
    """
    Canjea authorization_code por access_token, o refresh_token por uno nuevo.
    Acepta JSON y x-www-form-urlencoded.
    """
    body = {}
    if request.headers.get("content-type", "").startswith("application/json"):
        # ya tenemos los *_json
        body = {
            "grant_type": grant_type_json,
            "code": code_json,
            "redirect_uri": redirect_uri_json,
            "client_id": client_id_json,
            "refresh_token": refresh_token_json,
            "code_verifier": code_verifier_json,
        }
    else:
        # x-www-form-urlencoded
        body = {
            "grant_type": grant_type_form,
            "code": code_form,
            "redirect_uri": redirect_uri_form,
            "client_id": client_id_form,
            "refresh_token": refresh_token_form,
            "code_verifier": code_verifier_form,
        }

    grant_type = _body_value(body, "grant_type")
    if grant_type not in ("authorization_code", "refresh_token"):
        raise HTTPException(status_code=400, detail="unsupported_grant_type")

    if grant_type == "authorization_code":
        code = _body_value(body, "code")
        redirect_uri = _body_value(body, "redirect_uri", "")
        client_id = _body_value(body, "client_id", "")

        if not code:
            raise HTTPException(status_code=400, detail="invalid_request")

        data = _OAUTH_CODES.pop(code, None)
        if not data:
            raise HTTPException(status_code=400, detail="invalid_grant")

        exp_ts, expected_client, expected_redirect = data
        if exp_ts < time.time():
            raise HTTPException(status_code=400, detail="invalid_grant")

        # (No exigimos coincidencia estricta de client/redirect para simplificar,
        #  pero si quieres, descomenta estas líneas:)
        # if expected_client and client_id and client_id != expected_client:
        #     raise HTTPException(status_code=400, detail="invalid_client")
        # if expected_redirect and redirect_uri and redirect_uri != expected_redirect:
        #     raise HTTPException(status_code=400, detail="invalid_request")

        access_token = secrets.token_urlsafe(32)
        refresh_token = secrets.token_urlsafe(32)
        _OAUTH_TOKENS[access_token] = time.time() + 3600   # 1h
        _OAUTH_REFRESH[refresh_token] = time.time() + 30 * 24 * 3600  # 30 días

        return {
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": 3600,
            "refresh_token": refresh_token,
            "scope": "default",
        }

    # grant_type == "refresh_token"
    refresh_token = _body_value(body, "refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="invalid_request")

    exp = _OAUTH_REFRESH.get(refresh_token)
    if not exp or exp < time.time():
        raise HTTPException(status_code=400, detail="invalid_grant")

    access_token = secrets.token_urlsafe(32)
    _OAUTH_TOKENS[access_token] = time.time() + 3600  # 1h
    return {
        "token_type": "Bearer",
        "access_token": access_token,
        "expires_in": 3600,
        "refresh_token": refresh_token,
        "scope": "default",
    }

# ----------------------------------------------------------------------------
# =============================  ENDPOINTS  ==================================
# ----------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return health_info()

@app.get("/workspaces_v2", dependencies=[Depends(require_key_or_bearer)])
def workspaces_v2() -> dict:
    return get_workspaces_list()

@app.get("/views_v2", dependencies=[Depends(require_key_or_bearer)])
def views_v2(
    workspace_id: str = Query(..., description="Workspace ID"),
    q: str | None = Query(None, description="Texto a buscar"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict:
    return search_views(workspace_id, q, limit, offset)

@app.get("/view_details_v2", dependencies=[Depends(require_key_or_bearer)])
def view_details_v2(
    workspace_id: str = Query(..., description="Compatibilidad (no lo usa la API subyacente)"),
    view_id: str = Query(..., description="View ID o nombre exacto"),
) -> dict:
    return get_view_details(workspace_id, view_id)

class ExportViewBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    view: str = Field(..., description="ID o nombre de la vista/tabla")
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)

@app.post("/export_view_v2", dependencies=[Depends(require_key_or_bearer)])
def export_view_v2(payload: ExportViewBody = Body(...)) -> dict:
    return export_view(payload.workspace_id, payload.view, payload.limit, payload.offset)

class QueryBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    sql: str = Field(..., description="Consulta SQL")

@app.post("/query_v2", dependencies=[Depends(require_key_or_bearer)])
def query_v2(payload: QueryBody = Body(...)) -> dict:
    return query_data(payload.workspace_id, payload.sql)

# ----------------------------------------------------------------------------
# ===========================  MCP / JSON-RPC  ===============================
# ----------------------------------------------------------------------------

ACTIONS: list[dict] = [
    {"name": "workspaces_v2","description": "List the workspaces available to the authenticated user.","parameters": {"type": "object","properties": {},"additionalProperties": False}},
    {"name": "views_v2","description": "Search or list views within a workspace.","parameters": {"type": "object","properties": {"workspace_id": {"type": "string"},"q": {"type": ["string","null"]},"limit": {"type": "integer","minimum": 1,"maximum": 2000},"offset": {"type": "integer","minimum": 0}},"required": ["workspace_id"],"additionalProperties": False}},
    {"name": "view_details_v2","description": "Retrieve metadata for a specific view.","parameters": {"type": "object","properties": {"workspace_id": {"type": "string"},"view_id": {"type": "string"}},"required": ["workspace_id","view_id"],"additionalProperties": False}},
    {"name": "export_view_v2","description": "Export data from a specific view.","parameters": {"type": "object","properties": {"workspace_id": {"type": "string"},"view": {"type": "string"},"limit": {"type": "integer","minimum": 1,"maximum": 10000},"offset": {"type": "integer","minimum": 0}},"required": ["workspace_id","view"],"additionalProperties": False}},
    {"name": "query_v2","description": "Execute a SQL query against a workspace.","parameters": {"type": "object","properties": {"workspace_id": {"type": "string"},"sql": {"type": "string"}},"required": ["workspace_id","sql"],"additionalProperties": False}},
]

def _sse_frame(event: str, data_obj: dict) -> bytes:
    payload = json.dumps(data_obj, ensure_ascii=False)
    return (f"event: {event}\n" + f"data: {payload}\n\n").encode("utf-8")

@app.get("/sse")
async def sse_actions(request: Request) -> StreamingResponse:
    async def event_generator():
        yield _sse_frame("actions", {"actions": ACTIONS})
        for _ in range(5):
            if await request.is_disconnected():
                break
            await asyncio.sleep(1)
            yield b": keep-alive\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream")

TOOL_DEFINITIONS: list[dict] = [
    {"name":"workspaces_v2","title":"List Workspaces","description":"List all workspaces available to the authenticated user.","inputSchema":{"type":"object","properties": {},"additionalProperties": False}},
    {"name":"views_v2","title":"Search Views","description":"Search or list views within a workspace.","inputSchema":{"type":"object","properties":{"workspace_id":{"type":"string"},"q":{"type":["string","null"]},"limit":{"type":"integer","minimum":1,"maximum":2000},"offset":{"type":"integer","minimum":0}},"required":["workspace_id"],"additionalProperties":False}},
    {"name":"view_details_v2","title":"View Details","description":"Retrieve metadata for a specific view.","inputSchema":{"type":"object","properties":{"workspace_id":{"type":"string"},"view_id":{"type":"string"}},"required":["workspace_id","view_id"],"additionalProperties":False}},
    {"name":"export_view_v2","title":"Export View","description":"Export data from a specific view.","inputSchema":{"type":"object","properties":{"workspace_id":{"type":"string"},"view":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10000},"offset":{"type":"integer","minimum":0}},"required":["workspace_id","view"],"additionalProperties":False}},
    {"name":"query_v2","title":"Execute SQL","description":"Execute a SQL query against a workspace.","inputSchema":{"type":"object","properties":{"workspace_id":{"type":"string"},"sql":{"type":"string"}},"required":["workspace_id","sql"],"additionalProperties":False}},
]

@app.post("/mcp")
async def mcp_invoke(
    payload: Optional[dict] = Body(default=None),
    request: Request = None,
):
    if payload is not None:
        data = payload
    else:
        try:
            body_bytes = await request.body()
            data = json.loads(body_bytes.decode()) if body_bytes else {}
        except Exception:
            return JSONResponse(status_code=400, content={"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"}})

    if isinstance(data, dict) and data.get("jsonrpc") == "2.0":
        jsonrpc_id = data.get("id")
        method = data.get("method")
        params = data.get("params", {}) or {}

        if method == "tools/list":
            return {"jsonrpc":"2.0","id":jsonrpc_id,"result":{"tools":TOOL_DEFINITIONS}}

        if method == "initialize":
            requested = params.get("protocolVersion")
            protocol_version = requested or datetime.utcnow().strftime("%Y-%m-%d")
            capabilities = {"tools":{"listChanged": False}}
            server_info = {"name":"Zoho Analytics MCP","version":"0.1.0"}
            return {"jsonrpc":"2.0","id":jsonrpc_id,"result":{"protocolVersion":protocol_version,"capabilities":capabilities,"serverInfo":server_info}}

        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments", {}) or {}
            try:
                if name == "workspaces_v2":
                    result_data = get_workspaces_list()
                elif name == "views_v2":
                    wid = arguments.get("workspace_id")
                    if not wid: raise ValueError("Missing workspace_id")
                    result_data = search_views(wid, arguments.get("q"), int(arguments.get("limit",200)), int(arguments.get("offset",0)))
                elif name == "view_details_v2":
                    wid, vid = arguments.get("workspace_id"), arguments.get("view_id")
                    if not (wid and vid): raise ValueError("Missing workspace_id/view_id")
                    result_data = get_view_details(wid, vid)
                elif name == "export_view_v2":
                    wid, view = arguments.get("workspace_id"), arguments.get("view")
                    if not (wid and view): raise ValueError("Missing workspace_id/view")
                    limit, offset = int(arguments.get("limit",100)), int(arguments.get("offset",0))
                    result_data = export_view(wid, view, limit, offset)
                elif name == "query_v2":
                    wid, sql = arguments.get("workspace_id"), arguments.get("sql")
                    if not (wid and sql): raise ValueError("Missing workspace_id/sql")
                    result_data = query_data(wid, sql)
                else:
                    return JSONResponse(status_code=404, content={"jsonrpc":"2.0","id":jsonrpc_id,"error":{"code":-32601,"message":f"Unknown tool: {name}"}})
                return {"jsonrpc":"2.0","id":jsonrpc_id,"result":{"content":[{"type":"json","value":result_data}]}}
            except Exception as exc:
                return JSONResponse(status_code=400, content={"jsonrpc":"2.0","id":jsonrpc_id,"error":{"code":-32000,"message":str(exc)}})

        return JSONResponse(status_code=404, content={"jsonrpc":"2.0","id":jsonrpc_id,"error":{"code":-32601,"message":f"Method not found: {method}"}})

    if isinstance(data, dict) and "action" in data:
        name = data.get("action")
        args = data.get("input", {}) or {}
        try:
            if name == "workspaces_v2":
                result_data = get_workspaces_list()
            elif name == "views_v2":
                wid = args.get("workspace_id")
                if not wid: raise ValueError("Missing workspace_id")
                result_data = search_views(wid, args.get("q"), int(args.get("limit",200)), int(args.get("offset",0)))
            elif name == "view_details_v2":
                wid, vid = args.get("workspace_id"), args.get("view_id")
                if not (wid and vid): raise ValueError("Missing workspace_id/view_id")
                result_data = get_view_details(wid, vid)
            elif name == "export_view_v2":
                wid, view = args.get("workspace_id"), args.get("view")
                if not (wid and view): raise ValueError("Missing workspace_id/view")
                limit, offset = int(args.get("limit",100)), int(args.get("offset",0))
                result_data = export_view(wid, view, limit, offset)
            elif name == "query_v2":
                wid, sql = args.get("workspace_id"), args.get("sql")
                if not (wid and sql): raise ValueError("Missing workspace_id/sql")
                result_data = query_data(wid, sql)
            else:
                return JSONResponse(status_code=404, content={"ok": False, "error": f"Unknown action: {name}"})
            return {"ok": True, "action": name, "result": result_data}
        except Exception as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})

    return JSONResponse(status_code=400, content={"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid request"}})

@app.post("/mcp/")
async def mcp_invoke_alias(
    payload: Optional[dict] = Body(default=None),
    request: Request = None,
):
    return await mcp_invoke(payload=payload, request=request)
