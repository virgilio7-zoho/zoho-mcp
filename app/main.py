from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
import os
import requests

# ---------------------------
# FastAPI app & CORS
# ---------------------------
app = FastAPI(title="Zoho Analytics MCP", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajusta si quieres restringir
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# Health / root
# ---------------------------
@app.get("/", summary="Root")
def root():
    return {"status": "ok", "service": "zoho-mcp"}

@app.get("/health", summary="Healthcheck")
def health():
    return {"status": "ok"}

# ---------------------------
# Modelos de entrada
# ---------------------------
class QueryRequest(BaseModel):
    sql: str
    view: str
    workspace: str

class ViewRequest(BaseModel):
    workspace: str
    view: str
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    columns: Optional[str] = None     # "Mes,Ventas,Region"
    criteria: Optional[str] = None    # "Mes = '2024-09'"
    workspace_id: Optional[str] = None  # "2086177000000002085" (opcional)

class SmartViewReq(BaseModel):
    workspace: str
    view: str
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    columns: Optional[str] = None
    criteria: Optional[str] = None
    workspace_id: Optional[str] = None

# ---------------------------
# Endpoints principales
# ---------------------------
@app.post("/query", summary="Run SQL (SQLEXPORT)")
def query(req: QueryRequest):
    # run_sql es un alias hacia sql_export(workspace, sql) definido en zoho_client
    from .zoho_client import run_sql
    result = run_sql(req.workspace, req.view, req.sql)
    return result

@app.post("/view", summary="Read data from a view (auto, alias)")
def view_data(req: ViewRequest):
    # get_view_data es un alias que delega a smart_view_export(...)
    from .zoho_client import get_view_data
    data = get_view_data(
        workspace=req.workspace,
        view=req.view,
        limit=req.limit or 100,
        offset=req.offset or 0,
        columns=req.columns,
        criteria=req.criteria,
        workspace_id=req.workspace_id,
    )
    return data

@app.post("/view_smart", summary="Fetch view data (auto-detect API flavor)")
def view_smart(req: SmartViewReq):
    from .zoho_client import smart_view_export
    data = smart_view_export(
        workspace=req.workspace,
        view=req.view,
        limit=req.limit or 100,
        offset=req.offset or 0,
        columns=req.columns,
        criteria=req.criteria,
        workspace_id=req.workspace_id,
    )
    return data

# ---------------------------
# Endpoints de depuración
# ---------------------------
@app.get("/debug/env", summary="Show selected environment variables")
def debug_env():
    keys = [
        "ANALYTICS_SERVER_URL",
        "ANALYTICS_ORG_ID",
        "ACCOUNTS_SERVER_URL",
        "ZOHO_ANALYTICS_API_BASE",
        # NO exponemos client_secret/refresh_token por seguridad
    ]
    return {k: os.getenv(k) for k in keys}

@app.get("/debug/workspaces", summary="List workspaces (REST v2)")
def list_workspaces():
    # Intenta ambas bases v2 para mayor compatibilidad
    base = (os.getenv("ANALYTICS_SERVER_URL") or "https://analyticsapi.zoho.com").rstrip("/")
    from .zoho_oauth import ZohoOAuth
    token = ZohoOAuth.get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    urls = [f"{base}/restapi/v2/workspaces", f"{base}/api/v2/workspaces"]
    last = None
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            last = (url, resp.status_code, resp.text[:600])
            if resp.status_code < 400:
                return resp.json()
        except Exception as e:
            last = (url, "EXC", str(e))
            continue
    return {"status": "failure", "last": last}

@app.get("/debug/views", summary="List views/tables in a workspace (REST v2)")
def list_views(
    workspace: str = Query(..., description="Name of workspace (e.g., MARKEM)"),
    workspace_id: Optional[str] = Query(None, description="Optional workspace id (e.g., 2086177000000002085)")
):
    base = (os.getenv("ANALYTICS_SERVER_URL") or "https://analyticsapi.zoho.com").rstrip("/")
    from .zoho_oauth import ZohoOAuth
    token = ZohoOAuth.get_access_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    ws_segment = workspace_id if workspace_id else workspace
    urls = [
        f"{base}/restapi/v2/workspaces/{ws_segment}/views",
        f"{base}/restapi/v2/workspaces/{ws_segment}/tables",
        f"{base}/api/v2/workspaces/{ws_segment}/views",
        f"{base}/api/v2/workspaces/{ws_segment}/tables",
    ]
    out = []
    last = None
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            last = (url, resp.status_code, resp.text[:400])
            if resp.status_code < 400:
                out.append({"url": url, "data": resp.json()})
        except Exception as e:
            last = (url, "EXC", str(e))
            continue

    if out:
        return {"status": "success", "results": out}
    return {"status": "failure", "last": last}
