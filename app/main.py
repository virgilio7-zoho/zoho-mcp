from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import os
import requests

from .zoho_client import (
    get_view_data,
    smart_view_export,
    run_sql,
    sql_export,
)
from .zoho_oauth import ZohoOAuth


app = FastAPI(title="Zoho MCP API", version="1.0.0")

# CORS amplio para pruebas desde navegador / ChatGPT MCP
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Modelos ----------
class ViewBody(BaseModel):
    workspace: str = Field(..., description="Nombre del workspace (p.ej. MARKEM)")
    view: str = Field(..., description="Nombre exacto de la vista/tabla en Zoho (respetar mayúsculas/espacios)")
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)
    columns: Optional[str] = Field(None, description="Lista de columnas separadas por coma (opcional)")
    criteria: Optional[str] = Field(None, description="Criterio de filtro (opcional)")
    workspace_id: Optional[str] = Field(None, description="ID del workspace (opcional)")

class QueryBody(BaseModel):
    workspace: str = Field(..., description="Nombre del workspace (p.ej. MARKEM)")
    view: Optional[str] = Field(None, description="Ignorado por Zoho SQL; se mantiene por compatibilidad")
    sql: str = Field(..., description="Consulta SQL Zoho (ej.: SELECT 1 FROM \"MiVista\")")


# ---------- Endpoints ----------
@app.get("/", summary="Bienvenida")
def root():
    return {"status": "ok", "message": "Zoho MCP API up"}

@app.get("/health", summary="Healthcheck simple")
def health():
    return {"status": "ok"}

@app.get("/debug/token", summary="Devuelve si hay token válido (no el token)")
def debug_token():
    try:
        token = ZohoOAuth.get_access_token()
        return {"has_token": bool(token), "len": len(token) if token else 0}
    except Exception as e:
        return {"error": str(e)}

@app.post("/view", summary="Obtiene datos de una vista/tabla (auto legacy/v2, firma simple)")
def view_data(body: ViewBody):
    data = get_view_data(
        workspace=body.workspace,
        view=body.view,
        limit=body.limit,
        offset=body.offset,
        columns=body.columns,
        criteria=body.criteria,
        workspace_id=body.workspace_id,
    )
    return data

@app.post("/view_smart", summary="Obtiene datos de una vista/tabla (detector completo de rutas)")
def view_smart(body: ViewBody):
    data = smart_view_export(
        workspace=body.workspace,
        view=body.view,
        limit=body.limit,
        offset=body.offset,
        columns=body.columns,
        criteria=body.criteria,
        workspace_id=body.workspace_id,
    )
    return data

@app.post("/query", summary="Ejecuta SQL con SQLEXPORT (legacy)")
def query_sql(body: QueryBody):
    # Mantener compatibilidad con código existente que importaba run_sql
    return run_sql(workspace=body.workspace, view=body.view or "", sql=body.sql)

# ---------- (Opcional) Listado legacy para confirmar nombres exactos ----------
@app.get("/debug/legacy_list", summary="Lista views/tables (legacy LISTVIEWS/LISTTABLES)")
def legacy_list(
    workspace: str = Query(..., description="Nombre del workspace (ej. MARKEM)"),
    owner_or_org: str = Query("owner", description="'owner' (default) o 'org'"),
):
    base = (os.getenv("ANALYTICS_SERVER_URL") or "https://analyticsapi.zoho.com").rstrip("/")
    org = os.getenv("ANALYTICS_ORG_ID")
    owner = os.getenv("ANALYTICS_OWNER_NAME")

    ws_enc = requests.utils.requote_uri(workspace)
    if owner_or_org == "owner" and owner:
        head = f"{base}/api/{requests.utils.requote_uri(owner)}/{ws_enc}"
    elif org:
        head = f"{base}/api/{org}/{ws_enc}"
    else:
        return {"status": "failure", "error": "Falta ANALYTICS_OWNER_NAME o ANALYTICS_ORG_ID"}

    token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    def call(url, form):
        try:
            r = requests.post(url, headers=headers, data=form, timeout=60)
            return {"url": url, "status": r.status_code, "body": r.text[:1200]}
        except Exception as e:
            return {"url": url, "error": str(e)}

    out = []
    out.append(call(f"{head}", {"ZOHO_ACTION": "LISTVIEWS", "ZOHO_OUTPUT_FORMAT": "JSON"}))
    out.append(call(f"{head}", {"ZOHO_ACTION": "LISTTABLES", "ZOHO_OUTPUT_FORMAT": "JSON"}))
    return {"results": out}
