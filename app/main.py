from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import os

from .config import VIEW as DEFAULT_VIEW, WORKSPACE as DEFAULT_WORKSPACE
from .zoho_client import run_sql

app = FastAPI(
    title="Zoho Analytics MCP",
    version="1.1.0",
    description="HTTP MCP para consultar Zoho Analytics (errores claros + debug)."
)

class SQLRequest(BaseModel):
    sql: str = Field(..., description="Consulta SQL para Zoho Analytics")
    view: Optional[str] = Field(default=None, description="Tabla/Vista destino")
    workspace: Optional[str] = Field(default=None, description="Workspace destino")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug/env")
def debug_env():
    # Muestra qué variables están presentes (no revela sus valores)
    def present(name): return "set" if os.getenv(name) else "missing"
    return {
        "ZOHO_ACCOUNTS_BASE": present("ZOHO_ACCOUNTS_BASE"),
        "ZOHO_ANALYTICS_API_BASE": present("ZOHO_ANALYTICS_API_BASE"),
        "ZOHO_CLIENT_ID": present("ZOHO_CLIENT_ID"),
        "ZOHO_CLIENT_SECRET": present("ZOHO_CLIENT_SECRET"),
        "ZOHO_REFRESH_TOKEN": present("ZOHO_REFRESH_TOKEN"),
        "ZOHO_OWNER_ORG": present("ZOHO_OWNER_ORG"),
        "ZOHO_WORKSPACE": present("ZOHO_WORKSPACE"),
        "ZOHO_VIEW": present("ZOHO_VIEW"),
        # Compatibilidad con tus nombres antiguos:
        "ANALYTICS_CLIENT_ID": present("ANALYTICS_CLIENT_ID"),
        "ANALYTICS_CLIENT_SECRET": present("ANALYTICS_CLIENT_SECRET"),
        "ANALYTICS_REFRESH_TOKEN": present("ANALYTICS_REFRESH_TOKEN"),
        "ANALYTICS_ORG_ID": present("ANALYTICS_ORG_ID"),
        "ACCOUNTS_SERVER_URL": present("ACCOUNTS_SERVER_URL"),
        "ANALYTICS_SERVER_URL": present("ANALYTICS_SERVER_URL"),
    }
@app.get("/debug/workspaces")
def list_workspaces():
    from .zoho_oauth import ZohoOAuth
    import requests, os
    token = ZohoOAuth.get_access_token()
    base = os.getenv("ANALYTICS_SERVER_URL") or os.getenv("ZOHO_ANALYTICS_API_BASE")
    url = f"{base}/restapi/v2/workspaces"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "text": resp.text[:400]}

@app.get("/debug/views")
def list_views(workspace: str):
    from .zoho_oauth import ZohoOAuth
    import requests, os
    token = ZohoOAuth.get_access_token()
    base = os.getenv("ANALYTICS_SERVER_URL") or os.getenv("ZOHO_ANALYTICS_API_BASE")
    url = f"{base}/restapi/v2/workspaces/{workspace}/tables"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "text": resp.text[:400]}

from pydantic import BaseModel

class ViewRequest(BaseModel):
    workspace: str
    view: str
    limit: int | None = 100
    offset: int | None = 0
    columns: str | None = None
    criteria: str | None = None
    workspace_id: str | None = None  # <-- NUEVO (opcional)

@app.post("/view", summary="Read data from a view (REST v2)")
def view_data(req: ViewRequest):
    from .zoho_client import get_view_data
    data = get_view_data(
        workspace=req.workspace,
        view=req.view,
        limit=req.limit or 100,
        offset=req.offset or 0,
        columns=req.columns,
        criteria=req.criteria,
        workspace_id=req.workspace_id,   # <-- NUEVO
    )
    return data
@app.post("/query")
def query_sql(body: SQLRequest):
    view = body.view or DEFAULT_VIEW
    workspace = body.workspace or DEFAULT_WORKSPACE
    if not workspace or not view:
        raise HTTPException(status_code=400, detail="Falta 'workspace' o 'view' (envíalos en el body o configura defaults).")
    try:
        data = run_sql(workspace=workspace, view=view, sql=body.sql)
        return {"status": "ok", "workspace": workspace, "view": view, "rows": data}
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"App error: {e.__class__.__name__}")

