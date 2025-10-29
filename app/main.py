from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from .config import settings
from .zoho_client_v2 import list_workspaces, list_views, get_access_token

app = FastAPI(title="Zoho Analytics MCP (v2-ready)")

@app.get("/health")
def health():
    # Fuerza token para validar credenciales; si falla, FastAPI devuelve 500 con el error
    get_access_token()
    return {"status": "UP", "mode": "v2", "org": settings.ANALYTICS_ORG_ID}

@app.get("/workspaces_v2")
def workspaces_v2(limit: Optional[int] = Query(None, ge=1, le=100)):
    try:
        items = list_workspaces(limit or settings.WORKSPACE_RESULT_LIMIT)
        return {"count": len(items), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/views_v2")
def views_v2(workspace_id: str, limit: Optional[int] = Query(None, ge=1, le=200)):
    try:
        items = list_views(workspace_id, limit or settings.VIEW_RESULT_LIMIT)
        # Si quieres solo TABLAS:
        # items = [v for v in items if v.get("type","").upper() in ("TABLE","TABULAR VIEW")]
        return {"count": len(items), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
