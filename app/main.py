from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from app.zoho_client import list_workspaces, list_views, get_access_token, health_info

app = FastAPI(title="Zoho Analytics MCP (v2)", version="2.0")

@app.get("/health")
def health():
    try:
        info = health_info()
        return info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/workspaces_v2")
def workspaces_v2(limit: Optional[int] = Query(None, ge=1, le=100)):
    try:
        items = list_workspaces(limit)
        return {"count": len(items), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/views_v2")
def views_v2(workspace_id: str, limit: Optional[int] = Query(None, ge=1, le=200)):
    try:
        items = list_views(workspace_id, limit)
        return {"count": len(items), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
