from __future__ import annotations
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, Optional
from .config import settings
from .zoho_client import export_sql, export_view_or_table, get_access_token

app = FastAPI(title="Zoho Analytics MCP", version="1.0.0")

# CORS abierto para pruebas
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    sql: str = Field(..., description="Consulta SQL completa")
    workspace: Optional[str] = Field(None, description="Workspace; default env")
    # No imponemos limit aquí, lo haces en SQL si quieres.

class ViewSmartRequest(BaseModel):
    view: str = Field(..., description="Nombre de vista/tabla EXACTO en Zoho")
    workspace: Optional[str] = None
    limit: Optional[int] = Field(None, ge=1, le=20000)
    offset: Optional[int] = Field(0, ge=0)

@app.get("/health")
def health():
    return {"status": "UP", "workspace": settings.ZOHO_WORKSPACE}

@app.get("/token-check")
def token_check():
    """
    Intenta renovar un access token y lo oculta.
    Útil para verificar configuración OAuth/entorno.
    """
    try:
        token = get_access_token()
        return {"ok": True, "token_len": len(token)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
def run_query(req: QueryRequest = Body(...)):
    try:
        data = export_sql(req.sql, workspace=req.workspace)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/view_smart")
def view_smart(req: ViewSmartRequest = Body(...)):
    try:
        data = export_view_or_table(
            view_or_table=req.view,
            workspace=req.workspace,
            limit=req.limit,
            offset=req.offset,
        )
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {
        "name": "Zoho Analytics MCP",
        "docs": "/docs",
        "health": "/health",
        "query": "/query (POST)",
        "view": "/view_smart (POST)"
    }
