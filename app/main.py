from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware

from app.zoho_client import (
    export_view_or_table,
    export_sql,
    health_status,
)

app = FastAPI(title="Zoho Analytics MCP", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return health_status()

# -------- Export por nombre de vista/tabla (C1/GET interno) --------
@app.post("/view_smart")
def view_smart(payload: dict = Body(...)):
    """
    Body JSON:
    {
      "view": "VENDEDORES_DIFERENTES_COMPLETO",
      "limit": 10,          # opcional
      "offset": 0,          # opcional
      "workspace": "MARKEM" # opcional (usa env por defecto)
    }
    """
    view = payload.get("view", "")
    limit = payload.get("limit")
    offset = int(payload.get("offset", 0))
    workspace = payload.get("workspace")
    data = export_view_or_table(view, workspace=workspace, limit=limit, offset=offset)
    return data

# -------- SQL (POST) --------
@app.post("/query")
def query(payload: dict = Body(...)):
    """
    Body JSON:
    {
      "sql": "SELECT * FROM \"VENDEDORES_DIFERENTES_COMPLETO\" LIMIT 10",
      "limit": 100,         # opcional
      "offset": 0,          # opcional
      "workspace": "MARKEM" # opcional
    }
    """
    sql = payload.get("sql", "")
    limit = payload.get("limit")
    offset = int(payload.get("offset", 0))
    workspace = payload.get("workspace")
    data = export_sql(sql, workspace=workspace, limit=limit, offset=offset)
    return data
