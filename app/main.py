from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .zoho_client_v2 import v2_export_view, v2_sql_query

app = FastAPI(title="Zoho Analytics MCP (v2)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {
        "status": "UP",
        "workspace_id": settings.ZOHO_WORKSPACE_ID,
        "workspace": settings.ZOHO_WORKSPACE,
        "mode": "v2",
    }

# ---------- v2: Export de vista/tabla ----------
@app.post("/view_v2")
def view_v2(payload: dict = Body(...)):
    """
    Body:
    {
      "view": "VENDEDORES_DIFERENTES_COMPLETO",
      "limit": 10,         # opcional
      "offset": 0,         # opcional
      "workspace_id": "..."# opcional (si no, usa env)
    }
    """
    view = payload.get("view")
    if not view:
        return {"detail": "Falta 'view'."}
    limit = payload.get("limit")
    offset = payload.get("offset", 0)
    ws_id = payload.get("workspace_id")

    data = v2_export_view(view=view, limit=limit, offset=offset, workspace_id=ws_id)
    return data

# ---------- v2: SQL ----------
@app.post("/query_v2")
def query_v2(payload: dict = Body(...)):
    """
    Body:
    {
      "sql": "SELECT * FROM \"VENDEDORES_DIFERENTES_COMPLETO\" LIMIT 5",
      "workspace_id": "..."  # opcional
    }
    """
    sql = payload.get("sql")
    if not sql:
        return {"detail": "Falta 'sql'."}
    ws_id = payload.get("workspace_id")
    data = v2_sql_query(sql=sql, workspace_id=ws_id)
    return data
