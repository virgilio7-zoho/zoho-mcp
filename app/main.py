# app/main.py
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware

# Import “tolerante” a la estructura (si mueves archivos a raíz no rompe)
try:
    from app.zoho_client import v2_export_view, v2_sql_query, health_info
except ModuleNotFoundError:
    # fallback si el paquete es plano
    from zoho_client import v2_export_view, v2_sql_query, health_info  # type: ignore

app = FastAPI(title="Zoho MCP (v2)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return health_info()

@app.post("/view_v2")
def view_v2(body: dict = Body(...)):
    view = body.get("view")
    limit = int(body.get("limit", 100))
    offset = int(body.get("offset", 0))
    if not view:
        return {"detail": "Falta 'view'."}
    return v2_export_view(view, limit=limit, offset=offset)

@app.post("/query_v2")
def query_v2(body: dict = Body(...)):
    sql = body.get("sql")
    if not sql:
        return {"detail": "Falta 'sql'."}
    return v2_sql_query(sql)
