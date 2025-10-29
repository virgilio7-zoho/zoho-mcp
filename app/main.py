from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict

# 👇 Asegúrate que estos nombres coincidan con tu archivo cliente
from app.zoho_client import (
    list_views_v2,     # def list_views_v2(workspace_id: str) -> dict
    v2_export_view,    # def v2_export_view(workspace_id: str, view: Optional[str], view_id: Optional[str], limit: int, offset: int) -> dict
    v2_sql_query,      # def v2_sql_query(workspace_id: str, sql: str) -> dict
    health_info        # def health_info() -> dict
)

app = FastAPI(
    title="Zoho Analytics MCP (v2)",
    description=(
        "API MCP para consultar Zoho Analytics usando la **REST API v2**.\n\n"
        "Variables de entorno esperadas (nombres oficiales de MCP):\n\n"
        "- `ANALYTICS_CLIENT_ID`\n"
        "- `ANALYTICS_CLIENT_SECRET`\n"
        "- `ANALYTICS_REFRESH_TOKEN`\n"
        "- `ANALYTICS_ORG_ID`\n"
        "- `ANALYTICS_MCP_DATA_DIR`\n"
        "- `ACCOUNTS_SERVER_URL` (p.ej. https://accounts.zoho.com)\n"
        "- `ANALYTICS_SERVER_URL` (p.ej. https://analyticsapi.zoho.com)\n"
    ),
    version="2.0.0",
    openapi_tags=[
        {"name": "Health", "description": "Estado general del servicio y configuración básica."},
        {"name": "Views", "description": "Listado de vistas/tablas dentro de un Workspace de Zoho Analytics."},
        {"name": "Data", "description": "Extracción de filas de una vista/tabla usando REST v2."},
        {"name": "SQL", "description": "Ejecución de consultas SQL (v2) con manejo de jobs/polling."},
    ],
)

# CORS amplio (ajústalo si quieres restringir orígenes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
#   Pydantic Models
# =========================
class ViewsV2Response(BaseModel):
    views: List[Dict[str, Any]] = Field(
        ...,
        description="Lista de views del workspace (tablas, queries, pivot, etc.)."
    )


class DataV2Request(BaseModel):
    workspace_id: str = Field(..., description="ID del Workspace", example="208617700000002085")
    view: Optional[str] = Field(None, description="Nombre exacto de la vista/tabla", example="VENDEDORES_DIFERENTES_COMPLETO")
    view_id: Optional[str] = Field(None, description="ID interno de la vista (alternativa a 'view')", example="2086177000000176621")
    limit: int = Field(10, ge=1, le=1000, description="Número de filas a traer", example=10)
    offset: int = Field(0, ge=0, description="Desplazamiento para paginar", example=0)

    class Config:
        json_schema_extra = {
            "example": {
                "workspace_id": "208617700000002085",
                "view": "VENDEDORES_DIFERENTES_COMPLETO",
                "limit": 10,
                "offset": 0
            }
        }


class DataV2Response(BaseModel):
    columns: List[str] = Field(..., description="Lista de columnas (en orden).")
    rows: List[List[Any]] = Field(..., description="Matriz de datos (filas).")
    count: int = Field(..., description="Número de filas devueltas en esta página.")


class QueryV2Request(BaseModel):
    workspace_id: str = Field(..., description="ID del Workspace", example="208617700000002085")
    sql: str = Field(..., description="Consulta SQL válida para Zoho Analytics (v2).",
                     example='SELECT * FROM "VENDEDORES_DIFERENTES_COMPLETO" LIMIT 5')

    class Config:
        json_schema_extra = {
            "example": {
                "workspace_id": "208617700000002085",
                "sql": 'SELECT * FROM "VENDEDORES_DIFERENTES_COMPLETO" LIMIT 5'
            }
        }


class QueryV2Response(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    count: int


# =========================
#   Endpoints
# =========================
@app.get("/health", tags=["Health"])
def health():
    """
    Estado del servicio y un resumen de la configuración activa (modo v2).
    """
    return health_info()


@app.get("/views_v2", response_model=ViewsV2Response, tags=["Views"])
def views_v2(
    workspace_id: str = Query(..., description="Workspace ID", example="208617700000002085")
):
    """
    Devuelve la lista de **views** de un Workspace (tablas, consultas, pivots,...).
    """
    try:
        data = list_views_v2(workspace_id)
        return {"views": data.get("views", data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data_v2", response_model=DataV2Response, tags=["Data"])
def data_v2(payload: DataV2Request = Body(...)):
    """
    Devuelve filas de una **vista/tabla**. Puedes pasar `view` (nombre exacto) o `view_id`.
    """
    if not payload.view and not payload.view_id:
        raise HTTPException(status_code=422, detail="Debes especificar 'view' o 'view_id'.")

    try:
        res = v2_export_view(
            workspace_id=payload.workspace_id,
            view=payload.view,
            view_id=payload.view_id,
            limit=payload.limit,
            offset=payload.offset,
        )
        # Normalizamos: espera keys 'columns', 'rows', 'count'
        return {
            "columns": res.get("columns", []),
            "rows": res.get("rows", []),
            "count": res.get("count", len(res.get("rows", []))),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query_v2", response_model=QueryV2Response, tags=["SQL"])
def query_v2(payload: QueryV2Request = Body(...)):
    """
    Ejecuta una **consulta SQL** sobre un Workspace (v2). El servidor maneja el polling del job.
    """
    try:
        res = v2_sql_query(workspace_id=payload.workspace_id, sql=payload.sql)
        return {
            "columns": res.get("columns", []),
            "rows": res.get("rows", []),
            "count": res.get("count", len(res.get("rows", []))),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
