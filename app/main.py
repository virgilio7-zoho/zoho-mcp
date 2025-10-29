# app/main.py
from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.zoho_client import (
    health_info,
    get_workspaces_list,         # get_workspaces_list
    search_views,                # search_views
    get_view_details,            # get_view_details
    export_view,                 # export_view
    query_data,                  # query_data
)

app = FastAPI(title="Zoho Analytics MCP (v2) — Tools oficiales")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Health ----------
@app.get("/health")
def health():
    return health_info()


# ---------- get_workspaces_list ----------
@app.get("/workspaces_v2")
def workspaces_v2():
    return get_workspaces_list()


# ---------- search_views ----------
@app.get("/views_v2")
def views_v2(
    workspace_id: str = Query(..., description="Workspace ID"),
    q: str | None = Query(None, description="Texto a buscar"),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    return search_views(workspace_id, q, limit, offset)


# ---------- get_view_details ----------
@app.get("/view_details_v2")
def view_details_v2(
    workspace_id: str = Query(..., description="Workspace ID"),
    view_id: str = Query(..., description="View ID o nombre exacto"),
):
    return get_view_details(workspace_id, view_id)


# ---------- export_view ----------
class ExportViewBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    view: str = Field(..., description="ID o nombre de la vista/tabla")
    limit: int = Field(100, ge=1, le=10000)
    offset: int = Field(0, ge=0)

@app.post("/export_view_v2")
def export_view_v2(payload: ExportViewBody = Body(...)):
    return export_view(payload.workspace_id, payload.view, payload.limit, payload.offset)


# ---------- query_data ----------
class QueryBody(BaseModel):
    workspace_id: str = Field(..., description="Workspace ID")
    sql: str = Field(..., description="Consulta SQL")

@app.post("/query_v2")
def query_v2(payload: QueryBody = Body(...)):
    return query_data(payload.workspace_id, payload.sql)
