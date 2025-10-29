# app/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .zoho_client import (
    health_status,
    export_view_or_table,
    export_sql,
)

app = FastAPI(title="Zoho Analytics MCP", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ViewSmartRequest(BaseModel):
    view: str = Field(..., description='Nombre exacto de la vista/tabla')
    limit: int | None = Field(default=10)
    offset: int = Field(default=0)


class QueryRequest(BaseModel):
    sql: str = Field(..., description='Consulta SQL, ej: SELECT * FROM "Tabla" LIMIT 10')


@app.get("/health")
def health():
    return health_status()


@app.post("/view_smart")
def view_smart(req: ViewSmartRequest):
    try:
        data = export_view_or_table(req.view, limit=req.limit, offset=req.offset)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query")
def query(req: QueryRequest):
    try:
        data = export_sql(req.sql)
        return data
    except Exception as e:
        # tip común: si mandas texto plano en lugar de JSON, FastAPI te dará 422.
        raise HTTPException(status_code=500, detail=str(e))
