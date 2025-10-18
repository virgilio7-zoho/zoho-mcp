from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Optional
from .config import VIEW as DEFAULT_VIEW
from .zoho_client import run_sql

app = FastAPI(
    title="Zoho Analytics MCP",
    version="1.0.0",
    description="HTTP MCP para consultar Zoho Analytics con SQL seguro."
)

class SQLRequest(BaseModel):
    sql: str = Field(..., description="Consulta SQL para Zoho Analytics")
    view: Optional[str] = Field(default=None, description="Tabla/Vista (por defecto, la de config)")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/query")
def query_sql(body: SQLRequest):
    view = body.view or DEFAULT_VIEW
    data = run_sql(view=view, sql=body.sql)
    return {"status": "ok", "view": view, "rows": data}

class GroupSumRequest(BaseModel):
    view: Optional[str] = None
    group_col: str
    sum_col: str
    limit: Optional[int] = 100

@app.post("/helpers/group_sum")
def group_sum(body: GroupSumRequest):
    view = body.view or DEFAULT_VIEW
    sql = f'SELECT "{body.group_col}", SUM("{body.sum_col}") FROM "{view}" GROUP BY "{body.group_col}" LIMIT {body.limit}'
    data = run_sql(view=view, sql=sql)
    return {"status": "ok", "view": view, "rows": data}
