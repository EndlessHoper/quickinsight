from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pathlib import Path
import tempfile

from .db import Database
from .llm import LLM

app = FastAPI()
db: Database = None
llm: LLM = None


class AskRequest(BaseModel):
    question: str


class SqlRequest(BaseModel):
    sql: str


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".csv", ".sql", ".db", ".sqlite", ".sqlite3", ".parquet"):
        raise HTTPException(400, "Supported: .csv, .sql, .db, .sqlite, .parquet")

    # Stream to disk in chunks instead of buffering in RAM
    tmp_path = Path(tempfile.gettempdir()) / file.filename
    with open(tmp_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):  # 1MB chunks
            f.write(chunk)

    db.load_path(tmp_path)
    tmp_path.unlink(missing_ok=True)
    return {"tables": db.tables()}


@app.get("/api/tables")
def list_tables():
    return db.tables()


@app.get("/api/tables/{name}")
def get_table(name: str, limit: int = 50, offset: int = 0):
    try:
        return db.table_rows(name, limit, offset)
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/sql")
def run_sql(req: SqlRequest):
    try:
        return db.query(req.sql)
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/ask")
def ask(req: AskRequest):
    schema = db.schema_prompt()
    sql = llm.generate_sql(schema, req.question)

    # Try executing, retry once on error
    try:
        result = db.query(sql)
    except Exception as first_error:
        retry_prompt = (
            f"The query:\n{sql}\n\nFailed with error:\n{first_error}\n\n"
            f"Fix the query and return only the corrected SQL."
        )
        sql = llm.generate_sql(schema, retry_prompt)
        try:
            result = db.query(sql)
        except Exception as e:
            raise HTTPException(400, {"sql": sql, "error": str(e)})

    # Generate a plain-English explanation
    explanation = llm.explain_results(req.question, sql, result["columns"], result["rows"])

    return {"sql": sql, "explanation": explanation, **result}


# Serve frontend
static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
