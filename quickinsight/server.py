import tempfile
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .db import Database
from .llm import LLM

app = FastAPI()
db: Database = None
llm: LLM = None

# Track processing jobs
_jobs: dict[str, dict] = {}


class AskRequest(BaseModel):
    question: str


class SqlRequest(BaseModel):
    sql: str


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".csv", ".sql", ".db", ".sqlite", ".sqlite3", ".parquet"):
        raise HTTPException(400, "Supported: .csv, .sql, .db, .sqlite, .parquet")

    # Stream to disk in chunks
    tmp_path = Path(tempfile.gettempdir()) / file.filename
    total_written = 0
    with open(tmp_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            f.write(chunk)
            total_written += len(chunk)

    # Start processing in background
    job_id = str(uuid.uuid4())[:8]
    _jobs[job_id] = {
        "status": "processing",
        "file": file.filename,
        "file_size": total_written,
        "progress": 0,
        "stage": "Starting...",
        "error": None,
    }

    def process():
        try:
            db.load_path(tmp_path, progress_cb=lambda p, s: _update_job(job_id, p, s))
            _jobs[job_id]["status"] = "done"
            _jobs[job_id]["progress"] = 100
            _jobs[job_id]["stage"] = "Done"
        except Exception as e:
            import traceback
            traceback.print_exc()
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["error"] = str(e)
        finally:
            tmp_path.unlink(missing_ok=True)

    threading.Thread(target=process, daemon=True).start()
    return {"job_id": job_id}


def _update_job(job_id: str, progress: float, stage: str):
    if job_id in _jobs:
        _jobs[job_id]["progress"] = round(progress)
        _jobs[job_id]["stage"] = stage


@app.get("/api/job/{job_id}")
def get_job(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    job = _jobs[job_id]
    result = {**job}
    if job["status"] == "done":
        result["tables"] = db.tables()
    return result


@app.get("/api/tables")
def list_tables():
    return db.tables()


@app.get("/api/tables/{name}")
def get_table(name: str, limit: int = 50, offset: int = 0):
    try:
        return db.table_rows(name, limit, offset)
    except Exception as e:
        raise HTTPException(400, str(e)) from None


@app.post("/api/sql")
def run_sql(req: SqlRequest):
    try:
        return db.query(req.sql)
    except Exception as e:
        raise HTTPException(400, str(e)) from None


@app.post("/api/ask")
def ask(req: AskRequest):
    schema = db.schema_prompt()
    sql = llm.generate_sql(schema, req.question)

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
            raise HTTPException(400, {"sql": sql, "error": str(e)}) from None

    explanation = llm.explain_results(req.question, sql, result["columns"], result["rows"])
    return {"sql": sql, "explanation": explanation, **result}


# Serve frontend
static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
