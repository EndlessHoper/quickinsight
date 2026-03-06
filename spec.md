# QuickInsight

Chat with your data. Fully local.

## What it does

You have a CSV. You want answers. You run one command:

```
quickinsight sales.csv
```

A browser opens. You see your data. You ask questions in plain English. You get answers — with the SQL shown, editable, re-runnable.

Nothing leaves your machine. The AI runs locally. The database runs locally. No accounts, no API keys, no cloud.

Don't believe us? Turn off your wifi. Everything still works.

## How it works

```
CSV / SQL file
     |
     v
  DuckDB (loads file, auto-detects schema)
     |
     v
  Schema extractor (DDL + sample rows)
     |
     v
  LLM prompt: schema + user question
     |
     v
  Qwen3.5-4B via llama-cpp-python (generates SQL)
     |
     v
  DuckDB executes the SQL
     |
     v
  Results shown in browser (table + optional chart)
  SQL shown alongside, editable, re-runnable
```

## Input formats

- **CSV** — DuckDB reads it natively via `read_csv()`. Auto-detects delimiters, headers, types.
- **SQL dump files** — Executed directly against DuckDB. Supports CREATE TABLE + INSERT statements.
- Multiple files at once: `quickinsight sales.csv customers.csv` — each CSV becomes a table named after the file.
- Directory: `quickinsight ./data/` — loads all CSV/SQL files in the directory.

## Architecture

### Backend (Python)

- **FastAPI** — serves the UI and the API
- **DuckDB** — SQL engine. Loads CSV/SQL files, executes queries. In-process, no server.
- **llama-cpp-python** — runs the LLM in-process. Loads a GGUF model, exposes OpenAI-compatible completions.
- **Schema extraction** — on file load, queries DuckDB's `information_schema` to get table names, column names, column types, and 3 sample rows per table. This becomes the LLM prompt context.

### Frontend

- Single-page web UI served by FastAPI as static files
- Two panels:
  1. **Chat** — natural language input, streaming answer, generated SQL shown below each answer (editable, re-runnable)
  2. **Data explorer** — table list sidebar, click a table to browse rows with sort/filter/pagination
- When query results are numeric aggregations or time series, auto-show a chart.

### LLM

- **Model:** Qwen3.5-4B (Q4_K_M GGUF, ~2.5GB)
- Auto-downloaded on first run to `~/.quickinsight/models/`
- Loaded via llama-cpp-python in-process
- Alternatively: `--api-url http://localhost:8000/v1` to use any external OpenAI-compatible server (vLLM, Ollama, llama.cpp server, etc.)

### Text-to-SQL prompt

```
You are a DuckDB SQL expert. Given the following tables:

{CREATE TABLE statements with column types}

Sample data:
{3-5 rows per table, formatted as INSERT statements}

Write a DuckDB SQL query that answers: {user question}

Rules:
- Return ONLY the SQL query, no explanation
- Use DuckDB SQL syntax
- Always qualify column names with table names when joining
```

If the generated SQL errors on execution, the error is fed back to the LLM for one retry.

## API endpoints

```
POST /api/ask        { "question": "..." }  →  { "sql": "...", "results": [...], "columns": [...] }
POST /api/sql        { "sql": "..." }       →  { "results": [...], "columns": [...] }
GET  /api/tables                            →  [{ "name": "...", "columns": [...], "row_count": N }]
GET  /api/tables/:name?limit=50&offset=0    →  { "columns": [...], "rows": [...], "total": N }
```

## CLI

```
quickinsight <file_or_files_or_directory>
```

Options:
- `--port 8642` — default port
- `--model /path/to/model.gguf` — use a specific GGUF model file
- `--api-url http://...` — use an external OpenAI-compatible API instead of local model
- `--no-browser` — don't auto-open browser

## Install

```
pip install quickinsight
```

Requires Python 3.10+. Model auto-downloads on first run (~2.5GB).

## Dependencies

- `fastapi` + `uvicorn` — web server
- `duckdb` — SQL engine
- `llama-cpp-python` — local LLM inference
- `huggingface-hub` — model download

## What this is NOT

- Not a database client (use DBeaver)
- Not a BI tool (use Metabase)
- Not a notebook (use Jupyter)
- Not an enterprise product

It's a sharp knife. You have data, you want answers, you run one command.
