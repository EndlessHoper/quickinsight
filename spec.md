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
  Qwen3.5-4B via llama.cpp (generates SQL)
     |
     v
  DuckDB executes the SQL
     |
     v
  Results shown in browser (table + optional chart)
  SQL shown alongside, editable, re-runnable
```

## Input formats

- **CSV** — DuckDB reads it natively. Auto-detects delimiters, headers, types.
- **SQL dump files** — Executed directly against DuckDB. Supports CREATE TABLE + INSERT statements.
- Multiple files at once: `quickinsight sales.csv customers.csv` creates multiple tables.
- Directory: `quickinsight ./data/` loads all CSV/SQL files in the directory.

## Architecture

### Backend (Python)

- **FastAPI** — serves the UI and the API
- **DuckDB** — SQL engine, loads and queries data
- **llama-cpp-python** — runs the LLM locally, no Ollama dependency
- **Schema extraction** — reads DuckDB's information_schema, grabs DDL + sample rows per table

### Frontend

- Single-page web UI served by the backend
- Three panels:
  1. **Chat** — natural language input, streaming responses
  2. **Data explorer** — table list, schema view, browse rows, filter, sort
  3. **Results** — query results as table, generated SQL (editable), re-run button
- Auto-chart: when results look like aggregations or time series, show a chart

### LLM

- **Model:** Qwen3.5-4B (Q4_K_M GGUF, ~2.5GB)
- Downloaded automatically on first run to `~/.quickinsight/models/`
- Served via llama-cpp-python in-process (no separate server)

### Text-to-SQL prompt

```
You are a DuckDB SQL expert. Given the following tables:

{CREATE TABLE statements with column types}

Sample data:
{3-5 rows per table}

Write a DuckDB SQL query that answers: {user question}

Rules:
- Return ONLY the SQL query, no explanation
- Use DuckDB SQL syntax
- Always qualify column names with table names when joining
```

If the generated SQL errors, the error message is fed back to the LLM for one retry.

## CLI

```
quickinsight <file_or_files_or_directory>
```

Options:
- `--port 8642` — default port
- `--model /path/to/model.gguf` — use a specific model file
- `--no-browser` — don't auto-open browser

## Install

```
pip install quickinsight
```

Requires Python 3.10+. Model downloads on first run (~2.5GB).

## What this is NOT

- Not a database client (use DBeaver)
- Not a BI tool (use Metabase)
- Not a notebook (use Jupyter)
- Not an enterprise product

It's a sharp knife. You have data, you want answers, you run one command.
