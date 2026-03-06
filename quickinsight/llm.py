from pathlib import Path
import json
import re
import subprocess
import time
import urllib.request

SQL_PROMPT = """/no_think
You are a DuckDB SQL expert. Given the following tables:

{schema}

Rules:
- Return ONLY the SQL query, no explanation, no markdown fences
- Use DuckDB SQL syntax
- Always qualify column names with table names when joining
- The results will be displayed in a UI table for a human to read
- ALWAYS add LIMIT 20 unless the user explicitly asks for all rows or a specific count
- For "show me", "what are", "list" type questions, default to LIMIT 10
- For aggregations (COUNT, SUM, AVG, etc), limit to top/bottom 20 groups
- Use ORDER BY to show the most relevant results first
- If asked for a summary or description, use appropriate aggregations"""

EXPLAIN_PROMPT = """/no_think
You are a helpful data analyst. The user asked: {question}

The SQL query run was:
{sql}

The results were (showing column names then rows):
{results}

Write a brief, natural language summary of the findings. Be specific with numbers. Keep it to 2-3 sentences. Do not repeat the question. Do not mention SQL."""

DEFAULT_MODEL_REPO = "unsloth/Qwen3.5-4B-GGUF"
DEFAULT_MODEL_FILE = "Qwen3.5-4B-Q4_K_M.gguf"
MODEL_DIR = Path.home() / ".quickinsight" / "models"
LLAMA_SERVER_PORT = 8679


class LLM:
    def __init__(self, model_path: str | None = None, api_url: str | None = None):
        self._process = None

        if api_url:
            self.api_url = api_url
            return

        gguf_path = Path(model_path) if model_path else MODEL_DIR / DEFAULT_MODEL_FILE
        if not gguf_path.exists():
            self._download_model(gguf_path)

        self.api_url = f"http://127.0.0.1:{LLAMA_SERVER_PORT}/v1"
        self._start_server(gguf_path)

    def _download_model(self, dest: Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading model {DEFAULT_MODEL_REPO}/{DEFAULT_MODEL_FILE}...")
        from huggingface_hub import hf_hub_download
        hf_hub_download(
            repo_id=DEFAULT_MODEL_REPO,
            filename=DEFAULT_MODEL_FILE,
            local_dir=str(dest.parent),
        )
        print("Download complete.")

    def _start_server(self, model_path: Path):
        print(f"Starting llama-server with {model_path.name}...")
        self._process = subprocess.Popen(
            [
                "llama-server",
                "-m", str(model_path),
                "--port", str(LLAMA_SERVER_PORT),
                "-ngl", "99",
                "-c", "4096",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for server to be ready
        for _ in range(60):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{LLAMA_SERVER_PORT}/health")
                print("Model ready.")
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("llama-server failed to start")

    def stop(self):
        if self._process:
            self._process.terminate()
            self._process.wait()

    def generate_sql(self, schema: str, question: str) -> str:
        prompt = SQL_PROMPT.format(schema=schema)
        user_msg = f"Write a DuckDB SQL query that answers: {question}"
        return self._call_api(prompt, user_msg)

    def explain_results(self, question: str, sql: str, columns: list, rows: list) -> str:
        # Format results as a readable table for the LLM
        header = " | ".join(columns)
        lines = [header]
        for row in rows[:15]:  # Don't send too many rows
            lines.append(" | ".join(str(v) for v in row))
        results_text = "\n".join(lines)

        prompt = EXPLAIN_PROMPT.format(
            question=question, sql=sql, results=results_text,
        )
        return self._call_api(prompt, "Summarize these results.")

    def _call_api(self, system: str, user: str) -> str:
        body = json.dumps({
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": 512,
            "temperature": 0,
        }).encode()
        url = self.api_url.rstrip("/") + "/chat/completions"
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        return self._clean_sql(data["choices"][0]["message"]["content"])

    def _clean_sql(self, text: str) -> str:
        text = text.strip()
        # Strip thinking tags
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        # Strip markdown fences
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        return text
