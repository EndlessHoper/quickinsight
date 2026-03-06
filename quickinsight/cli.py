import argparse
import webbrowser
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        prog="quickinsight",
        description="Chat with your data. Fully local.",
    )
    parser.add_argument("files", nargs="*", help="CSV files, SQL files, or directories")
    parser.add_argument("--port", type=int, default=8642)
    parser.add_argument("--model", help="Path to a GGUF model file")
    parser.add_argument("--api-url", help="External OpenAI-compatible API URL")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    # Import here so --help is fast
    from .db import Database
    from .llm import LLM
    from . import server

    # Load data
    server.db = Database()
    for f in args.files:
        p = Path(f).resolve()
        if not p.exists():
            print(f"Error: {p} not found")
            return 1
        server.db.load_path(p)
        print(f"Loaded: {p}")

    tables = server.db.tables()
    if tables:
        print(f"\n{len(tables)} table(s):")
        for t in tables:
            cols = ", ".join(c["name"] for c in t["columns"])
            print(f"  {t['name']} ({t['row_count']} rows) — {cols}")

    # Load LLM
    print()
    server.llm = LLM(model_path=args.model, api_url=args.api_url)

    # Start server
    url = f"http://localhost:{args.port}"
    print(f"\nQuickInsight running at {url}")
    if not args.no_browser:
        webbrowser.open(url)

    import uvicorn
    try:
        uvicorn.run(server.app, host="0.0.0.0", port=args.port, log_level="warning")
    finally:
        server.llm.stop()
