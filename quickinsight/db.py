import duckdb
from pathlib import Path
from datetime import date, datetime, time
from decimal import Decimal


def _serialize_row(row):
    return [
        str(v) if isinstance(v, (date, datetime, time, Decimal, bytes)) else v
        for v in row
    ]


class Database:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")

    def load_csv(self, path: Path):
        table_name = path.stem.replace("-", "_").replace(" ", "_")
        self.conn.execute(
            f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_csv('{path}', auto_detect=true)"
        )

    def load_sql(self, path: Path):
        sql = path.read_text()
        self.conn.execute(sql)

    def load_path(self, path: Path):
        if path.is_dir():
            for f in sorted(path.iterdir()):
                if f.suffix == ".csv":
                    self.load_csv(f)
                elif f.suffix == ".sql":
                    self.load_sql(f)
        elif path.suffix == ".csv":
            self.load_csv(path)
        elif path.suffix == ".sql":
            self.load_sql(path)

    def tables(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        result = []
        for (name,) in rows:
            cols = self.conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ? AND table_schema = 'main' ORDER BY ordinal_position",
                [name],
            ).fetchall()
            count = self.conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            result.append({
                "name": name,
                "columns": [{"name": c, "type": t} for c, t in cols],
                "row_count": count,
            })
        return result

    def table_rows(self, name: str, limit: int = 50, offset: int = 0) -> dict:
        total = self.conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        cols = self.conn.execute(f'SELECT * FROM "{name}" LIMIT 0').description
        col_names = [c[0] for c in cols]
        rows = self.conn.execute(
            f'SELECT * FROM "{name}" LIMIT ? OFFSET ?', [limit, offset]
        ).fetchall()
        return {
            "columns": col_names,
            "rows": [_serialize_row(r) for r in rows],
            "total": total,
        }

    def query(self, sql: str) -> dict:
        result = self.conn.execute(sql)
        cols = [c[0] for c in result.description]
        rows = result.fetchall()
        return {
            "columns": cols,
            "rows": [_serialize_row(r) for r in rows],
        }

    def schema_prompt(self) -> str:
        parts = []
        for table in self.tables():
            name = table["name"]
            col_defs = ", ".join(f'"{c["name"]}" {c["type"]}' for c in table["columns"])
            parts.append(f'CREATE TABLE "{name}" ({col_defs});')
            # Sample rows
            sample = self.conn.execute(f'SELECT * FROM "{name}" LIMIT 3').fetchall()
            if sample:
                col_names = ", ".join(f'"{c["name"]}"' for c in table["columns"])
                for row in sample:
                    vals = ", ".join(
                        f"'{v}'" if isinstance(v, str) else str(v) for v in row
                    )
                    parts.append(f'-- INSERT INTO "{name}" ({col_names}) VALUES ({vals});')
            parts.append("")
        return "\n".join(parts)
