import subprocess
import tempfile
import duckdb
from pathlib import Path
from datetime import date, datetime, time
from decimal import Decimal


def _serialize_row(row):
    return [
        str(v) if isinstance(v, (date, datetime, time, Decimal, bytes)) else v
        for v in row
    ]


# awk script to convert MySQL dump → SQLite-compatible SQL
# Runs at native C speed, handles GBs in seconds
_MYSQL_TO_SQLITE_AWK = r"""
/^\/\*!/ { next }
/^--/ { next }
/^LOCK / { next }
/^UNLOCK / { next }
/^SET / { next }
/^DROP / { next }
/^USE / { next }
/^[[:space:]]*PRIMARY KEY / { next }
/^[[:space:]]*KEY / { next }
/^[[:space:]]*UNIQUE KEY / { next }
/^[[:space:]]*CONSTRAINT / { next }
/^[[:space:]]*INDEX / { next }
{
  gsub(/ENGINE=[^ ;]*/, "")
  gsub(/DEFAULT CHARSET=[^ ;]*/, "")
  gsub(/COLLATE=[^ ,;)]*/, "")
  gsub(/ COLLATE [^ ,;)]*/, "")
  gsub(/CHARACTER SET [^ ,;)]*/, "")
  gsub(/AUTO_INCREMENT=[0-9]*/, "")
  gsub(/ AUTO_INCREMENT/, "")
  gsub(/ UNSIGNED/, "")
  gsub(/ROW_FORMAT=[^ ;]*/, "")
  gsub(/ON UPDATE CURRENT_TIMESTAMP/, "")
  gsub(/IF NOT EXISTS /, "")
  gsub(/COMMENT '[^']*'/, "")
  gsub(/`/, "\"")
  gsub(/ int\([0-9]*\)/, " INTEGER")
  gsub(/ bigint\([0-9]*\)/, " BIGINT")
  gsub(/ smallint\([0-9]*\)/, " SMALLINT")
  gsub(/ tinyint\([0-9]*\)/, " TINYINT")
  gsub(/ mediumint\([0-9]*\)/, " INTEGER")
  gsub(/ float\([0-9,]*\)/, " REAL")
  gsub(/ double /, " REAL ")
  gsub(/ longtext/, " TEXT")
  gsub(/ mediumtext/, " TEXT")
  gsub(/ tinytext/, " TEXT")
  gsub(/ longblob/, " BLOB")
  gsub(/ mediumblob/, " BLOB")
  gsub(/ tinyblob/, " BLOB")
  gsub(/ enum\([^)]*\)/, " TEXT")
  gsub(/ set\([^)]*\)/, " TEXT")
  if (match($0, /^[[:space:]]*\)/)) { sub(/,[[:space:]]*$/, "", prev) }
  if (prev != "") print prev
  prev = $0
}
END { if (prev != "") print prev }
"""


class Database:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute("INSTALL sqlite; LOAD sqlite;")

    def load_csv(self, path: Path):
        table_name = path.stem.replace("-", "_").replace(" ", "_")
        self.conn.execute(
            f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_csv('{path}', auto_detect=true)"
        )

    def load_sql(self, path: Path):
        """Load a SQL dump via awk+sqlite3 for speed. All heavy lifting in C."""
        sqlite_path = Path(tempfile.mktemp(suffix=".db"))
        awk_path = Path(tempfile.mktemp(suffix=".awk"))
        try:
            awk_path.write_text(_MYSQL_TO_SQLITE_AWK)
            # awk converts MySQL→SQLite syntax, sqlite3 imports it
            # Wrapping in BEGIN/COMMIT makes sqlite3 bulk-import fast
            subprocess.run(
                f"""(echo "BEGIN TRANSACTION;"; awk -f '{awk_path}' '{path}'; echo "COMMIT;") | sqlite3 '{sqlite_path}'""",
                shell=True,
                capture_output=True,
                text=True,
            )
            if not sqlite_path.exists() or sqlite_path.stat().st_size == 0:
                # Not a MySQL dump, or conversion failed — try direct DuckDB execution
                self._load_sql_direct(path)
                return

            self.load_sqlite(sqlite_path)
        finally:
            sqlite_path.unlink(missing_ok=True)
            awk_path.unlink(missing_ok=True)

    def _load_sql_direct(self, path: Path):
        """Fallback: execute SQL directly in DuckDB (for DuckDB-native dumps)."""
        self.conn.execute("BEGIN TRANSACTION")
        count = 0
        with open(path, "r") as f:
            stmt = ""
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("--"):
                    continue
                stmt += line
                if stripped.endswith(";"):
                    self.conn.execute(stmt)
                    stmt = ""
                    count += 1
                    if count % 10000 == 0:
                        self.conn.execute("COMMIT")
                        self.conn.execute("BEGIN TRANSACTION")
            if stmt.strip():
                self.conn.execute(stmt)
        self.conn.execute("COMMIT")

    def load_sqlite(self, path: Path):
        """Load a .db / .sqlite file directly via DuckDB's sqlite scanner."""
        # Get table names via sqlite3 CLI (DuckDB doesn't expose sqlite_master)
        result = subprocess.run(
            ["sqlite3", str(path), ".tables"],
            capture_output=True, text=True,
        )
        table_names = result.stdout.split()

        self.conn.execute(f"ATTACH '{path}' AS src (TYPE SQLITE)")
        try:
            for table in table_names:
                self.conn.execute(
                    f'CREATE TABLE "{table}" AS SELECT * FROM src."{table}"'
                )
        finally:
            self.conn.execute("DETACH src")

    def load_parquet(self, path: Path):
        table_name = path.stem.replace("-", "_").replace(" ", "_")
        self.conn.execute(
            f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_parquet('{path}')"
        )

    def load_path(self, path: Path):
        if path.is_dir():
            for f in sorted(path.iterdir()):
                self._load_single(f)
        else:
            self._load_single(path)

    def _load_single(self, path: Path):
        ext = path.suffix.lower()
        if ext == ".csv":
            self.load_csv(path)
        elif ext == ".sql":
            self.load_sql(path)
        elif ext in (".db", ".sqlite", ".sqlite3"):
            self.load_sqlite(path)
        elif ext == ".parquet":
            self.load_parquet(path)

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
