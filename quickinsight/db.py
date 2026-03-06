import datetime as _dt
import subprocess
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import duckdb


def _serialize_row(row):
    return [
        str(v) if isinstance(v, (_dt.date, _dt.datetime, _dt.time, Decimal, bytes)) else v
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
        self._db_path = Path(tempfile.mktemp(suffix=".duckdb"))
        self.conn = duckdb.connect(str(self._db_path))
        self.conn.execute("INSTALL sqlite; LOAD sqlite;")
        # Data lives on disk, buffer pool caps RAM usage
        self.conn.execute("SET memory_limit = '2GB'")
        self._pg_dbs: list[str] = []

    def close(self):
        self.conn.close()
        self._db_path.unlink(missing_ok=True)
        Path(str(self._db_path) + ".wal").unlink(missing_ok=True)
        for db_name in self._pg_dbs:
            subprocess.run(["dropdb", "--if-exists", db_name], capture_output=True)

    def load_csv(self, path: Path, progress_cb=None):
        if progress_cb:
            progress_cb(10, "Reading CSV...")
        table_name = path.stem.replace("-", "_").replace(" ", "_")
        self.conn.execute(
            f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_csv('{path}', auto_detect=true)"
        )
        if progress_cb:
            progress_cb(100, "Done")

    @staticmethod
    def _detect_sql_type(path: Path) -> str:
        """Peek at first few KB to detect dump type."""
        with open(path) as f:
            head = f.read(4096)
        if "PostgreSQL database dump" in head or "COPY " in head or "public." in head:
            return "postgres"
        if "MySQL" in head or "ENGINE=" in head or "AUTO_INCREMENT" in head:
            return "mysql"
        return "generic"

    def load_sql(self, path: Path, progress_cb=None):
        sql_type = self._detect_sql_type(path)
        if sql_type == "postgres":
            return self._load_postgres_dump(path, progress_cb)
        return self._load_mysql_dump(path, progress_cb)

    def _load_postgres_dump(self, path: Path, progress_cb=None):
        """Load pg dump via psql, query directly from DuckDB."""
        import uuid as _uuid

        db_name = f"qi_{_uuid.uuid4().hex[:8]}"
        if progress_cb:
            progress_cb(5, "Loading into PostgreSQL...")

        subprocess.run(["createdb", db_name], capture_output=True, check=True)
        self._pg_dbs.append(db_name)

        proc = subprocess.Popen(
            ["psql", "-q", db_name, "-f", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        start = time.time()
        while proc.poll() is None:
            if progress_cb:
                elapsed = int(time.time() - start)
                progress_cb(min(80, 5 + elapsed), f"Loading into PostgreSQL... ({elapsed}s)")
            time.sleep(1)

        if progress_cb:
            progress_cb(85, "Connecting tables...")

        self.conn.execute("INSTALL postgres; LOAD postgres;")
        self.conn.execute(f"ATTACH 'dbname={db_name}' AS pg (TYPE POSTGRES)")
        tables = self.conn.execute(
            "SELECT table_name FROM pg.information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ).fetchall()
        for table, in tables:
            self.conn.execute(
                f'CREATE VIEW "{table}" AS SELECT * FROM pg.public."{table}"'
            )

        if progress_cb:
            progress_cb(100, "Done")

    def _load_mysql_dump(self, path: Path, progress_cb=None):
        """Load a MySQL dump via awk+sqlite3 for speed. All heavy lifting in C."""
        sqlite_path = Path(tempfile.mktemp(suffix=".db"))
        awk_path = Path(tempfile.mktemp(suffix=".awk"))
        input_size = path.stat().st_size
        try:
            awk_path.write_text(_MYSQL_TO_SQLITE_AWK)

            if progress_cb:
                progress_cb(5, "Converting SQL syntax...")

            # Run pipeline in a thread so we can monitor sqlite file growth
            proc = subprocess.Popen(
                f'(echo "BEGIN TRANSACTION;"; awk -f \'{awk_path}\' \'{path}\';'
                f' echo "COMMIT;") | sqlite3 \'{sqlite_path}\'',
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=None,
            )

            # Monitor progress via journal file (main .db stays 0 until COMMIT)
            journal_path = Path(str(sqlite_path) + "-journal")
            estimated_output = input_size * 0.5
            while proc.poll() is None:
                if progress_cb:
                    # Check journal first (grows during transaction), then main db
                    current = 0
                    for p in (journal_path, sqlite_path):
                        if p.exists():
                            current += p.stat().st_size
                    pct = min(70, 5 + int(65 * current / max(estimated_output, 1)))
                    size_mb = current / 1e6
                    progress_cb(pct, f"Importing data... ({size_mb:.0f} MB written)")
                time.sleep(0.5)

            if proc.returncode != 0:
                if progress_cb:
                    progress_cb(10, "Trying direct import...")
                self._load_sql_direct(path, progress_cb)
                return

            if not sqlite_path.exists() or sqlite_path.stat().st_size == 0:
                if progress_cb:
                    progress_cb(10, "Trying direct import...")
                self._load_sql_direct(path, progress_cb)
                return

            if progress_cb:
                progress_cb(75, "Loading into database...")
            self.load_sqlite(sqlite_path, progress_cb)
        finally:
            sqlite_path.unlink(missing_ok=True)
            awk_path.unlink(missing_ok=True)

    def _load_sql_direct(self, path: Path, progress_cb=None):
        """Fallback: execute SQL directly in DuckDB (for DuckDB-native dumps)."""
        file_size = path.stat().st_size
        bytes_read = 0
        self.conn.execute("BEGIN TRANSACTION")
        count = 0
        with open(path) as f:
            stmt = ""
            for line in f:
                bytes_read += len(line)
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
                        if progress_cb:
                            pct = min(95, int(95 * bytes_read / max(file_size, 1)))
                            progress_cb(pct, f"Executing SQL... ({count:,} statements)")
            if stmt.strip():
                self.conn.execute(stmt)
        self.conn.execute("COMMIT")

    def load_sqlite(self, path: Path, progress_cb=None):
        """Load a .db / .sqlite file directly via DuckDB's sqlite scanner."""
        # Get table names via sqlite3 CLI (DuckDB doesn't expose sqlite_master)
        result = subprocess.run(
            ["sqlite3", str(path), ".tables"],
            capture_output=True, text=True,
        )
        table_names = result.stdout.split()

        if progress_cb:
            progress_cb(10, "Loading SQLite tables...")
        self.conn.execute(f"ATTACH '{path}' AS src (TYPE SQLITE)")
        try:
            for i, table in enumerate(table_names):
                if progress_cb:
                    pct = 10 + int(85 * i / max(len(table_names), 1))
                    progress_cb(pct, f"Loading table {table}...")
                self.conn.execute(
                    f'CREATE TABLE "{table}" AS SELECT * FROM src."{table}"'
                )
        finally:
            self.conn.execute("DETACH src")

    def load_parquet(self, path: Path, progress_cb=None):
        if progress_cb:
            progress_cb(10, "Reading Parquet...")
        table_name = path.stem.replace("-", "_").replace(" ", "_")
        self.conn.execute(
            f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_parquet('{path}')"
        )
        if progress_cb:
            progress_cb(100, "Done")

    def load_path(self, path: Path, progress_cb=None):
        if path.is_dir():
            for f in sorted(path.iterdir()):
                self._load_single(f, progress_cb)
        else:
            self._load_single(path, progress_cb)

    def _load_single(self, path: Path, progress_cb=None):
        ext = path.suffix.lower()
        if ext == ".csv":
            self.load_csv(path, progress_cb)
        elif ext == ".sql":
            self.load_sql(path, progress_cb)
        elif ext in (".db", ".sqlite", ".sqlite3"):
            self.load_sqlite(path, progress_cb)
        elif ext == ".parquet":
            self.load_parquet(path, progress_cb)

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
        result = "\n".join(parts)
        # Cap schema size to fit in model context
        if len(result) > 6000:
            result = result[:6000] + "\n-- (schema truncated)"
        return result
