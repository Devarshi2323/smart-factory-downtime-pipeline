import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def get_connection_string() -> str:
    return (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'factory')} "
        f"user={os.getenv('DB_USER', 'factory')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )


def main() -> None:
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    with psycopg.connect(get_connection_string()) as conn:
        conn.execute(sql)
        conn.commit()

        print("Schema applied. Tables and row counts:")
        for table in ("stations", "fault_codes", "machine_events", "state_intervals", "part_cycles"):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<16} {count} rows")

        views = conn.execute(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema = 'public' ORDER BY table_name"
        ).fetchall()
        print("Views:", ", ".join(v[0] for v in views))


if __name__ == "__main__":
    main()